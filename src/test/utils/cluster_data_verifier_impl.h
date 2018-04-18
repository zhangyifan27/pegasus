// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/stats/timer.h>
#include <dsn/dist/fmt_logging.h>
#include <boost/lexical_cast.hpp>

#include "cluster_data_verifier.h"

namespace pegasus {
namespace test {

using namespace dsn::literals::chrono_literals;

struct insert_data : public dsn::pipeline::when<>, dsn::pipeline::result<>
{
    insert_data(pegasus_client *client, data_verifier::progress *progress)
        : _client(client), _progress(progress)
    {
    }

    // Delay 1s if set failed, terminate after 10 attempts.
    void run() override
    {
        _tries++;
        if (_tries == 1) { // first try
            _break_point_counter++;
            if (_break_point_counter > BREAK_POINT_THRESHOLD) {
                _progress->mark_break_point_mode();
                _break_point_counter = 0;
            } else {
                _progress->next();
            }
        }

        _timer.start();
        _client->async_set(
            _progress->hash_key,
            _progress->sort_key,
            _progress->value,
            [this](int err, pegasus_client::internal_info &&info) {
                if (err == PERR_OK) {
                    _timer.stop();
                    data_verifier::set_histogram.measure(_timer.u_elapsed());
                    _tries = 0;

                    step_down_next_stage();
                } else {
                    derror_f(
                        "SET failed: id={}, try={}, error={} gpid={}.{}, decree={}, server={})",
                        _progress->id,
                        _tries,
                        _client->get_error_string(err),
                        info.app_id,
                        info.partition_index,
                        info.decree,
                        info.server);

                    if (_tries >= 10) {
                        dfatal("SET failed in the last 10 attempts. id={}", _progress->id);
                    } else {
                        repeat(1_s);
                    }
                }
            });
    }

private:
    int _tries{0};

    int _break_point_counter{0};
    static constexpr int BREAK_POINT_THRESHOLD{1000};

    dsn::stats::timer _timer;

    pegasus_client *_client;
    data_verifier::progress *_progress;
};

struct verify_data : public dsn::pipeline::when<>, dsn::pipeline::result<>
{
    verify_data(pegasus_client *client, data_verifier::progress *progress)
        : _client(client), _progress(progress)
    {
    }

    void run() override
    {
        _tries++;
        _timer.start();
        _client->async_get(
            _progress->hash_key,
            _progress->sort_key,
            [this](int err, std::string &&actual, pegasus_client::internal_info &&info) {
                if (err == PERR_OK) {
                    _timer.stop();
                    data_verifier::get_histogram.measure(_timer.u_elapsed());
                    _tries = 0;

                    step_down_next_stage();
                } else {
                    derror_f(
                        "GET failed: id={}, try={}, error={} gpid={}.{}, decree={}, server={})",
                        _progress->id,
                        _tries,
                        _client->get_error_string(err),
                        info.app_id,
                        info.partition_index,
                        info.decree,
                        info.server);

                    auto delay = _tries > 3 ? 1_s : 0_s;
                    repeat(delay);
                }
            });
    }

private:
    int _tries{0};
    dsn::stats::timer _timer;

    pegasus_client *_client;
    data_verifier::progress *_progress;
};

struct recover_break_point : public dsn::pipeline::when<>, dsn::pipeline::result<>
{
    recover_break_point(pegasus_client *client, data_verifier::progress *progress)
        : _client(client), _progress(progress)
    {
    }

    void run() override
    {
        _progress->query_break_point_mode();
        _client->async_get(
            _progress->hash_key,
            _progress->sort_key,
            [this](int err, std::string &&actual, pegasus_client::internal_info &&info) {
                if (err == PERR_OK) {
                    _progress->id = boost::lexical_cast<uint64_t>(actual);
                    ddebug_f("start data verifier from index:{}", _progress->id);
                    step_down_next_stage();
                } else if (err == PERR_NOT_FOUND) {
                    _progress->id = 0; // start from 0
                    ddebug_f("start data verifier from empty cluster");
                    step_down_next_stage();
                } else {
                    dfatal_f("RECOVER failed: error={} gpid={}.{}, decree={}, server={})",
                             _client->get_error_string(err),
                             info.app_id,
                             info.partition_index,
                             info.decree,
                             info.server);
                }
            });
    }

private:
    pegasus_client *_client;
    data_verifier::progress *_progress;
};

} // namespace test
} // namespace pegasus
