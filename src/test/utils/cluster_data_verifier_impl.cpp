// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "cluster_data_verifier_impl.h"

namespace pegasus {
namespace test {

DEFINE_uint32(verify_delay_ms, 1000, "");

void insert_data::run()
{
    if (_tries == 0) { // first try
        _break_point_counter++;
        if (_break_point_counter > BREAK_POINT_THRESHOLD) {
            _progress->mark_break_point_mode();
            _break_point_counter = 0;
        } else {
            _progress->next();
        }
    }

    _tries++;
    _timer.start();

    pegasus_client::internal_info info;
    int err =
        _client->set(_progress->hash_key, _progress->sort_key, _progress->value, 5000, 0, &info);

    if (err == PERR_OK) {
        _timer.stop();
        data_verifier::set_histogram.measure(_timer.u_elapsed());
        _tries = 0;

        step_down_next_stage();
    } else {
        derror_f("SET failed: id={}, try={}, error={} gpid={}.{}, decree={}, server={})",
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
}

void verify_data::run()
{
    if (_tries == 0) { // first try
        if (FLAGS_verify_delay_ms > 0) {
            usleep(FLAGS_verify_delay_ms * 1000);
        }
    }

    _tries++;
    _timer.start();

    std::string actual;
    pegasus_client::internal_info info;
    int err = _client->get(_progress->hash_key, _progress->sort_key, actual, 5000, &info);

    if (err == PERR_OK) {
        _timer.stop();
        data_verifier::get_histogram.measure(_timer.u_elapsed());
        _tries = 0;

        step_down_next_stage();
    } else {
        derror_f("GET failed: id={}, try={}, error={} gpid={}.{}, decree={}, server={})",
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
}

void recover_break_point::run()
{
    _progress->query_break_point_mode();

    std::string actual;
    pegasus_client::internal_info info;
    int err = _client->get(_progress->hash_key, _progress->sort_key, actual, 5000, &info);

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
}

} // namespace test
} // namespace pegasus