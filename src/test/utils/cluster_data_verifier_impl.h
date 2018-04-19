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

DECLARE_uint32(verify_delay_ms);

using namespace dsn::literals::chrono_literals;

struct insert_data : public dsn::pipeline::when<>, dsn::pipeline::result<>
{
    insert_data(pegasus_client *client, data_verifier::progress *progress)
        : _client(client), _progress(progress)
    {
    }

    // Delay 1s if set failed, terminate after 10 attempts.
    void run() override;

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

    void run() override;

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

    void run() override;

private:
    pegasus_client *_client;
    data_verifier::progress *_progress;
};

} // namespace test
} // namespace pegasus
