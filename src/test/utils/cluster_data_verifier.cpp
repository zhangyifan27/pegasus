// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/tool-api/task_code.h>
#include <dsn/dist/fmt_logging.h>

#include "cluster_data_verifier_impl.h"

namespace pegasus {
namespace test {

DEFINE_TASK_CODE(LPC_DATA_VERIFIER, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT);
DEFINE_TASK_CODE(LPC_DATA_VERIFIER_HISTOGRAM, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT);

// static members
dsn::stats::histogram data_verifier::set_histogram;
dsn::stats::histogram data_verifier::get_histogram;
data_verifier::histogram_printer_t data_verifier::histogram_printer;

using namespace dsn::literals::chrono_literals;

data_verifier::~data_verifier()
{
    pause();
    wait_all();
}

data_verifier::data_verifier(pegasus_client *client, dsn::string_view data_prefix)
    : data_verifier(client, client, data_prefix)
{
}

data_verifier::data_verifier(pegasus_client *insert_client,
                             pegasus_client *verify_client,
                             dsn::string_view data_prefix)
    : _progress(data_prefix)
{
    thread_pool(LPC_DATA_VERIFIER).task_tracker(&_tracker);

    _recover = dsn::make_unique<recover_break_point>(insert_client, &_progress);
    _insert = dsn::make_unique<insert_data>(insert_client, &_progress);
    _verify = dsn::make_unique<verify_data>(verify_client, &_progress);

    from(*_recover).link(*_insert).link(*_verify).link(*_insert);
}

data_verifier::histogram_printer_t::histogram_printer_t()
    : _print([this]() {
          ddebug_f("SET metrics: {}", data_verifier::set_histogram.summary());
          ddebug_f("GET metrics: {}", data_verifier::get_histogram.summary());
          _print.repeat(10_s);
      })
{
    thread_pool(LPC_DATA_VERIFIER_HISTOGRAM).task_tracker(&_tracker).from(_print);
}

data_verifier::histogram_printer_t::~histogram_printer_t()
{
    pause();
    wait_all();
}

} // namespace test
} // namespace pegasus
