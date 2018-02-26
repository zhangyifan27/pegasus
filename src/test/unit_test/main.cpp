// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "server/pegasus_perf_counter.h"

#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>
#include <dsn/dist/replication/replication_service_app.h>

int g_test_count = 0;
int g_test_ret = 0;

class replication_service_test_app : public dsn::service_app
{
public:
    replication_service_test_app(const dsn::service_app_info *info) : ::dsn::service_app(info) {}

    dsn::error_code start(const std::vector<std::string> &args) override
    {
        g_test_ret = RUN_ALL_TESTS();
        g_test_count = 1;
        return dsn::ERR_OK;
    }

    dsn::error_code stop(bool) override { return dsn::ERR_OK; }
};

GTEST_API_ int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);

    // register all possible services
    dsn::service_app::register_factory<replication_service_test_app>("replica");

    ::dsn::tools::internal_use_only::register_component_provider(
        "pegasus::server::pegasus_perf_counter",
        pegasus::server::pegasus_perf_counter_factory,
        ::dsn::PROVIDER_TYPE_MAIN);

    // specify what services and tools will run in config file, then run
    dsn_run_config("config-test.ini", false);
    while (g_test_count == 0) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    dsn_exit(g_test_ret);
}
