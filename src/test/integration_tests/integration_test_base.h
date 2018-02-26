// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "test/utils/onebox_controller.h"
#include "test_utils.h"

#include <dsn/dist/replication/replication_ddl_client.h>
#include <gtest/gtest.h>

namespace pegasus {
namespace test {

class integration_test_base : public ::testing::Test
{
public:
    integration_test_base() : _onebox(new onebox_controller) {}

    void create_app(const std::string &app_name)
    {
        ASSERT_EQ(_ddl_client->create_app(app_name, "pegasus", 3, 3, {}, false), dsn::ERR_OK);
    }

    void SetUp() override
    {
        _onebox->clear_onebox();
        _onebox->start_onebox(3, 3);
        sleep(3); // wait for the onebox to setup

        _ddl_client =
            dsn::make_unique<dsn::replication::replication_ddl_client>(list_meta("onebox"));
    }

    void TearDown() override { _onebox->stop_onebox(); }

protected:
    std::unique_ptr<onebox_controller> _onebox;
    std::unique_ptr<dsn::replication::replication_ddl_client> _ddl_client;
};

} // namespace test
} // namespace pegasus
