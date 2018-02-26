// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "server/pegasus_duplication_backlog_handler.h"
#include "test/integration_tests/integration_test_base.h"

using namespace pegasus::server;
using namespace pegasus::test;

class replica_server_dup_test : public integration_test_base
{
};

TEST_F(replica_server_dup_test, duplication_sync)
{
    { // no dup
    }
}
