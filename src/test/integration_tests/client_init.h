// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <pegasus/client.h>
#include <dsn/service_api_c.h>

namespace pegasus {
namespace test {

std::unique_ptr<pegasus_client> p_client;

// We must run init_client before everything. It sets up the underlying
// rDSN environment and initializes a global pegasus client.
inline void init_client()
{
    if (!pegasus_client_factory::initialize("config.ini")) {
        derror("MainThread: init pegasus failed");
        dsn_exit(-1);
    }
    p_client.reset(pegasus_client_factory::get_client("onebox", "test_app"));
}

} // namespace test
} // namespace pegasus
