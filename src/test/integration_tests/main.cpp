// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include "client_init.h"

using namespace pegasus::test;

GTEST_API_ int main(int argc, char **argv)
{
    init_client();

    gflags::ParseCommandLineFlags(&argc, &argv, true);
    testing::InitGoogleTest(&argc, argv);
    int ans = RUN_ALL_TESTS();
    dsn_exit(ans);
}
