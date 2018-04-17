// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "test/utils/cluster_data_verifier.h"
#include "test/utils/client_init.h"

#include <gtest/gtest.h>
#include <gflags/gflags.h>

using namespace pegasus;

GTEST_API_ int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    testing::InitGoogleTest(&argc, argv);
    test::init_client();

    RUN_ALL_TESTS();
}
