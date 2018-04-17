// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "test/utils/cluster_data_verifier.h"
#include "test/utils/client_init.h"

#include <gtest/gtest.h>

using namespace pegasus;

TEST(cluster_data_verfier, histogram_printer)
{
    // ensure pipeline will terminate after program exit
    test::data_verifier::histogram_printer.run_pipeline();
}

TEST(cluster_data_verfier, data_verifier)
{
    // ensure pipeline will terminate after program exit
    auto client = test::must_get_client("onebox", "temp");

    test::data_verifier verifier(client, "test_");
    verifier.run_pipeline();
}
