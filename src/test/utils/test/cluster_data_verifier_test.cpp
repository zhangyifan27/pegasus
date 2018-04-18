// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "test/utils/client_init.h"
#include "test/utils/cluster_data_verifier_impl.h"

#include <gtest/gtest.h>

using namespace pegasus;

TEST(cluster_data_verfier, histogram_printer)
{
    // ensure pipeline will terminate after being destructed
    test::data_verifier::histogram_printer.run_pipeline();
    test::data_verifier::histogram_printer.~histogram_printer_t();
}

TEST(cluster_data_verfier, data_verifier)
{
    // ensure pipeline will terminate after being destructed
    auto client = test::must_get_client("onebox", "temp");

    test::data_verifier verifier(client, "test_");
    verifier.run_pipeline();
}
