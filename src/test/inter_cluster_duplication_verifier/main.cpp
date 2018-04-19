// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <gflags/gflags.h>

#include "test/utils/client_init.h"
#include "test/utils/cluster_data_verifier.h"

using namespace pegasus;

DEFINE_string(slave_cluster, "slave", "");
DEFINE_string(master_cluster, "master", "");
DEFINE_string(table_name, "temp", "");

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    test::init_pegasus_client();
    auto insert_client = test::must_get_client(FLAGS_master_cluster, FLAGS_table_name);
    auto verify_client = test::must_get_client(FLAGS_slave_cluster, FLAGS_table_name);

    test::data_verifier verifier(insert_client, verify_client, "duplication_test_");
    verifier.run_pipeline();

    test::data_verifier::histogram_printer.run_pipeline();

    verifier.wait_all();
    return 0;
}
