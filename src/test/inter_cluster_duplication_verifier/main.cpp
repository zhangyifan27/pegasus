// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <gflags/gflags.h>

#include "test/utils/client_init.h"
#include "test/utils/cluster_data_verifier.h"

using namespace pegasus;

DEFINE_string(remote_cluster, "", "");
DEFINE_string(local_cluster, "", "");
DEFINE_string(table_name, "", "");

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    test::init_client();
    auto insert_client = test::must_get_client(FLAGS_local_cluster, FLAGS_table_name);
    auto verify_client = test::must_get_client(FLAGS_remote_cluster, FLAGS_table_name);

    test::data_verifier verifier(insert_client, verify_client, "duplication_test_");
    verifier.run_pipeline();

    test::data_verifier::histogram_printer.run_pipeline();

    return 0;
}
