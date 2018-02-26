// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/dist/replication/fmt_logging.h>
#include <dsn/cpp/address.h>
#include <dsn/dist/replication/replication_other_types.h>

namespace pegasus {
namespace test {

inline std::vector<dsn::rpc_address> list_meta(const std::string &pegasus_cluster_name)
{
    std::vector<dsn::rpc_address> meta_list;
    std::string tmp_section = "uri-resolver.dsn://" + pegasus_cluster_name;
    dsn::replication::replica_helper::load_meta_servers(
        meta_list, tmp_section.c_str(), "arguments");
    if (meta_list.empty()) {
        dfatal("no meta address is configured for %s", pegasus_cluster_name.c_str());
    }
    return meta_list;
}

} // namespace test
} // namespace pegasus
