// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/dist/replication/duplication_backlog_handler.h>
#include <rrdb/rrdb.code.definition.h>

#include "client_lib/pegasus_client_factory_impl.h"

namespace pegasus {
namespace server {

// Duplicates the loaded mutations to the remote pegasus cluster.
class pegasus_duplication_backlog_handler : public dsn::replication::duplication_backlog_handler
{
public:
    explicit pegasus_duplication_backlog_handler(const std::string &remote_cluster,
                                                 const std::string &app)
    {
        pegasus_client *client =
            pegasus_client_factory::get_client(remote_cluster.c_str(), app.c_str());
        _client = static_cast<client::pegasus_client_impl *>(client);

        _cluster_id = static_cast<uint8_t>(dsn_config_get_value_uint64(
            "pegasus.server", "pegasus_cluster_id", 1, "The ID of this pegasus cluster."));
    }

    void duplicate(dsn::replication::mutation_tuple mutation, err_callback cb) override
    {
        send_request(std::get<0>(mutation), std::get<1>(mutation), std::get<2>(mutation), cb);
    }

    void wait_all() { _client->wait_all_tasks(); }

    void send_request(uint64_t timestamp, dsn_message_t req, dsn::blob data, err_callback cb);

private:
    friend class pg_duplication_backlog_handler_test;

    client::pegasus_client_impl *_client;
    uint8_t _cluster_id;
};

class pegasus_duplication_backlog_handler_factory
    : public dsn::replication::duplication_backlog_handler_factory
{
    using dup_handler = dsn::replication::duplication_backlog_handler;

public:
    std::unique_ptr<dup_handler> create(const std::string &remote, const std::string &app) override
    {
        return dsn::make_unique<pegasus_duplication_backlog_handler>(remote, app);
    }
};

extern uint64_t get_hash_from_request(dsn::task_code rpc_code, const dsn::blob &data);

} // namespace server
} // namespace pegasus
