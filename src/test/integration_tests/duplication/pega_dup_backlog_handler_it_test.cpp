// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/utility/message_utils.h>
#include <dsn/dist/replication/mutation_timestamp.h>

#include "server/pegasus_duplication_backlog_handler.h"
#include "test/integration_tests/integration_test_base.h"

using namespace pegasus::test;

namespace pegasus {
namespace server {

class pg_duplication_backlog_handler_test : public integration_test_base
{
public:
    pg_duplication_backlog_handler_test() : sender("onebox", "temp")
    {
        app_client = sender._client;
    }

    void loop_to_duplicate(uint64_t timestamp, dsn_message_t msg, dsn::blob data)
    {
        sender.duplicate(std::make_tuple(timestamp, msg, data),
                         [this, timestamp, msg, data](dsn::error_s err) {
                             if (err.code() == dsn::ERR_TIMEOUT) {
                                 derror_f("duplicate timeout");
                                 loop_to_duplicate(timestamp, msg, data);
                             } else {
                                 ASSERT_TRUE(err.is_ok()) << err.description();
                             }
                         });
    }

protected:
    pegasus_duplication_backlog_handler sender;
    pegasus::client::pegasus_client_impl *app_client;
};

TEST_F(pg_duplication_backlog_handler_test, simple_duplicate)
{
    dsn::apps::update_request req;

    std::string hash_key("hash");
    std::string sort_key("sort");
    std::string value("value");

    for (int i = 0; i < 100; i++) {
        std::string hash_key_i = hash_key + std::to_string(i);
        std::string sort_key_i = sort_key + std::to_string(i);
        std::string value_i = value + std::to_string(i);

        pegasus::pegasus_generate_key(req.key, hash_key_i, sort_key_i);
        req.value.assign(value_i.data(), 0, value_i.length());

        dsn_message_t msg =
            dsn::from_thrift_request_to_received_message(req, dsn::apps::RPC_RRDB_RRDB_PUT);

        dsn::blob data = dsn::move_message_to_blob(msg);

        loop_to_duplicate(static_cast<uint64_t>(100 + i), msg, data);
    }
    sender.wait_all();

    for (int i = 0; i < 100; i++) {
        std::string hash_key_i = hash_key + std::to_string(i);
        std::string sort_key_i = sort_key + std::to_string(i);
        std::string value_i = value + std::to_string(i);

        std::string remote_value;
        ASSERT_EQ(app_client->get(hash_key_i, sort_key_i, remote_value), 0) << hash_key_i << " "
                                                                            << sort_key_i;
        ASSERT_EQ(remote_value, value_i);
    }
}

TEST_F(pg_duplication_backlog_handler_test, confliction)
{
    dsn::apps::multi_put_request req;

    std::string hash_key("hash");
    std::string sort_key("sort");
    std::string value("value");

    req.hash_key.assign(hash_key.data(), 0, hash_key.length());
    req.kvs.emplace_back();
    req.kvs.back().key.assign(sort_key.data(), 0, sort_key.length());
    req.kvs.back().value.assign(value.data(), 0, value.length());
    dsn_message_t msg =
        dsn::from_thrift_request_to_received_message(req, dsn::apps::RPC_RRDB_RRDB_MULTI_PUT);
    dsn::blob data = dsn::move_message_to_blob(msg);

    // write in normal
    std::string value2 = value + "2";
    std::string remote_value;
    int err = app_client->set(hash_key, sort_key, value2, 10 * 1000);
    ASSERT_EQ(err, 0) << app_client->get_error_string(err);
    err = app_client->get(hash_key, sort_key, remote_value);
    ASSERT_EQ(err, 0) << app_client->get_error_string(err);
    ASSERT_EQ(remote_value, value2);

    // duplicating a small-timestamped update
    loop_to_duplicate(uint64_t(100), msg, data);
    sender.wait_all();

    // larger timestamp wins, even though the duplicated is newer.
    app_client->get(hash_key, sort_key, remote_value);
    ASSERT_EQ(remote_value, value2);

    // duplicating a large timestamp, this number(4 * 10^15) is certainly
    // larger than normal timestamp.
    loop_to_duplicate(uint64_t(4) * 100000 * 100000 * 100000, msg, data);
    sender.wait_all();

    // the larger timestamp wins
    app_client->set(hash_key, sort_key, value2, 10 * 1000);
    app_client->get(hash_key, sort_key, remote_value);
    ASSERT_EQ(remote_value, value);
}

} // namespace server
} // namespace pegasus
