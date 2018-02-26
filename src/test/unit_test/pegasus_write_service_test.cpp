// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "base/pegasus_key_schema.h"
#include "pegasus_server_test_base.h"

#include <dsn/dist/replication/fmt_logging.h>

namespace pegasus {
namespace server {

class pegasus_write_service_test : public pegasus_server_test_base
{
public:
    pegasus_write_service_test() : pegasus_server_test_base()
    {
        _write_svc = _server->_write_svc.get();
    }

    void test_multi_put()
    {
        dsn::apps::multi_put_request request;
        dsn::apps::update_response response;

        int64_t decree = 10;
        std::string hash_key = "hash_key";

        // alarm for empty request
        request.hash_key = dsn::blob(hash_key.data(), 0, hash_key.size());
        auto put_ctx = db_write_context::put(decree, 1000, 1);
        ASSERT_EQ(_write_svc->multi_put(put_ctx, request, response),
                  rocksdb::Status::kInvalidArgument);

        constexpr int kv_num = 100;
        std::string sort_key[kv_num];
        std::string value[kv_num];

        for (int i = 0; i < 100; i++) {
            sort_key[i] = "sort_key_" + std::to_string(i);
            value[i] = "value_" + std::to_string(i);
        }

        for (int i = 0; i < 100; i++) {
            request.kvs.emplace_back();
            request.kvs.back().key.assign(sort_key[i].data(), 0, sort_key[i].size());
            request.kvs.back().value.assign(value[i].data(), 0, value[i].size());
        }

        ASSERT_EQ(_write_svc->multi_put(put_ctx, request, response), 0);

        ASSERT_EQ(response.error, 0);
        ASSERT_EQ(response.app_id, _gpid.get_app_id());
        ASSERT_EQ(response.partition_index, _gpid.get_partition_index());
        ASSERT_EQ(response.decree, decree);
    }

    void test_multi_remove()
    {
        dsn::apps::multi_remove_request request;
        dsn::apps::multi_remove_response response;

        int64_t decree = 10;
        std::string hash_key = "hash_key";

        // alarm for empty request
        request.hash_key = dsn::blob(hash_key.data(), 0, hash_key.size());
        auto remove_ctx = db_write_context::remove(decree, 1000, 1);
        ASSERT_EQ(_write_svc->multi_remove(remove_ctx, request, response),
                  rocksdb::Status::kInvalidArgument);

        constexpr int kv_num = 100;
        std::string sort_key[kv_num];

        for (int i = 0; i < 100; i++) {
            sort_key[i] = "sort_key_" + std::to_string(i);
        }

        for (int i = 0; i < 100; i++) {
            request.sort_keys.emplace_back();
            request.sort_keys.back().assign(sort_key[i].data(), 0, sort_key[i].size());
        }

        ASSERT_EQ(_write_svc->multi_remove(remove_ctx, request, response), 0);

        ASSERT_EQ(response.error, 0);
        ASSERT_EQ(response.app_id, _gpid.get_app_id());
        ASSERT_EQ(response.partition_index, _gpid.get_partition_index());
        ASSERT_EQ(response.decree, decree);
    }

    void test_batched_writes()
    {
        int64_t decree = 10;
        std::string hash_key = "hash_key";

        auto put_ctx = db_write_context::put(decree, 1000, 1);
        auto remove_ctx = db_write_context::remove(decree, 1000, 1);

        constexpr int kv_num = 100;
        dsn::blob key[kv_num];
        std::string value[kv_num];

        for (int i = 0; i < 100; i++) {
            std::string sort_key = "sort_key_" + std::to_string(i);
            pegasus::pegasus_generate_key(key[i], hash_key, sort_key);

            value[i] = "value_" + std::to_string(i);
        }

        dsn::apps::update_response response;
        {
            _write_svc->batch_prepare();
            for (const auto &k : key) {
                dsn::apps::update_request req;
                req.key = k;
                _write_svc->batch_put(put_ctx, req);
                _write_svc->batch_remove(remove_ctx, k);
            }
            _write_svc->batch_commit(decree, response);
        }

        ASSERT_EQ(response.error, 0);
        ASSERT_EQ(response.app_id, _gpid.get_app_id());
        ASSERT_EQ(response.partition_index, _gpid.get_partition_index());
        ASSERT_EQ(response.decree, decree);
    }

protected:
    pegasus_write_service *_write_svc;
};

TEST_F(pegasus_write_service_test, multi_put) { test_multi_put(); }

TEST_F(pegasus_write_service_test, multi_remove) { test_multi_remove(); }

TEST_F(pegasus_write_service_test, batched_writes) { test_batched_writes(); }

TEST_F(pegasus_write_service_test, empty_put)
{
    auto put_ctx = db_write_context::put(10, 1000, 1);
    ASSERT_EQ(_write_svc->empty_put(put_ctx), 0);
}

} // namespace server
} // namespace pegasus
