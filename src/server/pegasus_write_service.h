// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/cpp/perf_counter_wrapper.h>

#include "base/pegasus_value_schema.h"
#include "base/string_view.h"
#include "rrdb/rrdb_types.h"

namespace pegasus {
namespace server {

// The context of an update to the database.
struct db_write_context
{
    int64_t decree;          // the mutation decree
    uint64_t timetag;        // the timetag calculated by the timestamp of this write
    uint64_t remote_timetag; // timetag of the remote write, 0 if it's not from remote.

    db_write_context() : decree(0), timetag(0), remote_timetag(0) {}

    static inline db_write_context put(int64_t d, uint64_t ts, uint8_t cid)
    {
        return create(d, ts, cid, false);
    }

    static inline db_write_context remove(int64_t d, uint64_t ts, uint8_t cid)
    {
        return create(d, ts, cid, true);
    }

private:
    static inline db_write_context
    create(int64_t decree, uint64_t timestamp, uint8_t cluster_id, bool delete_tag)
    {
        db_write_context ctx;
        ctx.decree = decree;
        ctx.timetag = pegasus::generate_timetag(timestamp, cluster_id, delete_tag);
        return ctx;
    }
};

class pegasus_server_impl;

// Handle the write requests.
// As the signatures imply, this class is not responsible for replying the rpc,
// the caller(pegasus_server_impl) should do it.
class pegasus_write_service
{
public:
    explicit pegasus_write_service(pegasus_server_impl *server);

    ~pegasus_write_service();

    int multi_put(const db_write_context &ctx,
                  const dsn::apps::multi_put_request &update,
                  dsn::apps::update_response &resp);

    int multi_remove(const db_write_context &ctx,
                     const dsn::apps::multi_remove_request &update,
                     dsn::apps::multi_remove_response &resp);

    void batch_prepare();

    void batch_put(const db_write_context &ctx, const dsn::apps::update_request &update);

    void batch_remove(const db_write_context &ctx, const dsn::blob &key);

    int batch_commit(int64_t decree, dsn::apps::update_response &resp);

    /// Inserts an empty record into database.
    int empty_put(const db_write_context &ctx);

private:
    friend class pegasus_write_service_test;

    class impl;
    std::unique_ptr<impl> _impl;

    uint64_t _batch_start_time;

    ::dsn::perf_counter_wrapper _pfc_put_qps;
    ::dsn::perf_counter_wrapper _pfc_multi_put_qps;
    ::dsn::perf_counter_wrapper _pfc_remove_qps;
    ::dsn::perf_counter_wrapper _pfc_multi_remove_qps;

    ::dsn::perf_counter_wrapper _pfc_put_latency;
    ::dsn::perf_counter_wrapper _pfc_multi_put_latency;
    ::dsn::perf_counter_wrapper _pfc_remove_latency;
    ::dsn::perf_counter_wrapper _pfc_multi_remove_latency;

    std::vector<::dsn::perf_counter *> _batch_perfcounters;
};

} // namespace server
} // namespace pegasus
