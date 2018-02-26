// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "key_ttl_compaction_filter.h"
#include "pegasus_scan_context.h"
#include "pegasus_write_service.h"

#include "base/string_view.h"
#include "base/pegasus_value_schema.h"
#include "base/pegasus_rpc_types.h"

#include <rocksdb/db.h>
#include <rrdb/rrdb.server.h>
#include <vector>
#include <dsn/cpp/perf_counter_wrapper.h>
#include <dsn/dist/replication/replication.codes.h>

namespace pegasus {
namespace server {

class pegasus_server_impl : public ::dsn::apps::rrdb_service
{
public:
    static void register_service()
    {
        replication_app_base::register_storage_engine(
            "pegasus", replication_app_base::create<pegasus::server::pegasus_server_impl>);
        register_rpc_handlers();
    }
    explicit pegasus_server_impl(dsn::replication::replica *r);
    virtual ~pegasus_server_impl() {}

    // the following methods may set physical error if internal error occurs
    virtual void on_get(const ::dsn::blob &key,
                        ::dsn::rpc_replier<::dsn::apps::read_response> &reply) override;
    virtual void on_multi_get(const ::dsn::apps::multi_get_request &args,
                              ::dsn::rpc_replier<::dsn::apps::multi_get_response> &reply) override;
    virtual void on_sortkey_count(const ::dsn::blob &args,
                                  ::dsn::rpc_replier<::dsn::apps::count_response> &reply) override;
    virtual void on_ttl(const ::dsn::blob &key,
                        ::dsn::rpc_replier<::dsn::apps::ttl_response> &reply) override;
    virtual void on_get_scanner(const ::dsn::apps::get_scanner_request &args,
                                ::dsn::rpc_replier<::dsn::apps::scan_response> &reply) override;
    virtual void on_scan(const ::dsn::apps::scan_request &args,
                         ::dsn::rpc_replier<::dsn::apps::scan_response> &reply) override;
    virtual void on_clear_scanner(const int64_t &args) override;

    // input:
    //  - argc = 0 : re-open the db
    //  - argc = 2n + 1, n >= 0; normal open the db
    // returns:
    //  - ERR_OK
    //  - ERR_FILE_OPERATION_FAILED
    //  - ERR_LOCAL_APP_FAILURE
    virtual ::dsn::error_code start(int argc, char **argv) override;

    // returns:
    //  - ERR_OK
    //  - ERR_FILE_OPERATION_FAILED
    virtual ::dsn::error_code stop(bool clear_state) override;

    /// Each of the write request (specifically, the rpc that's configured as write, see
    /// option `rpc_request_is_write_operation` in the configuration file) will first be
    /// replicated to the replicas through the underlying PacificA protocol in rDSN, and
    /// after being committed, the mutation will be applied into rocksdb by this function.
    ///
    /// \see dsn::replication::replication_app_base::apply_mutation
    /// \inherit dsn::replication::replication_app_base
    virtual int on_batched_write_requests(int64_t decree,
                                          uint64_t timestamp,
                                          dsn_message_t *requests,
                                          int count) override;

    virtual ::dsn::error_code prepare_get_checkpoint(dsn::blob &learn_req) override
    {
        return ::dsn::ERR_OK;
    }
    // returns:
    //  - ERR_OK
    //  - ERR_WRONG_TIMING
    //  - ERR_NO_NEED_OPERATE
    //  - ERR_LOCAL_APP_FAILURE
    //  - ERR_FILE_OPERATION_FAILED
    virtual ::dsn::error_code sync_checkpoint() override;

    // returns:
    //  - ERR_OK
    //  - ERR_WRONG_TIMING: is checkpointing now
    //  - ERR_NO_NEED_OPERATE: the checkpoint is fresh enough, no need to checkpoint
    //  - ERR_LOCAL_APP_FAILURE: some internal failure
    //  - ERR_FILE_OPERATION_FAILED: some file failure
    //  - ERR_TRY_AGAIN: need try again later
    virtual ::dsn::error_code async_checkpoint(bool is_emergency) override;

    //
    // copy the latest checkpoint to checkpoint_dir, and the decree of the checkpoint
    // copied will be assigned to checkpoint_decree if checkpoint_decree is not null.
    // if checkpoint_dir already exist, this function will delete it first.
    //
    // must be thread safe
    // this method will not trigger flush(), just copy even if the app is empty.
    virtual ::dsn::error_code copy_checkpoint_to_dir(const char *checkpoint_dir,
                                                     /*output*/ int64_t *last_decree) override;

    //
    // help function, just copy checkpoint to specified dir and ignore _is_checkpointing.
    // if checkpoint_dir already exist, this function will delete it first.
    ::dsn::error_code copy_checkpoint_to_dir_unsafe(const char *checkpoint_dir,
                                                    /**output*/ int64_t *checkpoint_decree);

    // get the last checkpoint
    // if succeed:
    //  - the checkpoint files path are put into "state.files"
    //  - the checkpoint_info are serialized into "state.meta"
    //  - the "state.from_decree_excluded" and "state.to_decree_excluded" are set properly
    // returns:
    //  - ERR_OK
    //  - ERR_OBJECT_NOT_FOUND
    //  - ERR_FILE_OPERATION_FAILED
    virtual ::dsn::error_code get_checkpoint(int64_t learn_start,
                                             const dsn::blob &learn_request,
                                             dsn::replication::learn_state &state) override;

    // apply checkpoint, this will clear and recreate the db
    // if succeed:
    //  - last_committed_decree() == last_durable_decree()
    // returns:
    //  - ERR_OK
    //  - ERR_FILE_OPERATION_FAILED
    //  - error code of close()
    //  - error code of open()
    //  - error code of checkpoint()
    virtual ::dsn::error_code
    storage_apply_checkpoint(chkpt_apply_mode mode,
                             const dsn::replication::learn_state &state) override;

    virtual int64_t last_durable_decree() const { return _last_durable_decree.load(); }

    // The cluster id of this pegasus cluster.
    uint8_t cluster_id() const { return _cluster_id; }

    inline bool check_if_record_expired(uint32_t epoch_now, rocksdb::Slice raw_value)
    {
        return pegasus::check_if_record_expired(
            _value_schema_version, epoch_now, to_string_view(raw_value));
    }

    /// =============================================================== ///
    /// === Methods for implementation of on_batched_write_requests === ///
    /// =============================================================== ///

    int on_batched_write_requests_impl(dsn_message_t *requests,
                                       int count,
                                       int64_t decree,
                                       uint64_t timestamp);

    int on_multi_put(multi_put_rpc &rpc)
    {
        return _write_svc->multi_put(_put_ctx, rpc.request(), rpc.response());
    }

    int on_multi_remove(multi_remove_rpc &rpc)
    {
        return _write_svc->multi_remove(_remove_ctx, rpc.request(), rpc.response());
    }

    int on_duplicate(duplicate_rpc &rpc)
    {
        rpc.response().error = on_duplicate_impl(false, *rpc.mutable_request());
        return rpc.response().error;
    }

    int on_duplicate_impl(bool batched, dsn::apps::duplicate_request &request);

    /// Delay replying for the batched requests until all of them completes.
    int on_batched_writes(dsn_message_t *requests, int count, int64_t decree);

    void on_single_put_in_batch(const put_rpc &rpc)
    {
        _write_svc->batch_put(_put_ctx, rpc.request());
        request_key_check(_put_ctx.decree, rpc.dsn_request(), rpc.request().key);
    }

    void on_single_remove_in_batch(const remove_rpc &rpc)
    {
        _write_svc->batch_remove(_remove_ctx, rpc.request());
        request_key_check(_remove_ctx.decree, rpc.dsn_request(), rpc.request());
    }

    void on_single_duplicate_in_batch(const duplicate_rpc &rpc)
    {
        on_duplicate_impl(true, *rpc.mutable_request());
    }

    void request_key_check(int64_t decree, dsn_message_t m, const dsn::blob &key);

private:
    // parse checkpoint directories in the data dir
    // checkpoint directory format is: "checkpoint.{decree}"
    void parse_checkpoints();

    // garbage collection checkpoints
    void gc_checkpoints();

    void set_last_durable_decree(int64_t decree) { _last_durable_decree.store(decree); }

    // return 1 if value is appended
    // return 2 if value is expired
    // return 3 if value is filtered
    int append_key_value_for_scan(std::vector<::dsn::apps::key_value> &kvs,
                                  const rocksdb::Slice &key,
                                  const rocksdb::Slice &value,
                                  ::dsn::apps::filter_type::type hash_key_filter_type,
                                  const ::dsn::blob &hash_key_filter_pattern,
                                  ::dsn::apps::filter_type::type sort_key_filter_type,
                                  const ::dsn::blob &sort_key_filter_pattern,
                                  uint32_t epoch_now,
                                  bool no_value);

    // return 1 if value is appended
    // return 2 if value is expired
    // return 3 if value is filtered
    int append_key_value_for_multi_get(std::vector<::dsn::apps::key_value> &kvs,
                                       const rocksdb::Slice &key,
                                       const rocksdb::Slice &value,
                                       ::dsn::apps::filter_type::type sort_key_filter_type,
                                       const ::dsn::blob &sort_key_filter_pattern,
                                       uint32_t epoch_now,
                                       bool no_value);

    // return true if the filter type is supported
    bool is_filter_type_supported(::dsn::apps::filter_type::type filter_type);

    // return true if the data is valid for the filter
    bool validate_filter(::dsn::apps::filter_type::type filter_type,
                         const ::dsn::blob &filter_pattern,
                         const ::dsn::blob &value);

    // statistic the sst file info for this replica. return (-1,-1) if failed.
    std::pair<int64_t, int64_t> statistic_sst_size();

    void updating_rocksdb_sstsize();

    // get the absolute path of restore directory and the flag whether force restore from env
    // return
    //      std::pair<std::string, bool>, pair.first is the path of the restore dir; pair.second is
    //      the flag that whether force restore
    std::pair<std::string, bool> get_restore_dir_from_env(int argc, char **argv);

private:
    friend class pegasus_write_service;
    friend class pegasus_write_service_test;
    friend class pegasus_sever_impl_test;

    dsn::gpid _gpid;
    std::string _primary_address;
    bool _verbose_log;

    uint8_t _cluster_id;

    KeyWithTTLCompactionFilter _key_ttl_compaction_filter;
    rocksdb::Options _db_opts;
    rocksdb::WriteOptions _wt_opts;
    rocksdb::ReadOptions _rd_opts;

    rocksdb::DB *_db;
    volatile bool _is_open;
    uint32_t _value_schema_version;
    std::atomic<int64_t> _last_durable_decree;

    std::unique_ptr<pegasus_write_service> _write_svc;
    std::vector<put_rpc> _put_rpc_batch;
    std::vector<remove_rpc> _remove_rpc_batch;
    std::vector<duplicate_rpc> _batched_duplicate_rpc_batch;
    db_write_context _put_ctx;
    db_write_context _remove_ctx;

    int _physical_error;

    uint32_t _checkpoint_reserve_min_count;
    uint32_t _checkpoint_reserve_time_seconds;
    std::atomic_bool _is_checkpointing;         // whether the db is doing checkpoint
    ::dsn::utils::ex_lock_nr _checkpoints_lock; // protected the following checkpoints vector
    std::deque<int64_t> _checkpoints;           // ordered checkpoints

    pegasus_context_cache _context_cache;

    uint32_t _updating_rocksdb_sstsize_interval_seconds;
    ::dsn::task_ptr _updating_task;

    // perf counters
    ::dsn::perf_counter_wrapper _pfc_get_qps;
    ::dsn::perf_counter_wrapper _pfc_multi_get_qps;
    ::dsn::perf_counter_wrapper _pfc_scan_qps;

    ::dsn::perf_counter_wrapper _pfc_get_latency;
    ::dsn::perf_counter_wrapper _pfc_multi_get_latency;
    ::dsn::perf_counter_wrapper _pfc_scan_latency;

    ::dsn::perf_counter_wrapper _pfc_recent_expire_count;
    ::dsn::perf_counter_wrapper _pfc_recent_filter_count;
    ::dsn::perf_counter_wrapper _pfc_sst_count;
    ::dsn::perf_counter_wrapper _pfc_sst_size;
};
}
} // namespace
