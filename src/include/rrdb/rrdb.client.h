#pragma once
#include "rrdb.code.definition.h"
#include "rrdb.types.h"
#include <iostream>
#include <dsn/utility/optional.h>
#include <dsn/tool-api/task_tracker.h>

namespace dsn {
namespace apps {
class rrdb_client
{
public:
    rrdb_client() {}
    explicit rrdb_client(::dsn::rpc_address server) { _server = server; }
    ~rrdb_client() { _tracker.cancel_outstanding_tasks(); }

    // ---------- call RPC_RRDB_RRDB_PUT ------------
    // - synchronous
    std::pair<::dsn::error_code, update_response>
    put_sync(const update_request &args,
             std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
             int thread_hash = 0, // if thread_hash == 0 && partition_hash != 0, thread_hash is
                                  // computed from partition_hash
             uint64_t partition_hash = 0,
             dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<update_response>(
            ::dsn::rpc::call(server_addr.unwrap_or(_server),
                             RPC_RRDB_RRDB_PUT,
                             args,
                             &_tracker,
                             empty_rpc_handler,
                             timeout,
                             thread_hash,
                             partition_hash));
    }

    // - asynchronous with on-stack update_request and update_response
    template <typename TCallback>
    ::dsn::task_ptr put(const update_request &args,
                        TCallback &&callback,
                        std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                        int request_thread_hash = 0, // if thread_hash == 0 && partition_hash != 0,
                                                     // thread_hash is computed from partition_hash
                        uint64_t request_partition_hash = 0,
                        int reply_thread_hash = 0,
                        dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::call(server_addr.unwrap_or(_server),
                                RPC_RRDB_RRDB_PUT,
                                args,
                                &_tracker,
                                std::forward<TCallback>(callback),
                                timeout,
                                request_thread_hash,
                                request_partition_hash,
                                reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_MULTI_PUT ------------
    // - synchronous
    std::pair<::dsn::error_code, update_response>
    multi_put_sync(const multi_put_request &args,
                   std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                   int thread_hash = 0, // if thread_hash == 0 && partition_hash != 0, thread_hash
                                        // is computed from partition_hash
                   uint64_t partition_hash = 0,
                   dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<update_response>(
            ::dsn::rpc::call(server_addr.unwrap_or(_server),
                             RPC_RRDB_RRDB_MULTI_PUT,
                             args,
                             &_tracker,
                             empty_rpc_handler,
                             timeout,
                             thread_hash,
                             partition_hash));
    }

    // - asynchronous with on-stack multi_put_request and update_response
    template <typename TCallback>
    ::dsn::task_ptr multi_put(const multi_put_request &args,
                              TCallback &&callback,
                              std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                              int request_thread_hash = 0, // if thread_hash == 0 && partition_hash
                                                           // != 0, thread_hash is computed from
                                                           // partition_hash
                              uint64_t request_partition_hash = 0,
                              int reply_thread_hash = 0,
                              dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::call(server_addr.unwrap_or(_server),
                                RPC_RRDB_RRDB_MULTI_PUT,
                                args,
                                &_tracker,
                                std::forward<TCallback>(callback),
                                timeout,
                                request_thread_hash,
                                request_partition_hash,
                                reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_REMOVE ------------
    // - synchronous
    std::pair<::dsn::error_code, update_response>
    remove_sync(const ::dsn::blob &args,
                std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                int thread_hash = 0, // if thread_hash == 0 && partition_hash != 0, thread_hash is
                                     // computed from partition_hash
                uint64_t partition_hash = 0,
                dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<update_response>(
            ::dsn::rpc::call(server_addr.unwrap_or(_server),
                             RPC_RRDB_RRDB_REMOVE,
                             args,
                             &_tracker,
                             empty_rpc_handler,
                             timeout,
                             thread_hash,
                             partition_hash));
    }

    // - asynchronous with on-stack ::dsn::blob and update_response
    template <typename TCallback>
    ::dsn::task_ptr remove(const ::dsn::blob &args,
                           TCallback &&callback,
                           std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                           int request_thread_hash = 0, // if thread_hash == 0 && partition_hash !=
                                                        // 0, thread_hash is computed from
                                                        // partition_hash
                           uint64_t request_partition_hash = 0,
                           int reply_thread_hash = 0,
                           dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::call(server_addr.unwrap_or(_server),
                                RPC_RRDB_RRDB_REMOVE,
                                args,
                                &_tracker,
                                std::forward<TCallback>(callback),
                                timeout,
                                request_thread_hash,
                                request_partition_hash,
                                reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_MULTI_REMOVE ------------
    // - synchronous
    std::pair<::dsn::error_code, multi_remove_response>
    multi_remove_sync(const multi_remove_request &args,
                      std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                      int thread_hash = 0, // if thread_hash == 0 && partition_hash != 0,
                                           // thread_hash is computed from partition_hash
                      uint64_t partition_hash = 0,
                      dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<multi_remove_response>(
            ::dsn::rpc::call(server_addr.unwrap_or(_server),
                             RPC_RRDB_RRDB_MULTI_REMOVE,
                             args,
                             &_tracker,
                             empty_rpc_handler,
                             timeout,
                             thread_hash,
                             partition_hash));
    }

    // - asynchronous with on-stack multi_remove_request and multi_remove_response
    template <typename TCallback>
    ::dsn::task_ptr multi_remove(const multi_remove_request &args,
                                 TCallback &&callback,
                                 std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                                 int request_thread_hash = 0, // if thread_hash == 0 &&
                                                              // partition_hash != 0, thread_hash is
                                                              // computed from partition_hash
                                 uint64_t request_partition_hash = 0,
                                 int reply_thread_hash = 0,
                                 dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::call(server_addr.unwrap_or(_server),
                                RPC_RRDB_RRDB_MULTI_REMOVE,
                                args,
                                &_tracker,
                                std::forward<TCallback>(callback),
                                timeout,
                                request_thread_hash,
                                request_partition_hash,
                                reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_INCR ------------
    // - synchronous
    std::pair<::dsn::error_code, incr_response>
    incr_sync(const incr_request &args,
              std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
              int thread_hash = 0, // if thread_hash == 0 && partition_hash != 0,
                                   // thread_hash is computed from partition_hash
              uint64_t partition_hash = 0,
              dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<incr_response>(
            ::dsn::rpc::call(server_addr.unwrap_or(_server),
                             RPC_RRDB_RRDB_INCR,
                             args,
                             &_tracker,
                             empty_rpc_handler,
                             timeout,
                             thread_hash,
                             partition_hash));
    }

    // - asynchronous with on-stack incr_request and incr_response
    template <typename TCallback>
    ::dsn::task_ptr incr(const incr_request &args,
                         TCallback &&callback,
                         std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                         int request_thread_hash = 0, // if thread_hash == 0 &&
                                                      // partition_hash != 0, thread_hash is
                                                      // computed from partition_hash
                         uint64_t request_partition_hash = 0,
                         int reply_thread_hash = 0,
                         dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::call(server_addr.unwrap_or(_server),
                                RPC_RRDB_RRDB_INCR,
                                args,
                                &_tracker,
                                std::forward<TCallback>(callback),
                                timeout,
                                request_thread_hash,
                                request_partition_hash,
                                reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_CHECK_AND_SET ------------
    // - synchronous
    std::pair<::dsn::error_code, check_and_set_response>
    check_and_set_sync(const check_and_set_request &args,
                       std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                       int thread_hash = 0, // if thread_hash == 0 && partition_hash != 0,
                                            // thread_hash is computed from partition_hash
                       uint64_t partition_hash = 0,
                       dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<check_and_set_response>(
            ::dsn::rpc::call(server_addr.unwrap_or(_server),
                             RPC_RRDB_RRDB_CHECK_AND_SET,
                             args,
                             &_tracker,
                             empty_rpc_handler,
                             timeout,
                             thread_hash,
                             partition_hash));
    }

    // - asynchronous with on-stack check_and_set_request and check_and_set_response
    template <typename TCallback>
    ::dsn::task_ptr check_and_set(const check_and_set_request &args,
                                  TCallback &&callback,
                                  std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                                  int request_thread_hash = 0, // if thread_hash == 0 &&
                                                               // partition_hash != 0, thread_hash
                                                               // is computed from partition_hash
                                  uint64_t request_partition_hash = 0,
                                  int reply_thread_hash = 0,
                                  dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::call(server_addr.unwrap_or(_server),
                                RPC_RRDB_RRDB_CHECK_AND_SET,
                                args,
                                &_tracker,
                                std::forward<TCallback>(callback),
                                timeout,
                                request_thread_hash,
                                request_partition_hash,
                                reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_GET ------------
    // - synchronous
    std::pair<::dsn::error_code, read_response>
    get_sync(const ::dsn::blob &args,
             std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
             int thread_hash = 0, // if thread_hash == 0 && partition_hash != 0, thread_hash is
                                  // computed from partition_hash
             uint64_t partition_hash = 0,
             dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<read_response>(
            ::dsn::rpc::call(server_addr.unwrap_or(_server),
                             RPC_RRDB_RRDB_GET,
                             args,
                             &_tracker,
                             empty_rpc_handler,
                             timeout,
                             thread_hash,
                             partition_hash));
    }

    // - asynchronous with on-stack ::dsn::blob and read_response
    template <typename TCallback>
    ::dsn::task_ptr get(const ::dsn::blob &args,
                        TCallback &&callback,
                        std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                        int request_thread_hash = 0, // if thread_hash == 0 && partition_hash != 0,
                                                     // thread_hash is computed from partition_hash
                        uint64_t request_partition_hash = 0,
                        int reply_thread_hash = 0,
                        dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::call(server_addr.unwrap_or(_server),
                                RPC_RRDB_RRDB_GET,
                                args,
                                &_tracker,
                                std::forward<TCallback>(callback),
                                timeout,
                                request_thread_hash,
                                request_partition_hash,
                                reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_MULTI_GET ------------
    // - synchronous
    std::pair<::dsn::error_code, multi_get_response>
    multi_get_sync(const multi_get_request &args,
                   std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                   int thread_hash = 0, // if thread_hash == 0 && partition_hash != 0, thread_hash
                                        // is computed from partition_hash
                   uint64_t partition_hash = 0,
                   dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<multi_get_response>(
            ::dsn::rpc::call(server_addr.unwrap_or(_server),
                             RPC_RRDB_RRDB_MULTI_GET,
                             args,
                             &_tracker,
                             empty_rpc_handler,
                             timeout,
                             thread_hash,
                             partition_hash));
    }

    // - asynchronous with on-stack multi_get_request and multi_get_response
    template <typename TCallback>
    ::dsn::task_ptr multi_get(const multi_get_request &args,
                              TCallback &&callback,
                              std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                              int request_thread_hash = 0, // if thread_hash == 0 && partition_hash
                                                           // != 0, thread_hash is computed from
                                                           // partition_hash
                              uint64_t request_partition_hash = 0,
                              int reply_thread_hash = 0,
                              dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::call(server_addr.unwrap_or(_server),
                                RPC_RRDB_RRDB_MULTI_GET,
                                args,
                                &_tracker,
                                std::forward<TCallback>(callback),
                                timeout,
                                request_thread_hash,
                                request_partition_hash,
                                reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_SORTKEY_COUNT ------------
    // - synchronous
    std::pair<::dsn::error_code, count_response>
    sortkey_count_sync(const ::dsn::blob &args,
                       std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                       int thread_hash = 0, // if thread_hash == 0 && partition_hash != 0,
                                            // thread_hash is computed from partition_hash
                       uint64_t partition_hash = 0,
                       dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<count_response>(
            ::dsn::rpc::call(server_addr.unwrap_or(_server),
                             RPC_RRDB_RRDB_SORTKEY_COUNT,
                             args,
                             &_tracker,
                             empty_rpc_handler,
                             timeout,
                             thread_hash,
                             partition_hash));
    }

    // - asynchronous with on-stack ::dsn::blob and count_response
    template <typename TCallback>
    ::dsn::task_ptr sortkey_count(const ::dsn::blob &args,
                                  TCallback &&callback,
                                  std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                                  int request_thread_hash = 0, // if thread_hash == 0 &&
                                                               // partition_hash != 0, thread_hash
                                                               // is computed from partition_hash
                                  uint64_t request_partition_hash = 0,
                                  int reply_thread_hash = 0,
                                  dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::call(server_addr.unwrap_or(_server),
                                RPC_RRDB_RRDB_SORTKEY_COUNT,
                                args,
                                &_tracker,
                                std::forward<TCallback>(callback),
                                timeout,
                                request_thread_hash,
                                request_partition_hash,
                                reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_TTL ------------
    // - synchronous
    std::pair<::dsn::error_code, ttl_response>
    ttl_sync(const ::dsn::blob &args,
             std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
             int thread_hash = 0, // if thread_hash == 0 && partition_hash != 0, thread_hash is
                                  // computed from partition_hash
             uint64_t partition_hash = 0,
             dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<ttl_response>(
            ::dsn::rpc::call(server_addr.unwrap_or(_server),
                             RPC_RRDB_RRDB_TTL,
                             args,
                             &_tracker,
                             empty_rpc_handler,
                             timeout,
                             thread_hash,
                             partition_hash));
    }

    // - asynchronous with on-stack ::dsn::blob and ttl_response
    template <typename TCallback>
    ::dsn::task_ptr ttl(const ::dsn::blob &args,
                        TCallback &&callback,
                        std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                        int request_thread_hash = 0, // if thread_hash == 0 && partition_hash != 0,
                                                     // thread_hash is computed from partition_hash
                        uint64_t request_partition_hash = 0,
                        int reply_thread_hash = 0,
                        dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::call(server_addr.unwrap_or(_server),
                                RPC_RRDB_RRDB_TTL,
                                args,
                                &_tracker,
                                std::forward<TCallback>(callback),
                                timeout,
                                request_thread_hash,
                                request_partition_hash,
                                reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_GET_SCANNER ------------
    // - synchronous
    std::pair<::dsn::error_code, scan_response>
    get_scanner_sync(const get_scanner_request &args,
                     std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                     int thread_hash = 0, // if thread_hash == 0 && partition_hash != 0, thread_hash
                                          // is computed from partition_hash
                     uint64_t partition_hash = 0,
                     dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<scan_response>(
            ::dsn::rpc::call(server_addr.unwrap_or(_server),
                             RPC_RRDB_RRDB_GET_SCANNER,
                             args,
                             &_tracker,
                             empty_rpc_handler,
                             timeout,
                             thread_hash,
                             partition_hash));
    }

    // - asynchronous with on-stack get_scanner_request and scan_response
    template <typename TCallback>
    ::dsn::task_ptr get_scanner(const get_scanner_request &args,
                                TCallback &&callback,
                                std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                                int request_thread_hash = 0, // if thread_hash == 0 &&
                                                             // partition_hash != 0, thread_hash is
                                                             // computed from partition_hash
                                uint64_t request_partition_hash = 0,
                                int reply_thread_hash = 0,
                                dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::call(server_addr.unwrap_or(_server),
                                RPC_RRDB_RRDB_GET_SCANNER,
                                args,
                                &_tracker,
                                std::forward<TCallback>(callback),
                                timeout,
                                request_thread_hash,
                                request_partition_hash,
                                reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_SCAN ------------
    // - synchronous
    std::pair<::dsn::error_code, scan_response>
    scan_sync(const scan_request &args,
              std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
              int thread_hash = 0, // if thread_hash == 0 && partition_hash != 0, thread_hash is
                                   // computed from partition_hash
              uint64_t partition_hash = 0,
              dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<scan_response>(
            ::dsn::rpc::call(server_addr.unwrap_or(_server),
                             RPC_RRDB_RRDB_SCAN,
                             args,
                             &_tracker,
                             empty_rpc_handler,
                             timeout,
                             thread_hash,
                             partition_hash));
    }

    // - asynchronous with on-stack scan_request and scan_response
    template <typename TCallback>
    ::dsn::task_ptr scan(const scan_request &args,
                         TCallback &&callback,
                         std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                         int request_thread_hash = 0, // if thread_hash == 0 && partition_hash != 0,
                                                      // thread_hash is computed from partition_hash
                         uint64_t request_partition_hash = 0,
                         int reply_thread_hash = 0,
                         dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::call(server_addr.unwrap_or(_server),
                                RPC_RRDB_RRDB_SCAN,
                                args,
                                &_tracker,
                                std::forward<TCallback>(callback),
                                timeout,
                                request_thread_hash,
                                request_partition_hash,
                                reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_CLEAR_SCANNER ------------
    void clear_scanner(const int64_t &args,
                       int thread_hash = 0, // if thread_hash == 0 && partition_hash != 0,
                                            // thread_hash is computed from partition_hash
                       uint64_t partition_hash = 0,
                       dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        ::dsn::rpc::call_one_way_typed(server_addr.unwrap_or(_server),
                                       RPC_RRDB_RRDB_CLEAR_SCANNER,
                                       args,
                                       thread_hash,
                                       partition_hash);
    }

private:
    ::dsn::rpc_address _server;
    dsn::task_tracker _tracker;
};
}
}
