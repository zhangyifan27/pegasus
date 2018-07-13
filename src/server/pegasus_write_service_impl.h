// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "pegasus_write_service.h"
#include "pegasus_server_impl.h"
#include "logging_utils.h"

#include "base/pegasus_key_schema.h"

#include <dsn/utility/fail_point.h>
#include <dsn/utility/string_conv.h>

namespace pegasus {
namespace server {

/// internal error codes used for fail injection
static constexpr int FAIL_DB_WRITE_BATCH_PUT = -101;
static constexpr int FAIL_DB_WRITE_BATCH_DELETE = -102;
static constexpr int FAIL_DB_WRITE = -103;

class pegasus_write_service::impl : public dsn::replication::replica_base
{
public:
    explicit impl(pegasus_server_impl *server)
        : replica_base(*server),
          _primary_address(server->_primary_address),
          _value_schema_version(server->_value_schema_version),
          _db(server->_db),
          _wt_opts(server->_wt_opts),
          _rd_opts(server->_rd_opts),
          _pfc_recent_expire_count(server->_pfc_recent_expire_count)
    {
    }

    int empty_put(int64_t decree)
    {
        int err = db_write_batch_put(decree, dsn::string_view(), dsn::string_view(), 0);
        if (err) {
            clear_up_batch_states(decree, err);
            return err;
        }

        err = db_write(decree);

        clear_up_batch_states(decree, err);
        return err;
    }

    int multi_put(int64_t decree,
                  const dsn::apps::multi_put_request &update,
                  dsn::apps::update_response &resp)
    {
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = decree;
        resp.server = _primary_address;

        if (update.kvs.empty()) {
            derror_replica("invalid argument for multi_put: decree = {}, error = {}",
                           decree,
                           "request.kvs is empty");
            resp.error = rocksdb::Status::kInvalidArgument;
            // we should write empty record to update rocksdb's last flushed decree
            return empty_put(decree);
        }

        for (auto &kv : update.kvs) {
            resp.error = db_write_batch_put(decree,
                                            composite_raw_key(update.hash_key, kv.key),
                                            kv.value,
                                            static_cast<uint32_t>(update.expire_ts_seconds));
            if (resp.error) {
                clear_up_batch_states(decree, resp.error);
                return resp.error;
            }
        }

        resp.error = db_write(decree);

        clear_up_batch_states(decree, resp.error);
        return resp.error;
    }

    int multi_remove(int64_t decree,
                     const dsn::apps::multi_remove_request &update,
                     dsn::apps::multi_remove_response &resp)
    {
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = decree;
        resp.server = _primary_address;

        if (update.sort_keys.empty()) {
            derror_replica("invalid argument for multi_remove: decree = {}, error = {}",
                           decree,
                           "request.sort_keys is empty");
            resp.error = rocksdb::Status::kInvalidArgument;
            // we should write empty record to update rocksdb's last flushed decree
            return empty_put(decree);
        }

        for (auto &sort_key : update.sort_keys) {
            resp.error =
                db_write_batch_delete(decree, composite_raw_key(update.hash_key, sort_key));
            if (resp.error) {
                clear_up_batch_states(decree, resp.error);
                return resp.error;
            }
        }

        resp.error = db_write(decree);
        if (resp.error == 0) {
            resp.count = update.sort_keys.size();
        }

        clear_up_batch_states(decree, resp.error);
        return resp.error;
    }

    int incr(int64_t decree, const dsn::apps::incr_request &update, dsn::apps::incr_response &resp)
    {
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = decree;
        resp.server = _primary_address;

        rocksdb::Slice raw_key(update.key.data(), update.key.length());
        uint32_t expire_ts = 0;
        std::string raw_value;
        int64_t new_value = 0;
        rocksdb::Status s = _db->Get(_rd_opts, raw_key, &raw_value);
        if (s.ok()) {
            expire_ts = pegasus_extract_expire_ts(_value_schema_version, raw_value);
            if (check_if_ts_expired(utils::epoch_now(), expire_ts)) {
                // ttl timeout, set to 0 before increment, and set expire_ts to 0
                _pfc_recent_expire_count->increment();
                new_value = update.increment;
                expire_ts = 0;
            } else {
                ::dsn::blob old_value;
                pegasus_extract_user_data(_value_schema_version, std::move(raw_value), old_value);
                if (old_value.length() == 0) {
                    // empty old value, set to 0 before increment
                    new_value = update.increment;
                } else {
                    int64_t old_value_int;
                    if (!dsn::buf2int64(old_value, old_value_int)) {
                        // invalid old value
                        derror_replica("incr failed: decree = {}, error = {}",
                                       decree,
                                       "old value is not an integer or out of range");
                        resp.error = rocksdb::Status::kInvalidArgument;
                        // we should write empty record to update rocksdb's last flushed decree
                        return empty_put(decree);
                    }
                    new_value = old_value_int + update.increment;
                    if ((update.increment > 0 && new_value < old_value_int) ||
                        (update.increment < 0 && new_value > old_value_int)) {
                        // new value is out of range, return old value by 'new_value'
                        derror_replica("incr failed: decree = {}, error = {}",
                                       decree,
                                       "new value is out of range");
                        resp.error = rocksdb::Status::kInvalidArgument;
                        resp.new_value = old_value_int;
                        // we should write empty record to update rocksdb's last flushed decree
                        return empty_put(decree);
                    }
                }
            }
        } else if (s.IsNotFound()) {
            // old value is not found, set to 0 before increment, and set expire_ts to 0
            new_value = update.increment;
            expire_ts = 0;
        } else {
            // read old value failed
            ::dsn::blob hash_key, sort_key;
            pegasus_restore_key(::dsn::blob(raw_key.data(), 0, raw_key.size()), hash_key, sort_key);
            derror_rocksdb("Get for Incr",
                           s.ToString(),
                           "decree: {}, hash_key: {}, sort_key: {}",
                           decree,
                           utils::c_escape_string(hash_key),
                           utils::c_escape_string(sort_key));
            resp.error = s.code();
            return resp.error;
        }

        resp.error = db_write_batch_put(decree, update.key, std::to_string(new_value), expire_ts);
        if (resp.error) {
            clear_up_batch_states(decree, resp.error);
            return resp.error;
        }

        resp.error = db_write(decree);
        if (resp.error == 0) {
            resp.new_value = new_value;
        }

        clear_up_batch_states(decree, resp.error);
        return resp.error;
    }

    int check_and_set(int64_t decree,
                      const dsn::apps::check_and_set_request &update,
                      dsn::apps::check_and_set_response &resp)
    {
        resp.app_id = get_gpid().get_app_id();
        resp.partition_index = get_gpid().get_partition_index();
        resp.decree = decree;
        resp.server = _primary_address;

        if (!is_check_type_supported(update.check_type)) {
            derror_replica("invalid argument for check_and_set: decree = {}, error = {}",
                           decree,
                           "check type {} not supported",
                           update.check_type);
            resp.error = rocksdb::Status::kInvalidArgument;
            // we should write empty record to update rocksdb's last flushed decree
            return empty_put(decree);
        }

        ::dsn::blob check_key;
        pegasus_generate_key(check_key, update.hash_key, update.check_sort_key);
        rocksdb::Slice check_raw_key(check_key.data(), check_key.length());
        std::string check_raw_value;
        rocksdb::Status s = _db->Get(_rd_opts, check_raw_key, &check_raw_value);
        if (s.ok()) {
            // read check value succeed
            if (check_if_record_expired(
                    _value_schema_version, utils::epoch_now(), check_raw_value)) {
                // check value ttl timeout
                _pfc_recent_expire_count->increment();
                s = rocksdb::Status::NotFound();
            }
        } else if (!s.IsNotFound()) {
            // read check value failed
            derror_rocksdb("GetCheckValue for CheckAndSet",
                           s.ToString(),
                           "decree: {}, hash_key: {}, check_sort_key: {}",
                           decree,
                           utils::c_escape_string(update.hash_key),
                           utils::c_escape_string(update.check_sort_key));
            resp.error = s.code();
            return resp.error;
        }
        dassert(s.ok() || s.IsNotFound(), "status = %s", s.ToString().c_str());

        ::dsn::blob check_value;
        if (s.ok()) {
            pegasus_extract_user_data(
                _value_schema_version, std::move(check_raw_value), check_value);
        }

        bool passed = validate_check(update.check_type, update.check_oprand, s.ok(), check_value);
        if (passed) {
            // check passed, write new value
            ::dsn::blob set_key;
            if (update.set_diff_sort_key) {
                pegasus_generate_key(set_key, update.hash_key, update.set_sort_key);
            } else {
                set_key = check_key;
            }
            resp.error = db_write_batch_put(decree,
                                            set_key,
                                            update.set_value,
                                            static_cast<uint32_t>(update.set_expire_ts_seconds));
        } else {
            // check not passed, write empty record to update rocksdb's last flushed decree
            resp.error = db_write_batch_put(decree, dsn::string_view(), dsn::string_view(), 0);
        }
        if (resp.error) {
            clear_up_batch_states(decree, resp.error);
            return resp.error;
        }

        resp.error = db_write(decree);
        if (resp.error) {
            clear_up_batch_states(decree, resp.error);
            return resp.error;
        }

        if (!passed) {
            // check not passed, return kTryAgain to user
            resp.error = rocksdb::Status::kTryAgain;
        }

        if (update.return_check_value) {
            resp.check_value_returned = true;
            if (s.ok()) {
                resp.check_value_exist = true;
                resp.check_value = std::move(check_value);
            }
        }

        clear_up_batch_states(decree, resp.error);
        return 0;
    }

    /// For batch write.

    int batch_put(int64_t decree,
                  const dsn::apps::update_request &update,
                  dsn::apps::update_response &resp)
    {
        resp.error = db_write_batch_put(
            decree, update.key, update.value, static_cast<uint32_t>(update.expire_ts_seconds));
        _update_responses.emplace_back(&resp);
        return resp.error;
    }

    int batch_remove(int64_t decree, const dsn::blob &key, dsn::apps::update_response &resp)
    {
        resp.error = db_write_batch_delete(decree, key);
        _update_responses.emplace_back(&resp);
        return resp.error;
    }

    int batch_commit(int64_t decree)
    {
        int err = db_write(decree);
        clear_up_batch_states(decree, err);
        return err;
    }

    void batch_abort(int64_t decree, int err) { clear_up_batch_states(decree, err); }

private:
    int db_write_batch_put(int64_t decree,
                           dsn::string_view raw_key,
                           dsn::string_view value,
                           uint32_t expire_sec)
    {
        FAIL_POINT_INJECT_F("db_write_batch_put",
                            [](dsn::string_view) -> int { return FAIL_DB_WRITE_BATCH_PUT; });

        rocksdb::Slice skey = utils::to_rocksdb_slice(raw_key);
        rocksdb::SliceParts skey_parts(&skey, 1);
        rocksdb::SliceParts svalue =
            _value_generator.generate_value(_value_schema_version, value, expire_sec);
        rocksdb::Status s = _batch.Put(skey_parts, svalue);
        if (dsn_unlikely(!s.ok())) {
            ::dsn::blob hash_key, sort_key;
            pegasus_restore_key(::dsn::blob(raw_key.data(), 0, raw_key.size()), hash_key, sort_key);
            derror_rocksdb("WriteBatchPut",
                           s.ToString(),
                           "decree: {}, hash_key: {}, sort_key: {}, expire_ts: {}",
                           decree,
                           utils::c_escape_string(hash_key),
                           utils::c_escape_string(sort_key),
                           expire_sec);
        }
        return s.code();
    }

    int db_write_batch_delete(int64_t decree, dsn::string_view raw_key)
    {
        FAIL_POINT_INJECT_F("db_write_batch_delete",
                            [](dsn::string_view) -> int { return FAIL_DB_WRITE_BATCH_DELETE; });

        rocksdb::Status s = _batch.Delete(utils::to_rocksdb_slice(raw_key));
        if (dsn_unlikely(!s.ok())) {
            ::dsn::blob hash_key, sort_key;
            pegasus_restore_key(::dsn::blob(raw_key.data(), 0, raw_key.size()), hash_key, sort_key);
            derror_rocksdb("WriteBatchDelete",
                           s.ToString(),
                           "decree: {}, hash_key: {}, sort_key: {}",
                           decree,
                           utils::c_escape_string(hash_key),
                           utils::c_escape_string(sort_key));
        }
        return s.code();
    }

    // Apply the write batch into rocksdb.
    int db_write(int64_t decree)
    {
        dassert(_batch.Count() != 0, "");

        FAIL_POINT_INJECT_F("db_write", [](dsn::string_view) -> int { return FAIL_DB_WRITE; });

        _wt_opts.given_decree = static_cast<uint64_t>(decree);
        auto status = _db->Write(_wt_opts, &_batch);
        if (!status.ok()) {
            derror_rocksdb("Write", status.ToString(), "decree: {}", decree);
        }
        return status.code();
    }

    void clear_up_batch_states(int64_t decree, int err)
    {
        if (!_update_responses.empty()) {
            dsn::apps::update_response resp;
            resp.error = err;
            resp.app_id = get_gpid().get_app_id();
            resp.partition_index = get_gpid().get_partition_index();
            resp.decree = decree;
            resp.server = _primary_address;
            for (dsn::apps::update_response *uresp : _update_responses) {
                *uresp = resp;
            }
            _update_responses.clear();
        }

        _batch.Clear();
    }

    dsn::blob composite_raw_key(dsn::string_view hash_key, dsn::string_view sort_key)
    {
        dsn::blob raw_key;
        pegasus_generate_key(raw_key, hash_key, sort_key);
        return raw_key;
    }

    // return true if the check type is supported
    bool is_check_type_supported(::dsn::apps::cas_check_type::type check_type)
    {
        return check_type >= ::dsn::apps::cas_check_type::CT_NO_CHECK &&
               check_type <= ::dsn::apps::cas_check_type::CT_VALUE_MATCH_POSTFIX;
    }

    // return true if the data is valid for the check
    bool validate_check(::dsn::apps::cas_check_type::type check_type,
                        const ::dsn::blob &check_oprand,
                        bool value_exist,
                        const ::dsn::blob &value)
    {
        switch (check_type) {
        case ::dsn::apps::cas_check_type::CT_NO_CHECK:
            return true;
        case ::dsn::apps::cas_check_type::CT_VALUE_NOT_EXIST:
            return !value_exist;
        case ::dsn::apps::cas_check_type::CT_VALUE_EXIST:
            return value_exist;
        case ::dsn::apps::cas_check_type::CT_VALUE_NOT_EMPTY:
            return value_exist && value.length() != 0;
        case ::dsn::apps::cas_check_type::CT_VALUE_EQUAL:
            return value_exist && value.length() == check_oprand.length() &&
                   ::memcmp(value.data(), check_oprand.data(), check_oprand.length()) == 0;
        case ::dsn::apps::cas_check_type::CT_VALUE_MATCH_ANYWHERE: {
            if (!value_exist)
                return false;
            if (check_oprand.length() == 0)
                return true;
            if (value.length() < check_oprand.length())
                return false;
            return dsn::string_view(value).find(check_oprand) != dsn::string_view::npos;
        }
        case ::dsn::apps::cas_check_type::CT_VALUE_MATCH_PREFIX: {
            if (!value_exist)
                return false;
            if (check_oprand.length() == 0)
                return true;
            if (value.length() < check_oprand.length())
                return false;
            return ::memcmp(value.data(), check_oprand.data(), check_oprand.length()) == 0;
        }
        case ::dsn::apps::cas_check_type::CT_VALUE_MATCH_POSTFIX: {
            if (!value_exist)
                return false;
            if (check_oprand.length() == 0)
                return true;
            if (value.length() < check_oprand.length())
                return false;
            return ::memcmp(value.data() + value.length() - check_oprand.length(),
                            check_oprand.data(),
                            check_oprand.length()) == 0;
        }
        default:
            dassert(false, "unsupported check type: %d", check_type);
        }
        return false;
    }

private:
    friend class pegasus_write_service_test;
    friend class pegasus_server_write_test;

    const std::string _primary_address;
    const uint32_t _value_schema_version;

    rocksdb::WriteBatch _batch;
    rocksdb::DB *_db;
    rocksdb::WriteOptions &_wt_opts;
    rocksdb::ReadOptions &_rd_opts;
    ::dsn::perf_counter_wrapper &_pfc_recent_expire_count;

    pegasus_value_generator _value_generator;

    // for setting update_response.error after committed.
    std::vector<dsn::apps::update_response *> _update_responses;
};

} // namespace server
} // namespace pegasus
