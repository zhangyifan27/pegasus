// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/utility/string_view.h>
#include <dsn/cpp/pipeline.h>
#include <pegasus/client.h>
#include <gflags/gflags.h>
#include <dsn/stats/histogram.h>

namespace pegasus {
namespace test {

struct verify_data;
struct insert_data;
struct recover_break_point;

// data_verifier is used to verify if pegasus is in good state during monkey test or
// complicated situations like upgrading.
struct data_verifier : dsn::pipeline::base
{
    data_verifier(pegasus_client *client, dsn::string_view data_prefix);

    // For inter-cluster duplication test.
    data_verifier(pegasus_client *insert_client,
                  pegasus_client *verify_client,
                  dsn::string_view data_prefix);

    ~data_verifier();

    struct progress
    {
        explicit progress(dsn::string_view data_prefix)
            : _hash_key_prefix(std::string(data_prefix) + "_hash_key_"),
              _sort_key_prefix(std::string(data_prefix) + "_sort_key_"),
              _value_prefix(std::string(data_prefix) + "_value_")
        {
        }

        // The progress is advanced only when the verification of
        // current key value passed.
        void next()
        {
            id++;
            auto id_str = std::to_string(id);

            hash_key = _hash_key_prefix + id_str;
            sort_key = _sort_key_prefix + id_str;
            value = _value_prefix + id_str;
        }

        // mark break point, so that when data verifier terminates due to pegasus failure,
        // it can restart from the break point.
        void mark_break_point_mode()
        {
            hash_key = _hash_key_prefix + "break_point";
            sort_key = "";
            value = std::to_string(id);
        }
        void query_break_point_mode()
        {
            hash_key = _hash_key_prefix + "break_point";
            sort_key = "";
        }

        uint64_t id{0};
        std::string hash_key;
        std::string sort_key;
        std::string value;

    private:
        const std::string _hash_key_prefix;
        const std::string _sort_key_prefix;
        const std::string _value_prefix;
    };

    // Periodically print histogram metrics in the background.
    struct histogram_printer_t : dsn::pipeline::base
    {
        histogram_printer_t();

        ~histogram_printer_t();

    private:
        dsn::pipeline::do_when<> _print;
        dsn::clientlet _tracker;
    };

    static dsn::stats::histogram set_histogram;
    static dsn::stats::histogram get_histogram;
    static histogram_printer_t histogram_printer;

private:
    // === pipeline === //
    std::unique_ptr<verify_data> _verify;
    std::unique_ptr<insert_data> _insert;
    std::unique_ptr<recover_break_point> _recover;
    progress _progress;

    dsn::clientlet _tracker;
};

} // namespace test
} // namespace pegasus
