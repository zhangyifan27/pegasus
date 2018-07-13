// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <cstdlib>
#include <string>
#include <vector>
#include <climits>
#include <map>

#include <dsn/service_api_c.h>
#include <unistd.h>
#include <pegasus/client.h>
#include <gtest/gtest.h>

using namespace ::pegasus;

extern pegasus_client *client;

TEST(check_and_set, value_not_exist)
{
    std::string hash_key("check_and_set_test_value_not_exist");

    {
        int ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::check_and_set_options options;
        pegasus_client::check_and_set_results results;

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_NOT_EXIST,
                                    "",
                                    "k1",
                                    "v1",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v1", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_NOT_EXIST,
                                    "",
                                    "k1",
                                    "v2",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v1", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v1", value);

        options.return_check_value = false;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_NOT_EXIST,
                                    "",
                                    "k1",
                                    "v1",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_FALSE(results.check_value_returned);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v1", value);

        ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);
    }

    {
        int ret = client->del(hash_key, "k2");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::check_and_set_options options;
        pegasus_client::check_and_set_results results;

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k2",
                                    pegasus_client::cas_check_type::CT_VALUE_NOT_EXIST,
                                    "",
                                    "k2",
                                    "",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);
        ret = client->get(hash_key, "k2", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k2",
                                    pegasus_client::cas_check_type::CT_VALUE_NOT_EXIST,
                                    "",
                                    "k2",
                                    "v2",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("", results.check_value);
        ret = client->get(hash_key, "k2", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("", value);

        ret = client->del(hash_key, "k2");
        ASSERT_EQ(0, ret);
    }

    {
        int ret = client->del(hash_key, "k3");
        ASSERT_EQ(0, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::check_and_set_options options;
        pegasus_client::check_and_set_results results;

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k3",
                                    pegasus_client::cas_check_type::CT_VALUE_NOT_EXIST,
                                    "",
                                    "k4",
                                    "v4",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);
        ret = client->get(hash_key, "k4", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v4", value);

        ret = client->del(hash_key, "k3");
        ASSERT_EQ(0, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);
    }
}

TEST(check_and_set, value_exist)
{
    std::string hash_key("check_and_set_test_value_exist");

    {
        int ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::check_and_set_options options;
        pegasus_client::check_and_set_results results;

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_EXIST,
                                    "",
                                    "k1",
                                    "v1",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_NOT_FOUND, ret);

        ret = client->set(hash_key, "k1", "");
        ASSERT_EQ(PERR_OK, ret);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_EXIST,
                                    "",
                                    "k1",
                                    "v1",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v1", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_EXIST,
                                    "",
                                    "k1",
                                    "v2",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v1", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);
    }

    {
        int ret = client->set(hash_key, "k3", "v3");
        ASSERT_EQ(PERR_OK, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::check_and_set_options options;
        pegasus_client::check_and_set_results results;

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k3",
                                    pegasus_client::cas_check_type::CT_VALUE_EXIST,
                                    "",
                                    "k4",
                                    "v4",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v3", results.check_value);
        ret = client->get(hash_key, "k4", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v4", value);

        ret = client->del(hash_key, "k3");
        ASSERT_EQ(0, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);
    }
}

TEST(check_and_set, value_not_empty)
{
    std::string hash_key("check_and_set_test_value_not_empty");

    {
        int ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::check_and_set_options options;
        pegasus_client::check_and_set_results results;

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_NOT_EMPTY,
                                    "",
                                    "k1",
                                    "v1",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_NOT_FOUND, ret);

        ret = client->set(hash_key, "k1", "");
        ASSERT_EQ(PERR_OK, ret);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_NOT_EMPTY,
                                    "",
                                    "k1",
                                    "v1",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("", value);

        ret = client->set(hash_key, "k1", "v1");
        ASSERT_EQ(PERR_OK, ret);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_NOT_EMPTY,
                                    "",
                                    "k1",
                                    "v2",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v1", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);
    }

    {
        int ret = client->set(hash_key, "k3", "v3");
        ASSERT_EQ(PERR_OK, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::check_and_set_options options;
        pegasus_client::check_and_set_results results;

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k3",
                                    pegasus_client::cas_check_type::CT_VALUE_NOT_EMPTY,
                                    "",
                                    "k4",
                                    "v4",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v3", results.check_value);
        ret = client->get(hash_key, "k4", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v4", value);

        ret = client->del(hash_key, "k3");
        ASSERT_EQ(0, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);
    }
}

TEST(check_and_set, value_equal)
{
    std::string hash_key("check_and_set_test_value_equal");

    {
        int ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::check_and_set_options options;
        pegasus_client::check_and_set_results results;

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_EQUAL,
                                    "",
                                    "k1",
                                    "v1",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_NOT_FOUND, ret);

        ret = client->set(hash_key, "k1", "");
        ASSERT_EQ(PERR_OK, ret);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_EQUAL,
                                    "",
                                    "k1",
                                    "v1",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v1", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_EQUAL,
                                    "",
                                    "k1",
                                    "v2",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v1", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v1", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_EQUAL,
                                    "v1",
                                    "k1",
                                    "v2",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v1", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);
    }

    {
        int ret = client->set(hash_key, "k3", "v3");
        ASSERT_EQ(PERR_OK, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::check_and_set_options options;
        pegasus_client::check_and_set_results results;

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k3",
                                    pegasus_client::cas_check_type::CT_VALUE_EQUAL,
                                    "v3",
                                    "k4",
                                    "v4",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v3", results.check_value);
        ret = client->get(hash_key, "k4", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v4", value);

        ret = client->del(hash_key, "k3");
        ASSERT_EQ(0, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);
    }
}

TEST(check_and_set, value_match_anywhere)
{
    std::string hash_key("check_and_set_test_value_match_anywhere");

    {
        int ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::check_and_set_options options;
        pegasus_client::check_and_set_results results;

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_ANYWHERE,
                                    "v",
                                    "k1",
                                    "v1",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_NOT_FOUND, ret);

        ret = client->set(hash_key, "k1", "");
        ASSERT_EQ(PERR_OK, ret);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_ANYWHERE,
                                    "v",
                                    "k1",
                                    "v1",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_ANYWHERE,
                                    "",
                                    "k1",
                                    "v1",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v1", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_ANYWHERE,
                                    "",
                                    "k1",
                                    "v2",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v1", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_ANYWHERE,
                                    "2",
                                    "k1",
                                    "v111v",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v111v", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_ANYWHERE,
                                    "111",
                                    "k1",
                                    "v2",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v111v", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_ANYWHERE,
                                    "y",
                                    "k1",
                                    "v3",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_ANYWHERE,
                                    "v2v",
                                    "k1",
                                    "v3",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_ANYWHERE,
                                    "v2",
                                    "k1",
                                    "v3",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v3", value);

        ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);
    }

    {
        int ret = client->set(hash_key, "k3", "v333v");
        ASSERT_EQ(PERR_OK, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::check_and_set_options options;
        pegasus_client::check_and_set_results results;

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k3",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_ANYWHERE,
                                    "333",
                                    "k4",
                                    "v4",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v333v", results.check_value);
        ret = client->get(hash_key, "k4", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v4", value);

        ret = client->del(hash_key, "k3");
        ASSERT_EQ(0, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);
    }
}

TEST(check_and_set, value_match_prefix)
{
    std::string hash_key("check_and_set_test_value_match_prefix");

    {
        int ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::check_and_set_options options;
        pegasus_client::check_and_set_results results;

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                    "v",
                                    "k1",
                                    "v1",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_NOT_FOUND, ret);

        ret = client->set(hash_key, "k1", "");
        ASSERT_EQ(PERR_OK, ret);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                    "v",
                                    "k1",
                                    "v1",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                    "",
                                    "k1",
                                    "v1",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v1", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                    "",
                                    "k1",
                                    "v2",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v1", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                    "v",
                                    "k1",
                                    "v111v",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v111v", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                    "111",
                                    "k1",
                                    "v2",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v111v", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v111v", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                    "v111",
                                    "k1",
                                    "v2",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v111v", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                    "y",
                                    "k1",
                                    "v3",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                    "v2v",
                                    "k1",
                                    "v3",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                    "2",
                                    "k1",
                                    "v3",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                    "v2",
                                    "k1",
                                    "v3",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v3", value);

        ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);
    }

    {
        int ret = client->set(hash_key, "k3", "v333v");
        ASSERT_EQ(PERR_OK, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::check_and_set_options options;
        pegasus_client::check_and_set_results results;

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k3",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_PREFIX,
                                    "v333",
                                    "k4",
                                    "v4",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v333v", results.check_value);
        ret = client->get(hash_key, "k4", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v4", value);

        ret = client->del(hash_key, "k3");
        ASSERT_EQ(0, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);
    }
}

TEST(check_and_set, value_match_postfix)
{
    std::string hash_key("check_and_set_test_value_match_postfix");

    {
        int ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::check_and_set_options options;
        pegasus_client::check_and_set_results results;

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                    "v",
                                    "k1",
                                    "v1",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_FALSE(results.check_value_exist);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_NOT_FOUND, ret);

        ret = client->set(hash_key, "k1", "");
        ASSERT_EQ(PERR_OK, ret);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                    "v",
                                    "k1",
                                    "v1",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                    "",
                                    "k1",
                                    "v1",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v1", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                    "",
                                    "k1",
                                    "v2",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v1", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                    "2",
                                    "k1",
                                    "v111v",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v111v", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                    "111",
                                    "k1",
                                    "v2",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v111v", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v111v", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                    "111v",
                                    "k1",
                                    "v2",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v111v", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                    "y",
                                    "k1",
                                    "v3",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                    "2v2",
                                    "k1",
                                    "v3",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                    "v",
                                    "k1",
                                    "v3",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v2", value);

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k1",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                    "v2",
                                    "k1",
                                    "v3",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v2", results.check_value);
        ret = client->get(hash_key, "k1", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v3", value);

        ret = client->del(hash_key, "k1");
        ASSERT_EQ(0, ret);
    }

    {
        int ret = client->set(hash_key, "k3", "v333v");
        ASSERT_EQ(PERR_OK, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);

        std::string value;
        pegasus_client::check_and_set_options options;
        pegasus_client::check_and_set_results results;

        options.return_check_value = true;
        ret = client->check_and_set(hash_key,
                                    "k3",
                                    pegasus_client::cas_check_type::CT_VALUE_MATCH_POSTFIX,
                                    "333v",
                                    "k4",
                                    "v4",
                                    options,
                                    results);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_TRUE(results.set_succeed);
        ASSERT_TRUE(results.check_value_returned);
        ASSERT_TRUE(results.check_value_exist);
        ASSERT_EQ("v333v", results.check_value);
        ret = client->get(hash_key, "k4", value);
        ASSERT_EQ(PERR_OK, ret);
        ASSERT_EQ("v4", value);

        ret = client->del(hash_key, "k3");
        ASSERT_EQ(0, ret);
        ret = client->del(hash_key, "k4");
        ASSERT_EQ(0, ret);
    }
}

TEST(check_and_set, invalid_type)
{
    std::string hash_key("check_and_set_test_value_invalid_type");

    {
        int ret = 0;
        pegasus_client::check_and_set_options options;
        pegasus_client::check_and_set_results results;

        options.return_check_value = true;
        ret = client->check_and_set(
            hash_key, "k1", (pegasus_client::cas_check_type)100, "v", "k1", "v1", options, results);
        ASSERT_EQ(PERR_INVALID_ARGUMENT, ret);
        ASSERT_FALSE(results.set_succeed);
        ASSERT_FALSE(results.check_value_returned);
    }
}
