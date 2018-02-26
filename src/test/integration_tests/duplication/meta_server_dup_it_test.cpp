// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <pegasus/client.h>
#include <dsn/cpp/smart_pointers.h>
#include <dsn/dist/replication/duplication_common.h>

#include "test/integration_tests/integration_test_base.h"

namespace pegasus {
namespace test {

using namespace dsn::replication;

class meta_server_dup_test : public integration_test_base
{
public:

    void add_dup(const std::string &app_name, const std::string &remote_cluster)
    {
        ASSERT_EQ(_ddl_client->add_dup(app_name, remote_cluster), dsn::ERR_OK);
    }

    void query_dup(const std::string &app_name, duplication_query_response *resp)
    {
        dsn::error_code err;
        do {
            err = _ddl_client->query_dup(app_name, resp);
        } while(err == dsn::ERR_FORWARD_TO_OTHERS);
        ASSERT_EQ(err, dsn::ERR_OK);
    }

    void
    change_dup_stat(const std::string &app_name, dupid_t dupid, duplication_status::type status)
    {
        ASSERT_EQ(_ddl_client->change_dup_status(app_name, dupid, status), dsn::ERR_OK);
    }
};

TEST_F(meta_server_dup_test, add_dup)
{
    std::string test_app = "test_app";

    create_app(test_app);
    add_dup(test_app, "dsn://duplication_cluster");

    duplication_query_response resp;
    query_dup(test_app, &resp);
    ASSERT_EQ(resp.entry_list.size(), 1);
    ASSERT_EQ(resp.entry_list.back().status, duplication_status::DS_START);
    dupid_t dupid = resp.entry_list.back().dupid;

    change_dup_stat(test_app, dupid, duplication_status::DS_PAUSE);
    query_dup(test_app, &resp);
    ASSERT_EQ(resp.entry_list.back().status, duplication_status::DS_PAUSE);

    change_dup_stat(test_app, dupid, duplication_status::DS_REMOVED);
    query_dup(test_app, &resp);
    ASSERT_EQ(resp.entry_list.size(), 0);

    add_dup(test_app, "dsn://duplication_cluster");
    query_dup(test_app, &resp);
    ASSERT_EQ(resp.entry_list.size(), 1);
    dupid_t dupid_2 = resp.entry_list.back().dupid;
    ASSERT_NE(dupid, dupid_2);
}

TEST_F(meta_server_dup_test, recover)
{
    std::string test_app = "test_app";

    create_app(test_app);
    add_dup(test_app, "dsn://duplication_cluster");

    duplication_query_response resp;
    query_dup(test_app, &resp);
    dupid_t dupid = resp.entry_list.back().dupid;
    change_dup_stat(test_app, dupid, duplication_status::DS_PAUSE);

    _onebox->stop_onebox();
    _onebox->start_onebox(3, 3);

    query_dup(test_app, &resp);
    ASSERT_EQ(resp.entry_list.size(), 1);
    ASSERT_EQ(resp.entry_list.back().status, duplication_status::DS_PAUSE);
}

} // namespace test
} // namespace pegasus
