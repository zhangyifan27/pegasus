set(MY_PROJ_NAME "pegasus_it_test")
project(${MY_PROJ_NAME} C CXX)

set(MY_PROJ_SRC ../../server/pegasus_duplication_backlog_handler.cpp)

set(MY_SRC_SEARCH_MODE "GLOB_RECURSE")

set(MY_PROJ_LIBS
    pegasus_client_static
    pegasus_test_utils
    dsn.replication.ddlclient
    dsn.replication.clientlib
    dsn_layer2_stateful_type1
    dsn.block_service.local
    dsn.block_service.fds
    galaxy-fds-sdk-cpp
    PocoNet
    PocoFoundation
    PocoNetSSL
    PocoJSON
    gtest
    fmt
    gflags
    rt pthread
    )

set(MY_BINPLACES config.ini)

set(MY_BOOST_PACKAGES system filesystem)

dsn_add_executable()
