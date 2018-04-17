set(MY_PROJ_NAME "pegasus_inter_cluster_duplication_verifier")
project(${MY_PROJ_NAME} C CXX)

set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS
        pegasus_client_static
        dsn.replication.ddlclient
        dsn.replication.clientlib
        pegasus_test_utils
        fmt
        gflags
        rt pthread
        )

set(MY_BINPLACES config.ini)

set(MY_BOOST_PACKAGES system filesystem)

dsn_add_executable()
