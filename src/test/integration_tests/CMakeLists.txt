set(MY_PROJ_NAME "pegasus_it_test")
project(${MY_PROJ_NAME} C CXX)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC duplication/meta_server_dup_it_test.cpp
                duplication/replica_server_dup_it_test.cpp
                duplication/pega_dup_backlog_handler_it_test.cpp
                ../../server/pegasus_duplication_backlog_handler.cpp
                )

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH
        "../../include"
        "../.."
        "../../../rdsn/thirdparty/output/include"
        )

set(MY_PROJ_LIBS
    pegasus_client_static
    pegasus_test_utils
    dsn.replication.ddlclient
    dsn.replication.clientlib
    gtest
    fmt
    gflags
    )

set(MY_BINPLACES config.ini)

set(MY_BOOST_PACKAGES system filesystem)

if (UNIX)
    set(CMAKE_INSTALL_RPATH ".")
    set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
    set(MY_PROJ_LIBS ${MY_PROJ_LIBS} rt pthread)
endif()

dsn_add_executable()
