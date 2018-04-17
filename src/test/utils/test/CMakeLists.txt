set(MY_PROJ_NAME "pegasus_test_utils_test")
project(${MY_PROJ_NAME} C CXX)

set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS
        pegasus_test_utils
        pegasus_client_static
        fmt
        gflags
        gtest pthread
        )

set(MY_BOOST_PACKAGES system filesystem)

set(MY_BINPLACES config.ini)

dsn_add_executable()
