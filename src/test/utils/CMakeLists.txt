set(MY_PROJ_NAME "pegasus_test_utils")

set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS
        pegasus_client_static
        fmt
        gflags
        )

set(MY_BOOST_PACKAGES system filesystem)

set(MY_PROJ_LIB_PATH "../..")

dsn_add_static_library()
