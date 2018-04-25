set(MY_PROJ_NAME pegasus_unit_test)

#Source files under CURRENT project directory will be automatically included.
#You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "../pegasus_server_impl.cpp"
                "../pegasus_write_service.cpp"
                "../pegasus_perf_counter.cpp"
                "../pegasus_counter_updater.cpp"
                "../pegasus_duplication_backlog_handler.cpp")

#Search mode for source files under CURRENT project directory ?
#"GLOB_RECURSE" for recursive search
#"GLOB" for non - recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIB_PATH "../../../rocksdb" ${LIBFDS_LIB_DIR} ${POCO_LIB_DIR})

set(MY_PROJ_LIBS
        dsn_layer2_stateful_type1
        dsn_meta_server
        dsn.replication.clientlib
        dsn.replication.ddlclient
        dsn.block_service.local
        dsn.block_service.fds
        dsn.failure_detector
        dsn.failure_detector.multimaster
        dsn.replication.zookeeper_provider
        pegasus_client_static
        zookeeper_mt
        event
        galaxy-fds-sdk-cpp
        PocoNet
        PocoFoundation
        PocoNetSSL
        PocoJSON
        crypto
        fmt
        rocksdb
        pegasus.base
        gtest
        z bz2 snappy rt aio pthread
        )

set(MY_BOOST_PACKAGES system filesystem)

#Extra files that will be installed
set(MY_BINPLACES config.ini)

dsn_add_executable()
