if(BUILD_UNIT_TESTS)
    include(GoogleTest)
    file(GLOB_RECURSE UCMSTORE_TEST_SOURCE_FILES "./case/*.cc")
    if(NOT BUILD_UCM_ASU)
        list(FILTER UCMSTORE_TEST_SOURCE_FILES EXCLUDE REGEX ".*[/\\\\]asu[/\\\\].*")
    endif()
    if(NOT TARGET dramstore)
        list(FILTER UCMSTORE_TEST_SOURCE_FILES EXCLUDE REGEX ".*[/\\\\]dram[/\\\\].*")
    endif()
    if(NOT TARGET yuanrongstore)
        list(FILTER UCMSTORE_TEST_SOURCE_FILES EXCLUDE REGEX ".*/yuanrong/.*\\.cc$")
    endif()
    add_executable(ucmstore.test ${UCMSTORE_TEST_SOURCE_FILES})
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "12" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS "13")
        set_source_files_properties(case/dram/kv_protocol_test.cc
            PROPERTIES COMPILE_OPTIONS "-Wno-nonnull")
    endif()
    target_include_directories(ucmstore.test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/case)
    target_link_libraries(ucmstore.test PRIVATE
        posixstore cachestore fakestore pcstore healthbreakerstore
        gtest_main gtest gmock
    )
    if(TARGET dramstore AND TARGET drampool_core)
        target_link_libraries(ucmstore.test PRIVATE
            dramstore
            drampool_core
        )
    endif()
    if(TARGET yuanrongstore)
        target_compile_definitions(ucmstore.test PRIVATE UCM_YUANRONG_TESTING)
        target_link_libraries(ucmstore.test PRIVATE yuanrongstore)
    endif()
    if(BUILD_UCM_ASU)
        target_link_libraries(ucmstore.test PRIVATE
            kv_client
            buffer_pool
            trans
        )
        target_compile_definitions(ucmstore.test PRIVATE ASU_BUILD_TESTS)
    endif()
    target_link_libraries(ucmstore.test PRIVATE delegator delegator_store)
    gtest_discover_tests(ucmstore.test)

    # Exercise Cache stream scheduling with simulated copies and a fake Device factory.
    # Keep this executable separate so it never links a hardware Device implementation.
    find_package(Threads REQUIRED)
    add_executable(cache_copy_stream.test
        standalone/cache_copy_stream_test.cc
        ${UCM_ROOT_DIR}/ucm/shared/trans/simu/simu_stream.cc
    )
    target_include_directories(cache_copy_stream.test PRIVATE
        ${UCM_ROOT_DIR}/ucm/shared
        ${UCM_ROOT_DIR}/ucm/shared/infra
    )
    target_link_libraries(cache_copy_stream.test PRIVATE storeintf infra_logger Threads::Threads)
    add_test(NAME cache_copy_stream.test COMMAND cache_copy_stream.test)
endif()
