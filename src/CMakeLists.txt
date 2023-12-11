cmake_minimum_required(VERSION 3.19)
project(ts-kv-blob)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")

if (NOT CMAKE_SYSTEM_NAME STREQUAL "Darwin" AND CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address,leak,undefined")
endif()

set(CMAKE_INCLUDE_CURRENT_DIR ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
FetchContent_MakeAvailable(googletest)

add_executable(ts-kv-blob
        level/level.cpp
        main.cpp
        memtable/memtable.cpp
        metric-storage/metric_storage.cpp
        model/aggregations.cpp
        model/column.cpp
        persistent-storage/disk_storage.cpp
        persistent-storage/persistent_storage_manager.cpp
        storage/storage.cpp
)

enable_testing()
add_executable(ts-kv-blob-test
        level/level.cpp
        memtable/memtable.cpp
        metric-storage/metric_storage.cpp
        model/aggregations.cpp
        model/column.cpp
        persistent-storage/disk_storage.cpp
        persistent-storage/persistent_storage_manager.cpp
        storage/storage.cpp
        tests/column_test.cpp
        tests/level_test.cpp
        tests/memtable_test.cpp
)

target_link_libraries(ts-kv-blob-test GTest::gtest_main gmock)

include(GoogleTest)
gtest_discover_tests(ts-kv-blob-test)


