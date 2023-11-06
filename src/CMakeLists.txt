cmake_minimum_required(VERSION 3.19)
project(ts-kv-blob)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address,leak,undefined")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")

set(Protobuf_PROTOC_EXECUTABLE /usr/local/bin/protoc)

set(CMAKE_INCLUDE_CURRENT_DIR ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Protobuf REQUIRED)
add_subdirectory(proto)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${Protobuf_INCLUDE_DIRS})

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

find_package(Protobuf REQUIRED)

TARGET_LINK_LIBRARIES(ts-kv-blob proto ${Protobuf_LIBRARIES})


