INCLUDE(FindProtobuf)
FIND_PACKAGE(Protobuf REQUIRED)
INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIR})
PROTOBUF_GENERATE_CPP(PROTO_SRC PROTO_HEADER
        ydb_operation.proto
        ydb_keyvalue.proto
        validation.proto
        ydb_common.proto
        ydb_issue_message.proto
        ydb_status_codes.proto
        validation.proto
)
ADD_LIBRARY(proto ${PROTO_HEADER} ${PROTO_SRC})