#pragma once

#include "proto/ydb_keyvalue.pb.h"

namespace tskv {

using Request = Ydb::KeyValue::ExecuteTransactionRequest;
using Response = Ydb::KeyValue::ExecuteTransactionResponse;

class Storage {
 public:
  Response Execute(const Request& request);
};

}  // namespace tskv
