#pragma once

#include "model/model.h"
#include "persistent-storage/persistent_storage.h"

namespace tskv {

class Level {
 public:
  Level(std::shared_ptr<IPersistentStorage> storage);
  // TimeSeries Read(const TimeRange& time_range,
  //                 AggregationType aggregation_type);
  // void Write(const Column& column);
};

}  // namespace tskv
