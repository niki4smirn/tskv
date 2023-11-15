#pragma once

#include "model/column.h"
#include "model/model.h"
#include "persistent-storage/persistent_storage.h"

namespace tskv {

class Level {
 public:
 public:
  Level(std::shared_ptr<IPersistentStorage> storage);
  Column Read(const TimeRange& time_range,
              StoredAggregationType aggregation_type);
  void Write(const SerializableColumn& column);
  void MovePagesFrom(Level& level);

 private:
  Column ReadRawValues(const TimeRange& time_range,
                       StoredAggregationType aggregation_type);

 private:
  std::shared_ptr<IPersistentStorage> storage_;
  std::vector<std::pair<ColumnType, PageId>> page_ids_;
};

}  // namespace tskv
