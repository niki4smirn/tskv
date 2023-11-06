#pragma once

#include "model/column.h"
#include "model/model.h"
#include "persistent-storage/persistent_storage.h"

namespace tskv {

class Level {
 public:
  Level(std::shared_ptr<IPersistentStorage> storage);
  Columns Read(const std::vector<TimeRange>& time_ranges,
              StoredAggregationType aggregation_type);
  void Write(const Column& column);

 private:
  Columns ReadRawValues(const std::vector<TimeRange>& time_ranges,
                       StoredAggregationType aggregation_type);

 private:
  std::shared_ptr<IPersistentStorage> storage_;
  std::vector<std::pair<ColumnType, PageId>> page_ids_;
};

}  // namespace tskv
