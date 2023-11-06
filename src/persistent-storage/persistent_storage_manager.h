#pragma once

#include "level/level.h"
#include "model/column.h"
#include "model/model.h"
#include "persistent_storage.h"

#include <memory>
#include <vector>

namespace tskv {

class PersistentStorageManager {
 public:
  explicit PersistentStorageManager(std::shared_ptr<IPersistentStorage> storage);
  void Write(const Columns& columns);

  Column Read(const TimeRange& time_range,
              StoredAggregationType aggregation_type);

 private:
  std::vector<Level> levels_;
};

}  // namespace tskv
