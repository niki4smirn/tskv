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
  PersistentStorageManager(std::shared_ptr<IPersistentStorage> storage);
  void Write(const Columns& columns);

  Columns Read(const std::vector<TimeRange>& time_ranges,
               AggregationType aggregation_type);

 private:
  std::vector<Level> levels_;
};

}  // namespace tskv
