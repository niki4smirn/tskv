#include "persistent_storage_manager.h"
#include "model/column.h"

namespace tskv {

PersistentStorageManager::PersistentStorageManager(
    std::shared_ptr<IPersistentStorage> persistent_storage) {
  // for now just one level
  levels_.emplace_back(persistent_storage);
}

void PersistentStorageManager::Write(const Columns& columns) {
  for (const auto& column : columns) {
    levels_[0].Write(column);
  }
}

Columns PersistentStorageManager::Read(
    const std::vector<TimeRange>& time_ranges,
    StoredAggregationType aggregation_type) {
  Columns result;
  for (const auto& time_range : time_ranges) {
    auto column = levels_[0].Read(time_range, aggregation_type);
    if (column) {
      result.push_back(std::move(column));
    }
  }
  return result;
}

}  // namespace tskv
