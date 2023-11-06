#include "persistent_storage_manager.h"
#include "model/column.h"

namespace tskv {

PersistentStorageManager::PersistentStorageManager(
    std::shared_ptr<IPersistentStorage> persistent_storage) {
  // for now just one level
  levels_.emplace_back(std::move(persistent_storage));
}

void PersistentStorageManager::Write(const Columns& columns) {
  for (const auto& column : columns) {
    levels_[0].Write(column);
  }
}

Column PersistentStorageManager::Read(
    const TimeRange& time_range,
    StoredAggregationType aggregation_type) {
  return levels_[0].Read(time_range, aggregation_type);
}

}  // namespace tskv
