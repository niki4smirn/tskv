#include "persistent_storage_manager.h"
#include "model/column.h"

namespace tskv {

PersistentStorageManager::PersistentStorageManager(
    const Options& options,
    std::shared_ptr<IPersistentStorage> persistent_storage) {
  for (const auto& _ : options.levels) {
    levels_.emplace_back(persistent_storage);
  }
}

void PersistentStorageManager::Write(const SerializableColumns& columns) {
  for (const auto& column : columns) {
    levels_[0].Write(column);
  }
  // TODO: write to other levels
}

Column PersistentStorageManager::Read(const TimeRange& time_range,
                                      StoredAggregationType aggregation_type) {
  return levels_[0].Read(time_range, aggregation_type);
}

}  // namespace tskv
