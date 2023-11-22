#include "persistent_storage_manager.h"

#include "model/column.h"

namespace tskv {

PersistentStorageManager::PersistentStorageManager(const Options& options) {
  for (size_t i = 0; i < options.levels.size(); ++i) {
    levels_.emplace_back(options.levels[i], options.storage);
  }
}

void PersistentStorageManager::Write(const SerializableColumns& columns) {
  for (const auto& column : columns) {
    levels_.front().Write(column);
  }

  MergeLevels();
}

Column PersistentStorageManager::Read(
    const TimeRange& time_range, StoredAggregationType aggregation_type) const {
  // TODO: not read all levels, check time_range and read only needed levels
  Column result;
  for (int i = levels_.size() - 1; i >= 0; --i) {
    auto column = levels_[i].Read(time_range, aggregation_type);
    if (result) {
      result->Merge(column);
    } else {
      result = column;
    }
  }
  return result;
}

void PersistentStorageManager::MergeLevels() {
  for (size_t i = 0; i < levels_.size() - 1; ++i) {
    if (levels_[i].NeedMerge()) {
      levels_[i + 1].MovePagesFrom(levels_[i]);
    }
  }
}

}  // namespace tskv
