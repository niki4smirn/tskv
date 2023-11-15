#include "persistent_storage_manager.h"

#include "model/column.h"

namespace tskv {

PersistentStorageManager::PersistentStorageManager(
    const Options& options,
    std::shared_ptr<IPersistentStorage> persistent_storage) {
  for (size_t i = 0; i < options.levels.size(); ++i) {
    levels_.emplace_back(persistent_storage);
  }
  level_options_ = options.levels;
  // temporary for testing
  auto now = 0;
  last_level_merges_.assign(options.levels.size(), now);
}

void PersistentStorageManager::Write(const SerializableColumns& columns) {
  for (const auto& column : columns) {
    levels_[0].Write(column);
  }

  // we should do it in parallel
  MergeLevels();
}

Column PersistentStorageManager::Read(const TimeRange& time_range,
                                      StoredAggregationType aggregation_type) {
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
  // temporary for testing
  auto now = 3;
  for (size_t i = 0; i < levels_.size() - 1; ++i) {
    if (level_options_[i].level_duration + last_level_merges_[i] < now) {
      // TODO: handle bucket intervals
      levels_[i + 1].MovePagesFrom(levels_[i]);
      last_level_merges_[i] = now;
    }
  }
}

}  // namespace tskv
