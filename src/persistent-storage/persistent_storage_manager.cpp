#include "persistent_storage_manager.h"

namespace tskv {

PersistentStorageManager::PersistentStorageManager(
    std::shared_ptr<IPersistentStorage>) {}

void PersistentStorageManager::Write(const Columns& columns) {}

Columns PersistentStorageManager::Read(
    const std::vector<TimeRange>& time_ranges,
    AggregationType aggregation_type) {
  return {};
}

}  // namespace tskv
