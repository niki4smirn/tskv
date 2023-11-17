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
  struct LevelOptions {
    Duration bucket_interval;
    Duration level_duration;
  };

  struct Options {
    std::vector<LevelOptions> levels;
  };

 public:
  PersistentStorageManager(const Options& options,
                           std::shared_ptr<IPersistentStorage> storage);
  void Write(const SerializableColumns& columns);

  Column Read(const TimeRange& time_range,
              StoredAggregationType aggregation_type) const;

 private:
  void MergeLevels();

 private:
  std::vector<Level> levels_;
  std::vector<LevelOptions> level_options_;
  std::vector<TimePoint> last_level_merges_;
};

}  // namespace tskv
