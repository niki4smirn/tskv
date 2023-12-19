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
  struct Options {
    std::vector<Level::Options> levels;
    std::shared_ptr<IPersistentStorage> storage;
  };

 public:
  explicit PersistentStorageManager(const Options& options);
  void Write(const SerializableColumns& columns);

  Column Read(const TimeRange& time_range,
              StoredAggregationType aggregation_type) const;

 private:
  void MergeLevels();

 private:
  std::vector<Level> levels_;
};

}  // namespace tskv
