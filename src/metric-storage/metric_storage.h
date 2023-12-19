#pragma once

#include <memory>
#include "memtable/memtable.h"
#include "model/aggregations.h"
#include "model/model.h"
#include "persistent-storage/persistent_storage_manager.h"

namespace tskv {

struct MetricOptions {
  std::vector<StoredAggregationType> aggregation_types;
};

class MetricStorage {
 public:
  struct Options {
    MetricOptions metric_options;
    Memtable::Options memtable_options;

    PersistentStorageManager::Options persistent_storage_manager_options;
  };

 public:
  explicit MetricStorage(const Options& options);
  Column Read(const TimeRange& time_range,
              AggregationType aggregation_type) const;
  void Write(const InputTimeSeries& time_series);
  void Flush();

 private:
  Memtable memtable_;
  PersistentStorageManager persistent_storage_manager_;
};

}  // namespace tskv
