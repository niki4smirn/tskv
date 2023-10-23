#pragma once

#include <memory>
#include "cache/cache.h"
#include "memtable/memtable.h"
#include "model/model.h"
#include "persistent-storage/persistent_storage_manager.h"

namespace tskv {

struct MetricOptions {
  std::vector<ColumnType> column_types;
};

class MetricStorage {
 public:
  struct Options {
    MetricOptions metric_options;
    Cache::Options cache_options;
    Memtable::Options memtable_options;

    std::shared_ptr<IPersistentStorage> persistent_storage;
  };

 public:
  MetricStorage(const Options& options);
  Column Read(const TimeRange& time_range, AggregationType aggregation_type);
  void Write(const InputTimeSeries& time_series);

 private:
  Cache cache_;
  Memtable memtable_;
  PersistentStorageManager persistent_storage_manager_;
  // WAL wal_;
};

}  // namespace tskv
