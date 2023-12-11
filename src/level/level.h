#pragma once

#include "model/column.h"
#include "model/model.h"
#include "persistent-storage/persistent_storage.h"

namespace tskv {

class Level {
 public:
  struct Options {
    Duration bucket_interval;
    Duration level_duration;
    bool store_raw{false};
  };

 public:
  Level(const Options& options, std::shared_ptr<IPersistentStorage> storage);
  Column Read(const TimeRange& time_range,
              StoredAggregationType aggregation_type) const;
  void Write(const SerializableColumn& column);
  void MovePagesFrom(Level& level);
  bool NeedMerge() const;

 private:
  Column ReadRawValues(const TimeRange& time_range) const;

 private:
  Options options_;
  std::shared_ptr<IPersistentStorage> storage_;
  std::vector<std::pair<ColumnType, PageId>> page_ids_;
  TimeRange time_range_{};
};

}  // namespace tskv
