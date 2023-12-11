#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "model/column.h"
#include "model/model.h"

namespace tskv {

struct MetricOptions;

class Memtable {
 public:
  struct Options {
    Duration bucket_interval;
    std::optional<size_t> max_bytes_size;
    std::optional<Duration> max_age;
    bool store_raw{false};
  };

  struct ReadResult {
    Column found;
    // only <= 1 time range, because memtable stores data suffix
    std::optional<TimeRange> not_found;
  };

 public:
  Memtable(const Options& options, const MetricOptions& metric_options);
  void Write(const InputTimeSeries& time_series);
  ReadResult Read(const TimeRange& time_range,
                  StoredAggregationType aggregation_type) const;
  Columns ExtractColumns();
  bool NeedFlush() const;

 private:
  ReadResult ReadRawValues(const TimeRange& time_range) const;

  size_t GetBytesSize() const;

  Columns columns_;
  Options options_;
};

}  // namespace tskv
