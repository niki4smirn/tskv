#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "model/model.h"
#include "model/column.h"

namespace tskv {

struct MetricOptions;

class Memtable {
 public:
  struct Options {
    Duration bucket_inteval;
    size_t capacity;
  };

  struct ReadResult {
    Columns found;
    std::vector<TimeRange> not_found;
  };

 public:
  Memtable(const Options& options, const MetricOptions& metric_options);
  void Write(const InputTimeSeries& time_series);
  ReadResult Read(const std::vector<TimeRange>& time_ranges,
                  StoredAggregationType aggregation_type);
  Columns ExtractColumns();
  bool NeedFlush() const;

 private:
  Columns columns_;
  Options options_;
  size_t size_{0};
};

}  // namespace tskv
