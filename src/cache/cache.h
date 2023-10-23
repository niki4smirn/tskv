#pragma once

#include <cstddef>
#include <vector>

#include "model/column.h"
#include "model/model.h"

namespace tskv {

class Cache {
 public:
  struct Options {
    size_t capacity;
  };

  struct ReadResult {
    Column found;
    std::vector<TimeRange> not_found;
  };

 public:
  Cache(const Options& options);

  ReadResult Read(const TimeRange& time_range,
                  AggregationType aggregation_type);
  void Write(const TimeRange& time_range, const InputTimeSeries& records,
             AggregationType aggregation_type);

 private:
  size_t capacity_;
};

}  // namespace tskv
