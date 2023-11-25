#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

#include "aggregations.h"

namespace tskv {

using Duration = uint64_t;
using TimePoint = uint64_t;
using Value = double;

// [start, end)
struct TimeRange {
  TimePoint start;
  TimePoint end;

  bool operator==(const TimeRange& other) const = default;

  Duration GetDuration() const { return end - start; }
  TimeRange Merge(const TimeRange& other) const {
    if (start == 0 && end == 0) {
      return other;
    }
    return {std::min(start, other.start), std::max(end, other.end)};
  }
};

struct Record {
  TimePoint timestamp;
  Value value;
};

using InputTimeSeries = std::vector<Record>;
}  // namespace tskv
