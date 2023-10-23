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
};

struct Record {
  TimePoint timestamp;
  Value value;
};

using InputTimeSeries = std::vector<Record>;
}  // namespace tskv
