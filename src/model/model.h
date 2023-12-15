#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

#include "aggregations.h"

namespace tskv {

using TimePoint = uint64_t;
using Value = double;

class Duration {
 public:
  Duration();
  Duration(uint64_t value);

  static Duration Milliseconds(int milliseconds);
  static Duration Seconds(int seconds);
  static Duration Minutes(int minutes);
  static Duration Hours(int hours);
  static Duration Days(int days);
  static Duration Weeks(int weeks);
  static Duration Months(int months);

  operator uint64_t() const;

 private:
  uint64_t value_;
};

// [start, end)
struct TimeRange {

  TimePoint start;
  TimePoint end;

  bool operator==(const TimeRange& other) const = default;

  Duration GetDuration() const;

  TimeRange Merge(const TimeRange& other) const;
};

struct Record {
  TimePoint timestamp;
  Value value;
};

using InputTimeSeries = std::vector<Record>;

}  // namespace tskv
