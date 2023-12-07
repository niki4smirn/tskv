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
  Duration() : value_(0) {}

  Duration(uint64_t value) : value_(value) {}

  static Duration Milliseconds(int milliseconds) {
    return Duration(milliseconds * 1000);
  }

  static Duration Seconds(int seconds) {
    return Duration(seconds * 1000 * 1000);
  }

  static Duration Minutes(int minutes) {
    return Duration(minutes * 60 * 1000 * 1000);
  }

  static Duration Hours(int hours) {
    return Duration(hours * 60ull * 60 * 1000 * 1000);
  }

  static Duration Days(int days) {
    return Duration(days * 24ull * 60 * 60 * 1000 * 1000);
  }

  static Duration Weeks(int weeks) {
    return Duration(weeks * 7ull * 24 * 60 * 60 * 1000 * 1000);
  }

  static Duration Months(int months) {
    return Duration(months * 30ull * 24 * 60 * 60 * 1000 * 1000);
  }

  operator uint64_t() const { return value_; }

 private:
  uint64_t value_;
};

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
