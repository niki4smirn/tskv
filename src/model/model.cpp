#include "model.h"

tskv::Duration::Duration() : value_(0) {}

tskv::Duration::Duration(uint64_t value) : value_(value) {}

tskv::Duration tskv::Duration::Milliseconds(int milliseconds) {
  return Duration(milliseconds * 1000);
}

tskv::Duration tskv::Duration::Seconds(int seconds) {
  return Duration(seconds * 1000 * 1000);
}

tskv::Duration tskv::Duration::Minutes(int minutes) {
  return Duration(minutes * 60 * 1000 * 1000);
}

tskv::Duration tskv::Duration::Hours(int hours) {
  return Duration(hours * 60ull * 60 * 1000 * 1000);
}

tskv::Duration tskv::Duration::Days(int days) {
  return Duration(days * 24ull * 60 * 60 * 1000 * 1000);
}

tskv::Duration tskv::Duration::Weeks(int weeks) {
  return Duration(weeks * 7ull * 24 * 60 * 60 * 1000 * 1000);
}

tskv::Duration tskv::Duration::Months(int months) {
  return Duration(months * 30ull * 24 * 60 * 60 * 1000 * 1000);
}

tskv::Duration::operator uint64_t() const {
  return value_;
}

tskv::Duration tskv::TimeRange::GetDuration() const {
  return end - start;
}

tskv::TimeRange tskv::TimeRange::Merge(const TimeRange& other) const {
  if (start == 0 && end == 0) {
    return other;
  }
  return {std::min(start, other.start), std::max(end, other.end)};
}
