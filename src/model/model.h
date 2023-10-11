#include <chrono>
#include <cstdint>
#include <vector>

namespace tskv {

using Duration = std::chrono::duration<int>;
using TimePoint = std::chrono::time_point<std::chrono::system_clock>;
using Value = double;

struct Record {
  TimePoint timestamp;
  Value value;
};

using TimeSeries = std::vector<Record>;

struct TimeRange {
  TimePoint start;
  TimePoint end;
};

enum class StoredAggregationType {
  kSum,
  kCount,
  kMin,
  kMax,

  // WARNING: kAggregationTypeCount must be the last element.
  kStoredAggregationTypeCount,
};

constexpr auto kStoredAggregationsNum =
    static_cast<size_t>(StoredAggregationType::kStoredAggregationTypeCount);

using CompressedBytes = std::vector<uint8_t>;

// unfortunately, there is no good way to eliminate duplication
enum class ColumnType {
  kSum,
  kCount,
  kMin,
  kMax,
  kRawTimestamps,
  kRawValues,
};

enum class AggregationType {
  kSum,
  kCount,
  kMin,
  kMax,
  kAvg,
};

struct Column {
  CompressedBytes data;
  TimeRange time_range;
  ColumnType type;
};

}  // namespace tskv
