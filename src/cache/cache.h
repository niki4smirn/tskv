#include <cstddef>
#include <vector>

#include "model/model.h"

namespace tskv {

struct CacheReadResult {
  std::vector<TimeSeries> found;
  std::vector<TimeRange> not_found;
};

class Cache {
 public:
  Cache(size_t capacity);

  CacheReadResult Read(const TimeRange& time_range,
                       AggregationType aggregation_type);
  void Write(const TimeRange& time_range, const TimeSeries& records,
             AggregationType aggregation_type);
};

}  // namespace tskv
