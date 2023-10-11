#include "model/model.h"

namespace tskv {

class MetricStorage {
 public:
  TimeSeries Read(const TimeRange& time_range, AggregationType aggregation_type);
  void Write(const TimeSeries& time_series);
};

}  // namespace tskv
