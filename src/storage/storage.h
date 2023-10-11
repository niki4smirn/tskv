#include "model/model.h"

namespace tskv {

using MetricId = uint64_t;

class Storage {
 public:
  TimeSeries Read(MetricId metric_id, const TimeRange& time_range,
                  AggregationType aggregation_type);

  // should somehow return error (for example, when there is no free space in
  // persistent storage)
  //
  // Still not sure about error handling pattern. Probably std::expected is the
  // best choice for the project (I hope it's not a problem, that it was intoduced
  // only in C++23)
  void Write(MetricId metric_id, const TimeSeries& time_series);
};

}  // namespace tskv
