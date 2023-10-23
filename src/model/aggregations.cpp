#include "aggregations.h"

#include <cassert>

namespace tskv {

StoredAggregationType ToStoredAggregationType(
    AggregationType aggregation_type) {
  assert(aggregation_type != AggregationType::kAvg);
  return static_cast<StoredAggregationType>(aggregation_type);
}

}  // namespace tskv
