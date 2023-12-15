#include "aggregations.h"

#include <cassert>

#include "model/column.h"

namespace tskv {

StoredAggregationType ToStoredAggregationType(
    AggregationType aggregation_type) {
  assert(aggregation_type != AggregationType::kAvg);
  return static_cast<StoredAggregationType>(aggregation_type);
}

ColumnType ToColumnType(AggregationType aggregation_type) {
  return ToColumnType(ToStoredAggregationType(aggregation_type));
}

ColumnType ToColumnType(StoredAggregationType aggregation_type) {
  switch (aggregation_type) {
    case StoredAggregationType::kSum:
      return ColumnType::kSum;
    case StoredAggregationType::kCount:
      return ColumnType::kCount;
    case StoredAggregationType::kMin:
      return ColumnType::kMin;
    case StoredAggregationType::kMax:
      return ColumnType::kMax;
    case StoredAggregationType::kLast:
      return ColumnType::kLast;
    case StoredAggregationType::kNone:
      return ColumnType::kRawRead;
  }
}

}  // namespace tskv
