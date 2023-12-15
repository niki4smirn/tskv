#pragma once

#include <cstddef>

namespace tskv {

enum class ColumnType;

enum class StoredAggregationType {
  kNone,
  kSum,
  kCount,
  kMin,
  kMax,
  kLast,
};

// WARNING: preserve order like in StoredAggregationType to make it easier to
// convert between them
enum class AggregationType {
  kNone,
  kSum,
  kCount,
  kMin,
  kMax,
  kLast,
  kAvg,
};

StoredAggregationType ToStoredAggregationType(AggregationType aggregation_type);
ColumnType ToColumnType(AggregationType aggregation_type);
ColumnType ToColumnType(StoredAggregationType aggregation_type);

}  // namespace tskv
