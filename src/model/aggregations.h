#include <cstddef>

namespace tskv {

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

// WARNING: preserve order like in StoredAggregationType to make it easier to
// convert between them
enum class AggregationType {
  kSum,
  kCount,
  kMin,
  kMax,
  kAvg,
};

StoredAggregationType ToStoredAggregationType(AggregationType aggregation_type);

}  // namespace tskv
