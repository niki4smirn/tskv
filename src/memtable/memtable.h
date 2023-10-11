#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "model/model.h"

namespace tskv {

struct Options {
  Duration bucket_inteval;
  TimeRange time_range;
};

class IAggregation {
 public:
  virtual ~IAggregation() = default;
  IAggregation(const Options& options);
  virtual void Write(const Record& record) = 0;
  virtual TimeSeries Read(const TimeRange& time_range) = 0;
};

class RawData {
 public:
  void Write(const Record& record);
  TimeSeries Read(const TimeRange& time_range);

 private:
  std::vector<TimePoint> timepoints_;
  std::vector<Value> values_;
};

class Memtable {
 public:
  Memtable(const Options& options);
  void Write(const TimeSeries& time_series);
  TimeSeries Read(const TimeRange& range,
                  StoredAggregationType aggregation_type);
  std::vector<Column> ToColumns();

 private:
  // we should store aggregations OR raw_data, but I don't like using std::variant,
  // so we will store both until I find better representation
  std::array<std::shared_ptr<IAggregation>, kStoredAggregationsNum>
      aggregations_;
  RawData raw_data_;
};

}  // namespace tskv
