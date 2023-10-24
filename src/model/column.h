#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>
#include "model/model.h"

namespace tskv {

using CompressedBytes = std::vector<uint8_t>;
struct Record;
using InputTimeSeries = std::vector<Record>;

// WARNING: preserve order like in AggregationType to make it easier to
// convert between them
enum class ColumnType {
  kSum,
  kCount,
  kMin,
  kMax,
  kRawTimestamps,
  kRawValues,
};

// I think, that Column should stores data vector with offsets and lengths, so
// that we don't need to copy data in some cases
//
// But for now I implement it in a simple way
class IColumn {
 protected:
  using Column = std::shared_ptr<IColumn>;

 public:
  virtual ColumnType GetType() const = 0;
  virtual CompressedBytes ToBytes() const = 0;
  virtual void Merge(Column column) = 0;
  virtual Column Read(const TimeRange& time_range) const = 0;
  virtual void Write(const InputTimeSeries& time_series) = 0;
  virtual std::vector<Value> GetValues() const = 0;
  virtual ~IColumn() = default;
};

using Column = std::shared_ptr<IColumn>;
using Columns = std::vector<Column>;

class SumColumn : public IColumn {
 public:
  SumColumn(size_t bucket_interval);
  SumColumn(std::vector<double> buckets, const TimeRange& time_range,
            size_t bucket_interval);
  ColumnType GetType() const override;
  CompressedBytes ToBytes() const override;
  void Merge(Column column) override;
  Column Read(const TimeRange& time_range) const override;
  void Write(const InputTimeSeries& time_series) override;
  std::vector<Value> GetValues() const override;

 private:
  std::optional<size_t> GetBucketIdx(TimePoint timestamp) const;

 private:
  std::vector<Value> buckets_;
  TimeRange time_range_;
  size_t bucket_interval_;
};

template <typename... Args>
Column CreateColumn(ColumnType column_type, Args&&... args) {
  switch (column_type) {
    case ColumnType::kSum:
      return std::make_shared<SumColumn>(std::forward<Args>(args)...);
    default:
      throw std::runtime_error("Unsupported column type");
  }
}

Column FromBytes(const CompressedBytes& bytes, ColumnType column_type);

}  // namespace tskv
