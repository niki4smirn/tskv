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
  kRawRead,
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
  virtual void Write(const InputTimeSeries& time_series) = 0;
  virtual std::vector<Value> GetValues() const = 0;
  virtual ~IColumn() = default;
};

class IReadColumn : public IColumn {
 public:
  virtual Column Read(const TimeRange& time_range) const = 0;
};

using Column = std::shared_ptr<IColumn>;
using Columns = std::vector<Column>;

class SumColumn : public IReadColumn {
 public:
  SumColumn(size_t bucket_interval);
  SumColumn(const std::vector<double>& buckets, const TimeRange& time_range,
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

class RawTimestampsColumn : public IColumn {
 public:
  friend class ReadRawColumn;
  RawTimestampsColumn() = default;
  RawTimestampsColumn(const std::vector<TimePoint>& timestamps);
  ColumnType GetType() const override;
  CompressedBytes ToBytes() const override;
  void Merge(Column column) override;
  void Write(const InputTimeSeries& time_series) override;
  // not the best way to return timestamps, but I didn't want to break the interface
  std::vector<Value> GetValues() const override;

 private:
  std::vector<TimePoint> timestamps_;
};

class RawValuesColumn : public IColumn {
 public:
  friend class ReadRawColumn;
  RawValuesColumn() = default;
  RawValuesColumn(const std::vector<Value>& values);
  ColumnType GetType() const override;
  CompressedBytes ToBytes() const override;
  void Merge(Column column) override;
  void Write(const InputTimeSeries& time_series) override;
  std::vector<Value> GetValues() const override;

 private:
  std::vector<Value> values_;
};

class ReadRawColumn : public IReadColumn {
 public:
  ReadRawColumn() = default;
  ReadRawColumn(const std::vector<TimePoint>& timestamps,
                const std::vector<Value>& values);
  ColumnType GetType() const override;
  CompressedBytes ToBytes() const override;
  void Merge(Column column) override;
  Column Read(const TimeRange& time_range) const override;
  void Write(const InputTimeSeries& time_series) override;
  std::vector<Value> GetValues() const override;

  std::vector<TimePoint> GetTimestamps() const;

 private:
  std::vector<TimePoint> timestamps_;
  std::vector<Value> values_;
};

Column CreateColumn(ColumnType column_type, size_t bucket_interval);

Column CreateRawColumn(ColumnType column_type);

Column FromBytes(const CompressedBytes& bytes, ColumnType);

}  // namespace tskv
