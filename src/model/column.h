#pragma once

#include <cassert>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>
#include "model/model.h"

namespace tskv {

using CompressedBytes = std::vector<uint8_t>;

template <typename T>
void Append(CompressedBytes& bytes, const T& value) {
  auto begin = reinterpret_cast<const uint8_t*>(&value);
  auto end = begin + sizeof(T);
  bytes.insert(bytes.end(), begin, end);
}

template <typename T>
void Append(CompressedBytes& bytes, T* value, size_t size) {
  auto begin = reinterpret_cast<const uint8_t*>(value);
  auto end = begin + size * sizeof(T);
  bytes.insert(bytes.end(), begin, end);
}

struct CompressedBytesReader {
  explicit CompressedBytesReader(const CompressedBytes& bytes);

  template <typename T>
  T Read() {
    assert(offset_ + sizeof(T) <= bytes_.size());
    auto begin = bytes_.begin() + offset_;
    auto end = begin + sizeof(T);
    offset_ += sizeof(T);
    T value;
    std::copy(begin, end, reinterpret_cast<uint8_t*>(&value));
    return value;
  }

  template <typename T>
  std::vector<T> ReadAll() {
    auto begin = bytes_.begin() + offset_;
    auto end = bytes_.end();
    offset_ += (end - begin);
    std::vector<T> values;
    values.insert(values.end(), reinterpret_cast<const T*>(&*begin),
                  reinterpret_cast<const T*>(&*end));
    return values;
  }

 private:
  const CompressedBytes& bytes_;
  size_t offset_{0};
};

struct Record;
using InputTimeSeries = std::vector<Record>;

// WARNING: preserve order like in AggregationType to make it easier to
// convert between them
enum class ColumnType {
  kSum,
  kCount,
  kMin,
  kMax,
  kLast,
  kRawTimestamps,
  kRawValues,
  kRawRead,
  kAvg,
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
  virtual void Merge(Column column) = 0;
  virtual void Write(const InputTimeSeries& time_series) = 0;
  virtual std::vector<Value> GetValues() const = 0;
  // extracts data from column and clears it
  // returns new column with extracted data
  virtual Column Extract() = 0;
  virtual ~IColumn() = default;
};

class IReadColumn : public IColumn {
 public:
  virtual std::shared_ptr<IReadColumn> Read(
      const TimeRange& time_range) const = 0;
  virtual TimeRange GetTimeRange() const = 0;
};

class ISerializableColumn : public IColumn {
 public:
  virtual CompressedBytes ToBytes() const = 0;
};

using Column = std::shared_ptr<IColumn>;
using ReadColumn = std::shared_ptr<IReadColumn>;
using ReadColumns = std::vector<ReadColumn>;

class IAggregateColumn : public ISerializableColumn, public IReadColumn {
 public:
  virtual void ScaleBuckets(Duration bucket_interval) = 0;
  virtual size_t GetBucketsNum() const = 0;
};

class AggregateColumn {
 public:
  explicit AggregateColumn(Duration bucket_interval);
  AggregateColumn(std::vector<double> buckets, const TimePoint& start_time,
                  Duration bucket_interval);
  ReadColumn Read(const TimeRange& time_range, ColumnType column_type) const;
  std::vector<Value> GetValues() const;
  TimeRange GetTimeRange() const;
  Column Extract(ColumnType column_type);
  CompressedBytes ToBytes() const;

  std::optional<size_t> GetBucketIdx(TimePoint timestamp) const;

  friend class SumColumn;
  friend class CountColumn;
  friend class MinColumn;
  friend class MaxColumn;
  friend class LastColumn;
  friend class AvgColumn;

 private:
  std::vector<double> buckets_;
  TimePoint start_time_{};
  Duration bucket_interval_;
};

using SerializableColumn = std::shared_ptr<ISerializableColumn>;
using Columns = std::vector<Column>;
using SerializableColumns = std::vector<SerializableColumn>;

class SumColumn : public IAggregateColumn {
 public:
  explicit SumColumn(Duration bucket_interval);
  SumColumn(std::vector<double> buckets, const TimePoint& start_time,
            Duration bucket_interval);
  ColumnType GetType() const override;
  void ScaleBuckets(Duration bucket_interval) override;
  void Merge(Column column) override;
  void Write(const InputTimeSeries& time_series) override;
  ReadColumn Read(const TimeRange& time_range) const override;
  std::vector<Value> GetValues() const override;
  TimeRange GetTimeRange() const override;
  Column Extract() override;
  CompressedBytes ToBytes() const override;
  size_t GetBucketsNum() const override;

  friend class AvgColumn;

 private:
  AggregateColumn column_;
  std::vector<double>& buckets_;
  TimePoint& start_time_;
  Duration& bucket_interval_;
};

class CountColumn : public IAggregateColumn {
 public:
  explicit CountColumn(Duration bucket_interval);
  CountColumn(std::vector<double> buckets, const TimePoint& start_time,
              Duration bucket_interval);
  ColumnType GetType() const override;
  void ScaleBuckets(Duration bucket_interval) override;
  void Merge(Column column) override;
  void Write(const InputTimeSeries& time_series) override;
  ReadColumn Read(const TimeRange& time_range) const override;
  std::vector<Value> GetValues() const override;
  TimeRange GetTimeRange() const override;
  Column Extract() override;
  CompressedBytes ToBytes() const override;
  size_t GetBucketsNum() const override;

  friend class AvgColumn;

 private:
  AggregateColumn column_;
  std::vector<double>& buckets_;
  TimePoint& start_time_;
  Duration& bucket_interval_;
};

class MinColumn : public IAggregateColumn {
 public:
  explicit MinColumn(Duration bucket_interval);
  MinColumn(std::vector<double> buckets, const TimePoint& start_time,
            Duration bucket_interval);
  ColumnType GetType() const override;
  void ScaleBuckets(Duration bucket_interval) override;
  void Merge(Column column) override;
  void Write(const InputTimeSeries& time_series) override;
  ReadColumn Read(const TimeRange& time_range) const override;
  std::vector<Value> GetValues() const override;
  TimeRange GetTimeRange() const override;
  Column Extract() override;
  CompressedBytes ToBytes() const override;
  size_t GetBucketsNum() const override;

 private:
  AggregateColumn column_;
  std::vector<double>& buckets_;
  TimePoint& start_time_;
  Duration& bucket_interval_;
};

class MaxColumn : public IAggregateColumn {
 public:
  explicit MaxColumn(Duration bucket_interval);
  MaxColumn(std::vector<double> buckets, const TimePoint& start_time,
            Duration bucket_interval);
  ColumnType GetType() const override;
  void ScaleBuckets(Duration bucket_interval) override;
  void Merge(Column column) override;
  void Write(const InputTimeSeries& time_series) override;
  ReadColumn Read(const TimeRange& time_range) const override;
  std::vector<Value> GetValues() const override;
  TimeRange GetTimeRange() const override;
  Column Extract() override;
  CompressedBytes ToBytes() const override;
  size_t GetBucketsNum() const override;

 private:
  AggregateColumn column_;
  std::vector<double>& buckets_;
  TimePoint& start_time_;
  Duration& bucket_interval_;
};

class LastColumn : public IAggregateColumn {
 public:
  explicit LastColumn(Duration bucket_interval);
  LastColumn(std::vector<double> buckets, const TimePoint& start_time,
             Duration bucket_interval);
  ColumnType GetType() const override;
  void ScaleBuckets(Duration bucket_interval) override;
  void Merge(Column column) override;
  void Write(const InputTimeSeries& time_series) override;
  ReadColumn Read(const TimeRange& time_range) const override;
  std::vector<Value> GetValues() const override;
  TimeRange GetTimeRange() const override;
  Column Extract() override;
  CompressedBytes ToBytes() const override;
  size_t GetBucketsNum() const override;

 private:
  AggregateColumn column_;
  std::vector<double>& buckets_;
  TimePoint& start_time_;
  Duration& bucket_interval_;
};

class RawTimestampsColumn : public ISerializableColumn {
 public:
  friend class ReadRawColumn;
  RawTimestampsColumn() = default;
  explicit RawTimestampsColumn(std::vector<TimePoint> timestamps);
  ColumnType GetType() const override;
  CompressedBytes ToBytes() const override;
  void Merge(Column column) override;
  void Write(const InputTimeSeries& time_series) override;
  // not the best way to return timestamps, but I didn't want to break the interface
  std::vector<Value> GetValues() const override;
  Column Extract() override;
  TimeRange GetTimeRange() const;
  size_t TimestampsNum() const;

 private:
  std::vector<TimePoint> timestamps_;
};

class RawValuesColumn : public ISerializableColumn {
 public:
  friend class ReadRawColumn;
  RawValuesColumn() = default;
  explicit RawValuesColumn(std::vector<Value> values);
  ColumnType GetType() const override;
  CompressedBytes ToBytes() const override;
  void Merge(Column column) override;
  void Write(const InputTimeSeries& time_series) override;
  std::vector<Value> GetValues() const override;
  Column Extract() override;
  size_t ValuesNum() const;

 private:
  std::vector<Value> values_;
};

class ReadRawColumn : public IReadColumn {
 public:
  ReadRawColumn() = default;
  ReadRawColumn(std::shared_ptr<RawTimestampsColumn> timestamps_column,
                std::shared_ptr<RawValuesColumn> values_column);
  ColumnType GetType() const override;
  void Merge(Column column) override;
  ReadColumn Read(const TimeRange& time_range) const override;
  void Write(const InputTimeSeries& time_series) override;
  std::vector<Value> GetValues() const override;
  TimeRange GetTimeRange() const override;
  Column Extract() override;

  std::vector<TimePoint> GetTimestamps() const;

 private:
  std::shared_ptr<RawTimestampsColumn> timestamps_column_;
  std::shared_ptr<RawValuesColumn> values_column_;
};

class AvgColumn : public IReadColumn {
 public:
  AvgColumn(std::vector<double> buckets, const TimePoint& start_time,
            Duration bucket_interval);
  AvgColumn(std::shared_ptr<SumColumn> sum_column,
            std::shared_ptr<CountColumn> count_column);
  ColumnType GetType() const override;
  void Merge(Column column) override;
  ReadColumn Read(const TimeRange& time_range) const override;
  void Write(const InputTimeSeries& time_series) override;
  std::vector<Value> GetValues() const override;
  TimeRange GetTimeRange() const override;
  Column Extract() override;

 private:
  static AggregateColumn CreateAvgAggregateColumn(
      std::shared_ptr<SumColumn> sum_column,
      std::shared_ptr<CountColumn> count_column);

 private:
  AggregateColumn column_;
};

template <typename T>
Column CreateAggregatedColumn(Duration bucket_interval) {
  auto col = std::make_shared<T>(bucket_interval);
  // some strange things here, because in cpp we don't have such thing as
  // interface, so in case of diamond interface inheritance we need to
  // explicitly cast to the base class
  auto read_column = std::static_pointer_cast<IReadColumn>(col);
  return std::static_pointer_cast<IColumn>(read_column);
}

Column CreateAggregatedColumn(ColumnType column_type, Duration bucket_interval);

Column CreateRawColumn(ColumnType column_type);

template <typename T>
Column AggregateFromBytes(const CompressedBytes& bytes) {
  auto reader = CompressedBytesReader(bytes);
  auto bucket_interval = reader.Read<size_t>();
  auto start = reader.Read<TimePoint>();
  auto buckets = reader.ReadAll<Value>();
  auto col = std::make_shared<T>(buckets, start, bucket_interval);
  auto read_column = std::static_pointer_cast<IReadColumn>(col);
  return std::static_pointer_cast<IColumn>(read_column);
}

Column FromBytes(const CompressedBytes& bytes, ColumnType column_type);

}  // namespace tskv
