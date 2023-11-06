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
  virtual std::shared_ptr<IReadColumn> Read(
      const TimeRange& time_range) const = 0;
  virtual TimeRange GetTimeRange() const = 0;
};

using Column = std::shared_ptr<IColumn>;
using ReadColumn = std::shared_ptr<IReadColumn>;
using Columns = std::vector<Column>;

class SumColumn : public IReadColumn {
 public:
  explicit SumColumn(size_t bucket_interval);
  SumColumn(const std::vector<double>& buckets, const TimeRange& time_range,
            size_t bucket_interval);
  ColumnType GetType() const override;
  CompressedBytes ToBytes() const override;
  void Merge(Column column) override;
  ReadColumn Read(const TimeRange& time_range) const override;
  void Write(const InputTimeSeries& time_series) override;
  std::vector<Value> GetValues() const override;
  TimeRange GetTimeRange() const override;

 private:
  std::optional<size_t> GetBucketIdx(TimePoint timestamp) const;

 private:
  std::vector<Value> buckets_;
  TimeRange time_range_{};
  size_t bucket_interval_;
};

class RawTimestampsColumn : public IColumn {
 public:
  friend class ReadRawColumn;
  RawTimestampsColumn() = default;
  explicit RawTimestampsColumn(const std::vector<TimePoint>& timestamps);
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
  explicit RawValuesColumn(const std::vector<Value>& values);
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
  ReadRawColumn(std::shared_ptr<RawTimestampsColumn> timestamps_column,
                std::shared_ptr<RawValuesColumn> values_column);
  ColumnType GetType() const override;
  CompressedBytes ToBytes() const override;
  void Merge(Column column) override;
  ReadColumn Read(const TimeRange& time_range) const override;
  void Write(const InputTimeSeries& time_series) override;
  std::vector<Value> GetValues() const override;
  TimeRange GetTimeRange() const override;

  std::vector<TimePoint> GetTimestamps() const;

 private:
  std::shared_ptr<RawTimestampsColumn> timestamps_column_;
  std::shared_ptr<RawValuesColumn> values_column_;
};

Column CreateColumn(ColumnType column_type, size_t bucket_interval);

Column CreateRawColumn(ColumnType column_type);

Column FromBytes(const CompressedBytes& bytes, ColumnType column_type);

}  // namespace tskv
