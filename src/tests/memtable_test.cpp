#include "gtest/gtest.h"

#include "memtable/memtable.h"
#include "metric-storage/metric_storage.h"
#include "model/aggregations.h"
#include "model/column.h"
#include "model/model.h"

TEST(Memtable, ReadWrite) {
  tskv::Memtable memtable(
      tskv::Memtable::Options{
          .bucket_interval = 2,
          .max_bytes_size = 4 * sizeof(double) - 1,
      },
      tskv::MetricOptions{{tskv::StoredAggregationType::kSum}});
  auto read_res =
      memtable.Read(tskv::TimeRange{0, 100}, tskv::StoredAggregationType::kSum);
  ASSERT_FALSE(read_res.found);
  ASSERT_EQ(*read_res.not_found, tskv::TimeRange(0, 100));
  ASSERT_FALSE(memtable.NeedFlush());

  memtable.Write(
      tskv::InputTimeSeries{{3, 10}, {4, 1}, {5, 2}, {7, 3}, {7, 1}});
  read_res =
      memtable.Read(tskv::TimeRange{1, 7}, tskv::StoredAggregationType::kSum);
  auto expected = std::vector<double>{10, 3, 4};
  ASSERT_EQ(read_res.found->GetValues(), expected);
  ASSERT_EQ(*read_res.not_found, tskv::TimeRange(1, 2));
  ASSERT_FALSE(memtable.NeedFlush());

  read_res =
      memtable.Read(tskv::TimeRange{0, 2}, tskv::StoredAggregationType::kSum);
  ASSERT_FALSE(read_res.found);
  ASSERT_EQ(*read_res.not_found, tskv::TimeRange(0, 2));

  read_res =
      memtable.Read(tskv::TimeRange{0, 3}, tskv::StoredAggregationType::kSum);
  expected = std::vector<double>{10};
  ASSERT_EQ(read_res.found->GetValues(), expected);
  ASSERT_EQ(*read_res.not_found, tskv::TimeRange(0, 2));

  read_res =
      memtable.Read(tskv::TimeRange{3, 5}, tskv::StoredAggregationType::kSum);
  expected = std::vector<double>{10, 3};
  ASSERT_EQ(read_res.found->GetValues(), expected);
  ASSERT_FALSE(read_res.not_found.has_value());

  memtable.Write(tskv::InputTimeSeries{{8, -1}});
  read_res =
      memtable.Read(tskv::TimeRange{2, 9}, tskv::StoredAggregationType::kSum);
  expected = std::vector<double>{10, 3, 4, -1};
  ASSERT_EQ(read_res.found->GetValues(), expected);
  ASSERT_FALSE(read_res.not_found.has_value());
  ASSERT_TRUE(memtable.NeedFlush());

  read_res =
      memtable.Read(tskv::TimeRange{0, 100}, tskv::StoredAggregationType::kSum);
  expected = std::vector<double>{10, 3, 4, -1};
  ASSERT_EQ(read_res.found->GetValues(), expected);
  ASSERT_EQ(*read_res.not_found, tskv::TimeRange(0, 2));

  read_res =
      memtable.Read(tskv::TimeRange{5, 7}, tskv::StoredAggregationType::kSum);
  expected = std::vector<double>{3, 4};
  ASSERT_EQ(read_res.found->GetValues(), expected);
  ASSERT_FALSE(read_res.not_found.has_value());
}

TEST(Memtable, ExtractColumns) {
  tskv::Memtable memtable(
      tskv::Memtable::Options{
          .bucket_interval = 2,
          .max_bytes_size = 1000,
          .store_raw = true,
      },
      tskv::MetricOptions{{tskv::StoredAggregationType::kSum}});

  memtable.Write(
      tskv::InputTimeSeries{{3, 10}, {4, 1}, {5, 2}, {7, 3}, {7, 1}});
  auto columns = memtable.ExtractColumns();

  EXPECT_EQ(columns.size(), 3);
  for (auto& column : columns) {
    if (column->GetType() == tskv::ColumnType::kSum) {
      auto read_column = std::static_pointer_cast<tskv::IReadColumn>(column);
      auto expected = std::vector<double>{10, 3, 4};
      ASSERT_EQ(read_column->GetValues(), expected);
      ASSERT_EQ(read_column->GetTimeRange(), tskv::TimeRange(2, 8));
    } else if (column->GetType() == tskv::ColumnType::kRawTimestamps) {
      auto expected = std::vector<double>{3, 4, 5, 7, 7};
      ASSERT_EQ(column->GetValues(), expected);
    } else if (column->GetType() == tskv::ColumnType::kRawValues) {
      auto expected = std::vector<double>{10, 1, 2, 3, 1};
      ASSERT_EQ(column->GetValues(), expected);
    } else {
      FAIL();
    }
  }
}
