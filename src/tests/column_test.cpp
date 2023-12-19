#include <gtest/gtest.h>
#include <memory>

#include "model/column.h"
#include "model/model.h"

TEST(SumColumn, Basic) {
  tskv::SumColumn column(std::vector<double>{1, 2, 3, 4, 5}, tskv::TimePoint(1),
                         1);
  EXPECT_EQ(column.GetType(), tskv::ColumnType::kSum);
  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(column.GetValues(), expected);
  EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 6));
}

TEST(SumColumn, Write) {
  {
    tskv::SumColumn column(1);
    column.Write({{1, 1}, {2, 2}, {2, 1}, {3, 1}, {3, 10}, {4, 2}, {4, -1}});
    auto expected = std::vector<double>{1, 3, 11, 1};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 5));

    column.Write({{4, 3}, {5, 11}, {6, 8}, {6, 7}});
    expected = std::vector<double>{1, 3, 11, 4, 11, 15};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 7));

    column.Write({{7, 1}, {7, 2}, {7, 3}, {7, 4}});
    expected = std::vector<double>{1, 3, 11, 4, 11, 15, 10};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 8));
  }
  {
    tskv::SumColumn column(2);
    column.Write({{1, 1}, {2, 2}, {2, 1}, {3, 1}, {3, 10}, {4, 2}, {4, -1}});
    auto expected = std::vector<double>{1, 14, 1};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 6));

    column.Write({{4, 3}, {5, 11}, {6, 8}, {6, 7}});
    expected = std::vector<double>{1, 14, 15, 15};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 8));

    column.Write({{7, 1}, {7, 2}, {7, 3}, {7, 4}});
    expected = std::vector<double>{1, 14, 15, 25};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 8));
  }
}

TEST(SumColumn, Read) {
  {
    tskv::SumColumn column(std::vector<double>{1, 2, 3, 4, 5},
                           tskv::TimePoint(1), 1);
    auto expected = std::vector<double>{1, 2, 3, 4, 5};
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 6))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 6))->GetTimeRange(),
              tskv::TimeRange(1, 6));
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 6))->GetType(),
              tskv::ColumnType::kSum);

    expected = std::vector<double>{1, 2, 3, 4};
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 5))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 5))->GetTimeRange(),
              tskv::TimeRange(1, 5));
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 5))->GetType(),
              tskv::ColumnType::kSum);

    expected = std::vector<double>{2, 3, 4, 5};
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 6))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 6))->GetTimeRange(),
              tskv::TimeRange(2, 6));
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 6))->GetType(),
              tskv::ColumnType::kSum);

    expected = std::vector<double>{3};
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 4))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 4))->GetTimeRange(),
              tskv::TimeRange(3, 4));
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 4))->GetType(),
              tskv::ColumnType::kSum);
  }
  {
    tskv::SumColumn column(std::vector<double>{1, 2, 3, 4, 5},
                           tskv::TimePoint(2), 2);
    auto expected = std::vector<double>{1, 2, 3, 4, 5};
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 12))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 12))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 12))->GetType(),
              tskv::ColumnType::kSum);

    EXPECT_EQ(column.Read(tskv::TimeRange(3, 12))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 12))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 12))->GetType(),
              tskv::ColumnType::kSum);

    EXPECT_EQ(column.Read(tskv::TimeRange(1, 100))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 100))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 100))->GetType(),
              tskv::ColumnType::kSum);

    EXPECT_EQ(column.Read(tskv::TimeRange(2, 11))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 11))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 11))->GetType(),
              tskv::ColumnType::kSum);

    expected = std::vector<double>{1, 2, 3, 4};
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 10))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 10))->GetTimeRange(),
              tskv::TimeRange(2, 10));
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 10))->GetType(),
              tskv::ColumnType::kSum);

    expected = std::vector<double>{2, 3, 4, 5};
    EXPECT_EQ(column.Read(tskv::TimeRange(4, 12))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(4, 12))->GetTimeRange(),
              tskv::TimeRange(4, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(4, 12))->GetType(),
              tskv::ColumnType::kSum);

    EXPECT_EQ(column.Read(tskv::TimeRange(5, 12))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(5, 12))->GetTimeRange(),
              tskv::TimeRange(4, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(5, 12))->GetType(),
              tskv::ColumnType::kSum);

    expected = std::vector<double>{3};
    EXPECT_EQ(column.Read(tskv::TimeRange(6, 8))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(6, 8))->GetTimeRange(),
              tskv::TimeRange(6, 8));
    EXPECT_EQ(column.Read(tskv::TimeRange(6, 8))->GetType(),
              tskv::ColumnType::kSum);

    expected = std::vector<double>{3, 4};
    EXPECT_EQ(column.Read(tskv::TimeRange(6, 9))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(6, 9))->GetTimeRange(),
              tskv::TimeRange(6, 10));
    EXPECT_EQ(column.Read(tskv::TimeRange(6, 9))->GetType(),
              tskv::ColumnType::kSum);
  }
}

TEST(SumColumn, Merge) {
  {
    tskv::SumColumn column1(std::vector<double>{1, 2, 3, 4, 5},
                            tskv::TimePoint(1), 1);
    tskv::SumColumn column2(std::vector<double>{5, 4, 3}, tskv::TimePoint(3),
                            1);
    std::shared_ptr<tskv::IReadColumn> column2_read =
        std::make_shared<tskv::SumColumn>(column2);
    column1.Merge(column2_read);
    auto expected = std::vector<double>{1, 2, 8, 8, 8};
    EXPECT_EQ(column1.GetValues(), expected);
    EXPECT_EQ(column1.GetTimeRange(), tskv::TimeRange(1, 6));
  }
  {
    tskv::SumColumn column1(std::vector<double>{1, 2, 3}, tskv::TimePoint(3),
                            3);
    tskv::SumColumn column2(std::vector<double>{10, 20}, tskv::TimePoint(9), 3);
    std::shared_ptr<tskv::IReadColumn> column2_read =
        std::make_shared<tskv::SumColumn>(column2);
    column1.Merge(column2_read);
    auto expected = std::vector<double>{1, 2, 13, 20};
    EXPECT_EQ(column1.GetValues(), expected);
    EXPECT_EQ(column1.GetTimeRange(), tskv::TimeRange(3, 15));
  }
}

TEST(SumColumn, Extract) {
  tskv::SumColumn column(std::vector<double>{1, 2, 3, 4, 5}, tskv::TimePoint(5),
                         5);
  auto result = std::static_pointer_cast<tskv::IReadColumn>(column.Extract());
  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(result->GetValues(), expected);
  EXPECT_EQ(result->GetTimeRange(), tskv::TimeRange(5, 30));
  EXPECT_EQ(result->GetType(), tskv::ColumnType::kSum);
  EXPECT_TRUE(column.GetValues().empty());
  EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 0));
}

TEST(SumColumn, ToBytes) {
  tskv::SumColumn column(std::vector<double>{1, 2, 3, 4, 5},
                         tskv::TimePoint(45), 15);
  auto bytes = column.ToBytes();
  auto expected = std::vector<uint8_t>{
      15, 0,  0, 0,   0,  0, 0, 0, 45, 0,  0, 0, 0,  0, 0, 0, 0,  0, 0,
      0,  0,  0, 240, 63, 0, 0, 0, 0,  0,  0, 0, 64, 0, 0, 0, 0,  0, 0,
      8,  64, 0, 0,   0,  0, 0, 0, 16, 64, 0, 0, 0,  0, 0, 0, 20, 64};
  EXPECT_EQ(bytes, expected);
}

TEST(SumColumn, FromBytes) {
  auto bytes = std::vector<uint8_t>{
      15, 0,  0, 0,   0,  0, 0, 0, 45, 0,  0, 0, 0,  0, 0, 0, 0,  0, 0,
      0,  0,  0, 240, 63, 0, 0, 0, 0,  0,  0, 0, 64, 0, 0, 0, 0,  0, 0,
      8,  64, 0, 0,   0,  0, 0, 0, 16, 64, 0, 0, 0,  0, 0, 0, 20, 64};
  auto read_column = std::static_pointer_cast<tskv::IReadColumn>(
      tskv::FromBytes(bytes, tskv::ColumnType::kSum));
  auto sum_column = std::static_pointer_cast<tskv::SumColumn>(read_column);
  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(sum_column->GetValues(), expected);
  EXPECT_EQ(sum_column->GetTimeRange(), tskv::TimeRange(45, 120));
}

TEST(SumColumn, ScaleBuckets) {
  {
    tskv::SumColumn column(std::vector<double>{1, 2, 3, 4, 5},
                           tskv::TimePoint(1), 1);
    column.ScaleBuckets(2);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kSum);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 6));
    auto expected = std::vector<double>{1, 5, 9};
    EXPECT_EQ(column.GetValues(), expected);
  }
  {
    tskv::SumColumn column(std::vector<double>{1, 4, 2, 3, 9, 15, 0, 1, 8, 5},
                           tskv::TimePoint(2), 2);
    column.ScaleBuckets(2);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kSum);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(2, 22));
    auto expected = std::vector<double>{1, 4, 2, 3, 9, 15, 0, 1, 8, 5};
    EXPECT_EQ(column.GetValues(), expected);
  }
  {
    tskv::SumColumn column(std::vector<double>{1, 4, 2, 3, 9, 15, 0, 1, 8, 5},
                           tskv::TimePoint(2), 2);
    column.ScaleBuckets(6);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kSum);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 24));
    auto expected = std::vector<double>{5, 14, 16, 13};
    EXPECT_EQ(column.GetValues(), expected);
  }
  {
    tskv::SumColumn column(std::vector<double>{1, 4, 2, 3, 9, 15, 0, 1, 8},
                           tskv::TimePoint(0), 2);
    column.ScaleBuckets(4);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kSum);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 20));
    auto expected = std::vector<double>{5, 5, 24, 1, 8};
    EXPECT_EQ(column.GetValues(), expected);
  }
  {
    tskv::SumColumn column(std::vector<double>{0, 0, 0}, tskv::TimePoint(0), 1);
    column.ScaleBuckets(2);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kSum);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 4));
    auto expected = std::vector<double>{0, 0};
    EXPECT_EQ(column.GetValues(), expected);
  }
}

TEST(CountColumn, Basic) {
  tskv::CountColumn column(std::vector<double>{1, 2, 3, 4, 5},
                           tskv::TimePoint(1), 1);
  EXPECT_EQ(column.GetType(), tskv::ColumnType::kCount);
  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(column.GetValues(), expected);
  EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 6));
}

TEST(CountColumn, Write) {
  {
    tskv::CountColumn column(1);
    column.Write({{1, 1}, {2, 2}, {2, 1}, {3, 1}, {3, 10}, {4, 2}, {4, -1}});
    auto expected = std::vector<double>{1, 2, 2, 2};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 5));

    column.Write({{4, 3}, {5, 11}, {6, 8}, {6, 7}});
    expected = std::vector<double>{1, 2, 2, 3, 1, 2};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 7));

    column.Write({{7, 1}, {7, 2}, {7, 3}, {7, 4}});
    expected = std::vector<double>{1, 2, 2, 3, 1, 2, 4};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 8));
  }
  {
    tskv::CountColumn column(2);
    column.Write({{1, 1}, {2, 2}, {2, 1}, {3, 1}, {3, 10}, {4, 2}, {4, -1}});
    auto expected = std::vector<double>{1, 4, 2};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 6));

    column.Write({{4, 3}, {5, 11}, {6, 8}, {6, 7}});
    expected = std::vector<double>{1, 4, 4, 2};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 8));

    column.Write({{7, 1}, {7, 2}, {7, 3}, {7, 4}});
    expected = std::vector<double>{1, 4, 4, 6};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 8));
  }
}

TEST(CountColumn, Read) {
  {
    tskv::CountColumn column(std::vector<double>{1, 2, 3, 4, 5},
                             tskv::TimePoint(1), 1);
    auto expected = std::vector<double>{1, 2, 3, 4, 5};
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 6))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 6))->GetTimeRange(),
              tskv::TimeRange(1, 6));
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 6))->GetType(),
              tskv::ColumnType::kCount);

    expected = std::vector<double>{1, 2, 3, 4};
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 5))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 5))->GetTimeRange(),
              tskv::TimeRange(1, 5));
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 5))->GetType(),
              tskv::ColumnType::kCount);

    expected = std::vector<double>{2, 3, 4, 5};
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 6))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 6))->GetTimeRange(),
              tskv::TimeRange(2, 6));
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 6))->GetType(),
              tskv::ColumnType::kCount);

    expected = std::vector<double>{3};
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 4))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 4))->GetTimeRange(),
              tskv::TimeRange(3, 4));
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 4))->GetType(),
              tskv::ColumnType::kCount);
  }
  {
    tskv::CountColumn column(std::vector<double>{1, 2, 3, 4, 5},
                             tskv::TimePoint(2), 2);
    auto expected = std::vector<double>{1, 2, 3, 4, 5};
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 12))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 12))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 12))->GetType(),
              tskv::ColumnType::kCount);

    EXPECT_EQ(column.Read(tskv::TimeRange(3, 12))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 12))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 12))->GetType(),
              tskv::ColumnType::kCount);

    EXPECT_EQ(column.Read(tskv::TimeRange(1, 100))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 100))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 100))->GetType(),
              tskv::ColumnType::kCount);

    EXPECT_EQ(column.Read(tskv::TimeRange(2, 11))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 11))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 11))->GetType(),
              tskv::ColumnType::kCount);
  }
}

TEST(CountColumn, Merge) {
  {
    tskv::CountColumn column1(std::vector<double>{1, 2, 3, 4, 5},
                              tskv::TimePoint(1), 1);
    tskv::CountColumn column2(std::vector<double>{5, 4, 3}, tskv::TimePoint(3),
                              1);
    std::shared_ptr<tskv::IReadColumn> column2_read =
        std::make_shared<tskv::CountColumn>(column2);
    column1.Merge(column2_read);
    auto expected = std::vector<double>{1, 2, 8, 8, 8};
    EXPECT_EQ(column1.GetValues(), expected);
    EXPECT_EQ(column1.GetTimeRange(), tskv::TimeRange(1, 6));
  }
  {
    tskv::CountColumn column1(std::vector<double>{1, 2, 3}, tskv::TimePoint(3),
                              3);
    tskv::CountColumn column2(std::vector<double>{10, 20}, tskv::TimePoint(9),
                              3);
    std::shared_ptr<tskv::IReadColumn> column2_read =
        std::make_shared<tskv::CountColumn>(column2);
    column1.Merge(column2_read);
    auto expected = std::vector<double>{1, 2, 13, 20};
    EXPECT_EQ(column1.GetValues(), expected);
    EXPECT_EQ(column1.GetTimeRange(), tskv::TimeRange(3, 15));
  }
}

TEST(CountColumn, Extract) {
  tskv::CountColumn column(std::vector<double>{1, 2, 3, 4, 5},
                           tskv::TimePoint(5), 5);
  auto result = std::static_pointer_cast<tskv::IReadColumn>(column.Extract());
  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(result->GetValues(), expected);
  EXPECT_EQ(result->GetTimeRange(), tskv::TimeRange(5, 30));
  EXPECT_EQ(result->GetType(), tskv::ColumnType::kCount);
  EXPECT_TRUE(column.GetValues().empty());
  EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 0));
}

TEST(CountColumn, ToBytes) {
  tskv::CountColumn column(std::vector<double>{1, 2, 3, 4, 5},
                           tskv::TimePoint(45), 15);
  auto bytes = column.ToBytes();
  auto expected = std::vector<uint8_t>{
      15, 0,  0, 0,   0,  0, 0, 0, 45, 0,  0, 0, 0,  0, 0, 0, 0,  0, 0,
      0,  0,  0, 240, 63, 0, 0, 0, 0,  0,  0, 0, 64, 0, 0, 0, 0,  0, 0,
      8,  64, 0, 0,   0,  0, 0, 0, 16, 64, 0, 0, 0,  0, 0, 0, 20, 64};
  EXPECT_EQ(bytes, expected);
}

TEST(CountColumn, FromBytes) {
  auto bytes = std::vector<uint8_t>{
      15, 0,  0, 0,   0,  0, 0, 0, 45, 0,  0, 0, 0,  0, 0, 0, 0,  0, 0,
      0,  0,  0, 240, 63, 0, 0, 0, 0,  0,  0, 0, 64, 0, 0, 0, 0,  0, 0,
      8,  64, 0, 0,   0,  0, 0, 0, 16, 64, 0, 0, 0,  0, 0, 0, 20, 64};
  auto read_column = std::static_pointer_cast<tskv::IReadColumn>(
      tskv::FromBytes(bytes, tskv::ColumnType::kCount));
  auto count_column = std::static_pointer_cast<tskv::CountColumn>(read_column);
  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(count_column->GetValues(), expected);
  EXPECT_EQ(count_column->GetTimeRange(), tskv::TimeRange(45, 120));
}

TEST(CountColumn, ScaleBuckets) {
  {
    tskv::CountColumn column(std::vector<double>{1, 2, 3, 4, 5},
                             tskv::TimePoint(1), 1);
    column.ScaleBuckets(2);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kCount);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 6));
    auto expected = std::vector<double>{1, 5, 9};
    EXPECT_EQ(column.GetValues(), expected);
  }
  {
    tskv::CountColumn column(std::vector<double>{1, 4, 2, 3, 9, 15, 0, 1, 8, 5},
                             tskv::TimePoint(2), 2);
    column.ScaleBuckets(2);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kCount);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(2, 22));
    auto expected = std::vector<double>{1, 4, 2, 3, 9, 15, 0, 1, 8, 5};
    EXPECT_EQ(column.GetValues(), expected);
  }
  {
    tskv::CountColumn column(std::vector<double>{1, 4, 2, 3, 9, 15, 0, 1, 8, 5},
                             tskv::TimePoint(2), 2);
    column.ScaleBuckets(6);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kCount);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 24));
    auto expected = std::vector<double>{5, 14, 16, 13};
    EXPECT_EQ(column.GetValues(), expected);
  }
  {
    tskv::CountColumn column(std::vector<double>{1, 4, 2, 3, 9, 15, 0, 1, 8},
                             tskv::TimePoint(0), 2);
    column.ScaleBuckets(4);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kCount);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 20));
    auto expected = std::vector<double>{5, 5, 24, 1, 8};
    EXPECT_EQ(column.GetValues(), expected);
  }
  {
    tskv::CountColumn column(std::vector<double>{0, 0, 0}, tskv::TimePoint(0),
                             1);
    column.ScaleBuckets(2);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kCount);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 4));
    auto expected = std::vector<double>{0, 0};
    EXPECT_EQ(column.GetValues(), expected);
  }
}

TEST(MinColumn, Basic) {
  tskv::MinColumn column(std::vector<double>{1, 2, 3, 4, 5}, tskv::TimePoint(1),
                         1);
  EXPECT_EQ(column.GetType(), tskv::ColumnType::kMin);
  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(column.GetValues(), expected);
  EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 6));
}

TEST(MinColumn, Write) {
  {
    tskv::MinColumn column(1);
    column.Write({{1, 1}, {2, 2}, {2, 1}, {3, 1}, {3, 10}, {4, 2}, {4, -1}});
    auto expected = std::vector<double>{1, 1, 1, -1};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 5));

    column.Write({{4, 3}, {5, 11}, {6, 8}, {6, 7}});
    expected = std::vector<double>{1, 1, 1, -1, 11, 7};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 7));

    column.Write({{7, 1}, {7, 2}, {7, 3}, {7, 4}});
    expected = std::vector<double>{1, 1, 1, -1, 11, 7, 1};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 8));
  }
  {
    tskv::MinColumn column(2);
    column.Write({{1, 1}, {2, 2}, {2, 1}, {3, 1}, {3, 10}, {4, 2}, {4, -1}});
    auto expected = std::vector<double>{1, 1, -1};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 6));

    column.Write({{4, 3}, {5, 11}, {6, 8}, {6, 7}});
    expected = std::vector<double>{1, 1, -1, 7};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 8));

    column.Write({{7, 1}, {7, 2}, {7, 3}, {7, 4}});
    expected = std::vector<double>{1, 1, -1, 1};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 8));
  }
}

TEST(MinColumn, Read) {
  {
    tskv::MinColumn column(std::vector<double>{1, 2, 3, 4, 5},
                           tskv::TimePoint(1), 1);
    auto expected = std::vector<double>{1, 2, 3, 4, 5};
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 6))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 6))->GetTimeRange(),
              tskv::TimeRange(1, 6));
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 6))->GetType(),
              tskv::ColumnType::kMin);

    expected = std::vector<double>{1, 2, 3, 4};
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 5))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 5))->GetTimeRange(),
              tskv::TimeRange(1, 5));
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 5))->GetType(),
              tskv::ColumnType::kMin);

    expected = std::vector<double>{2, 3, 4, 5};
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 6))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 6))->GetTimeRange(),
              tskv::TimeRange(2, 6));
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 6))->GetType(),
              tskv::ColumnType::kMin);

    expected = std::vector<double>{3};
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 4))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 4))->GetTimeRange(),
              tskv::TimeRange(3, 4));
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 4))->GetType(),
              tskv::ColumnType::kMin);
  }
  {
    tskv::MinColumn column(std::vector<double>{1, 2, 3, 4, 5},
                           tskv::TimePoint(2), 2);
    auto expected = std::vector<double>{1, 2, 3, 4, 5};
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 12))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 12))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 12))->GetType(),
              tskv::ColumnType::kMin);

    EXPECT_EQ(column.Read(tskv::TimeRange(3, 12))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 12))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 12))->GetType(),
              tskv::ColumnType::kMin);

    EXPECT_EQ(column.Read(tskv::TimeRange(1, 100))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 100))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 100))->GetType(),
              tskv::ColumnType::kMin);

    EXPECT_EQ(column.Read(tskv::TimeRange(2, 11))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 11))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 11))->GetType(),
              tskv::ColumnType::kMin);
  }
}

TEST(MinColumn, Merge) {
  {
    tskv::MinColumn column1(std::vector<double>{1, 2, 3, 4, 5},
                            tskv::TimePoint(1), 1);
    tskv::MinColumn column2(std::vector<double>{5, 4, 3}, tskv::TimePoint(3),
                            1);
    std::shared_ptr<tskv::IReadColumn> column2_read =
        std::make_shared<tskv::MinColumn>(column2);
    column1.Merge(column2_read);
    auto expected = std::vector<double>{1, 2, 3, 4, 3};
    EXPECT_EQ(column1.GetValues(), expected);
    EXPECT_EQ(column1.GetTimeRange(), tskv::TimeRange(1, 6));
  }
  {
    tskv::MinColumn column1(std::vector<double>{1, 2, 3}, tskv::TimePoint(3),
                            3);
    tskv::MinColumn column2(std::vector<double>{10, 20}, tskv::TimePoint(9), 3);
    std::shared_ptr<tskv::IReadColumn> column2_read =
        std::make_shared<tskv::MinColumn>(column2);
    column1.Merge(column2_read);
    auto expected = std::vector<double>{1, 2, 3, 20};
    EXPECT_EQ(column1.GetValues(), expected);
    EXPECT_EQ(column1.GetTimeRange(), tskv::TimeRange(3, 15));
  }
}

TEST(MinColumn, Extract) {
  tskv::MinColumn column(std::vector<double>{1, 2, 3, 4, 5}, tskv::TimePoint(5),
                         5);
  auto result = std::static_pointer_cast<tskv::IReadColumn>(column.Extract());
  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(result->GetValues(), expected);
  EXPECT_EQ(result->GetTimeRange(), tskv::TimeRange(5, 30));
  EXPECT_EQ(result->GetType(), tskv::ColumnType::kMin);
  EXPECT_TRUE(column.GetValues().empty());
  EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 0));
}

TEST(MinColumn, ToBytes) {
  tskv::MinColumn column(std::vector<double>{1, 2, 3, 4, 5},
                         tskv::TimePoint(45), 15);
  auto bytes = column.ToBytes();
  auto expected = std::vector<uint8_t>{
      15, 0,  0, 0,   0,  0, 0, 0, 45, 0,  0, 0, 0,  0, 0, 0, 0,  0, 0,
      0,  0,  0, 240, 63, 0, 0, 0, 0,  0,  0, 0, 64, 0, 0, 0, 0,  0, 0,
      8,  64, 0, 0,   0,  0, 0, 0, 16, 64, 0, 0, 0,  0, 0, 0, 20, 64};
  EXPECT_EQ(bytes, expected);
}

TEST(MinColumn, FromBytes) {
  auto bytes = std::vector<uint8_t>{
      15, 0,  0, 0,   0,  0, 0, 0, 45, 0,  0, 0, 0,  0, 0, 0, 0,  0, 0,
      0,  0,  0, 240, 63, 0, 0, 0, 0,  0,  0, 0, 64, 0, 0, 0, 0,  0, 0,
      8,  64, 0, 0,   0,  0, 0, 0, 16, 64, 0, 0, 0,  0, 0, 0, 20, 64};

  auto read_column = std::static_pointer_cast<tskv::IReadColumn>(
      tskv::FromBytes(bytes, tskv::ColumnType::kMin));
  auto min_column = std::static_pointer_cast<tskv::MinColumn>(read_column);
  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(min_column->GetValues(), expected);
  EXPECT_EQ(min_column->GetTimeRange(), tskv::TimeRange(45, 120));
}

TEST(MinColumn, ScaleBuckets) {
  {
    tskv::MinColumn column(std::vector<double>{1, 2, 3, 4, 5},
                           tskv::TimePoint(1), 1);
    column.ScaleBuckets(2);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kMin);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 6));
    auto expected = std::vector<double>{1, 2, 4};
    EXPECT_EQ(column.GetValues(), expected);
  }
  {
    tskv::MinColumn column(std::vector<double>{1, 4, 2, 3, 9, 15, 0, 1, 8, 5},
                           tskv::TimePoint(2), 2);
    column.ScaleBuckets(2);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kMin);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(2, 22));
    auto expected = std::vector<double>{1, 4, 2, 3, 9, 15, 0, 1, 8, 5};
    EXPECT_EQ(column.GetValues(), expected);
  }
  {
    tskv::MinColumn column(std::vector<double>{1, 4, 2, 3, 9, 15, 0, 1, 8, 5},
                           tskv::TimePoint(2), 2);
    column.ScaleBuckets(6);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kMin);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 24));
    auto expected = std::vector<double>{1, 2, 0, 5};
    EXPECT_EQ(column.GetValues(), expected);
  }
  {
    tskv::MinColumn column(std::vector<double>{1, 4, 2, 3, 9, 15, 0, 1, 8},
                           tskv::TimePoint(0), 2);
    column.ScaleBuckets(4);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kMin);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 20));
    auto expected = std::vector<double>{1, 2, 9, 0, 8};
    EXPECT_EQ(column.GetValues(), expected);
  }
  {
    auto neutral = std::numeric_limits<double>::max();
    tskv::MinColumn column(std::vector<double>(3, neutral), tskv::TimePoint(0),
                           1);
    column.ScaleBuckets(2);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kMin);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 4));
    auto expected = std::vector<double>(2, neutral);
    EXPECT_EQ(column.GetValues(), expected);
  }
}

TEST(MaxColumn, Basic) {
  tskv::MaxColumn column(std::vector<double>{1, 2, 3, 4, 5}, tskv::TimePoint(1),
                         1);
  EXPECT_EQ(column.GetType(), tskv::ColumnType::kMax);
  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(column.GetValues(), expected);
  EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 6));
}

TEST(MaxColumn, Write) {
  {
    tskv::MaxColumn column(1);
    column.Write({{1, 1}, {2, 2}, {2, 1}, {3, 1}, {3, 10}, {4, 2}, {4, -1}});
    auto expected = std::vector<double>{1, 2, 10, 2};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 5));

    column.Write({{4, 3}, {5, 11}, {6, 8}, {6, 7}});
    expected = std::vector<double>{1, 2, 10, 3, 11, 8};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 7));

    column.Write({{7, 1}, {7, 2}, {7, 3}, {7, 4}});
    expected = std::vector<double>{1, 2, 10, 3, 11, 8, 4};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 8));
  }
  {
    tskv::MaxColumn column(2);
    column.Write({{1, 1}, {2, 2}, {2, 1}, {3, 1}, {3, 10}, {4, 2}, {4, -1}});
    auto expected = std::vector<double>{1, 10, 2};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 6));

    column.Write({{4, 3}, {5, 11}, {6, 8}, {6, 7}});
    expected = std::vector<double>{1, 10, 11, 8};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 8));

    column.Write({{7, 1}, {7, 2}, {7, 3}, {7, 4}});
    expected = std::vector<double>{1, 10, 11, 8};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 8));
  }
}

TEST(MaxColumn, Read) {
  {
    tskv::MaxColumn column(std::vector<double>{1, 2, 3, 4, 5},
                           tskv::TimePoint(1), 1);
    auto expected = std::vector<double>{1, 2, 3, 4, 5};
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 6))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 6))->GetTimeRange(),
              tskv::TimeRange(1, 6));
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 6))->GetType(),
              tskv::ColumnType::kMax);

    expected = std::vector<double>{1, 2, 3, 4};
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 5))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 5))->GetTimeRange(),
              tskv::TimeRange(1, 5));
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 5))->GetType(),
              tskv::ColumnType::kMax);

    expected = std::vector<double>{2, 3, 4, 5};
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 6))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 6))->GetTimeRange(),
              tskv::TimeRange(2, 6));
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 6))->GetType(),
              tskv::ColumnType::kMax);

    expected = std::vector<double>{3};
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 4))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 4))->GetTimeRange(),
              tskv::TimeRange(3, 4));
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 4))->GetType(),
              tskv::ColumnType::kMax);
  }
  {
    tskv::MaxColumn column(std::vector<double>{1, 2, 3, 4, 5},
                           tskv::TimePoint(2), 2);
    auto expected = std::vector<double>{1, 2, 3, 4, 5};
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 12))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 12))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 12))->GetType(),
              tskv::ColumnType::kMax);

    EXPECT_EQ(column.Read(tskv::TimeRange(3, 12))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 12))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 12))->GetType(),
              tskv::ColumnType::kMax);

    EXPECT_EQ(column.Read(tskv::TimeRange(1, 100))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 100))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 100))->GetType(),
              tskv::ColumnType::kMax);

    EXPECT_EQ(column.Read(tskv::TimeRange(2, 11))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 11))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 11))->GetType(),
              tskv::ColumnType::kMax);
  }
}

TEST(MaxColumn, Merge) {
  {
    tskv::MaxColumn column1(std::vector<double>{1, 2, 3, 4, 5},
                            tskv::TimePoint(1), 1);
    tskv::MaxColumn column2(std::vector<double>{5, 4, 3}, tskv::TimePoint(3),
                            1);
    std::shared_ptr<tskv::IReadColumn> column2_read =
        std::make_shared<tskv::MaxColumn>(column2);
    column1.Merge(column2_read);
    auto expected = std::vector<double>{1, 2, 5, 4, 5};
    EXPECT_EQ(column1.GetValues(), expected);
    EXPECT_EQ(column1.GetTimeRange(), tskv::TimeRange(1, 6));
  }
  {
    tskv::MaxColumn column1(std::vector<double>{1, 2, 3}, tskv::TimePoint(3),
                            3);
    tskv::MaxColumn column2(std::vector<double>{10, 20}, tskv::TimePoint(9), 3);
    std::shared_ptr<tskv::IReadColumn> column2_read =
        std::make_shared<tskv::MaxColumn>(column2);
    column1.Merge(column2_read);
    auto expected = std::vector<double>{1, 2, 10, 20};
    EXPECT_EQ(column1.GetValues(), expected);
    EXPECT_EQ(column1.GetTimeRange(), tskv::TimeRange(3, 15));
  }
}

TEST(MaxColumn, Extract) {
  tskv::MaxColumn column(std::vector<double>{1, 2, 3, 4, 5}, tskv::TimePoint(5),
                         5);
  auto result = std::static_pointer_cast<tskv::IReadColumn>(column.Extract());
  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(result->GetValues(), expected);
  EXPECT_EQ(result->GetTimeRange(), tskv::TimeRange(5, 30));
  EXPECT_EQ(result->GetType(), tskv::ColumnType::kMax);
  EXPECT_TRUE(column.GetValues().empty());
  EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 0));
}

TEST(MaxColumn, ToBytes) {
  tskv::MaxColumn column(std::vector<double>{1, 2, 3, 4, 5},
                         tskv::TimePoint(45), 15);
  auto bytes = column.ToBytes();
  auto expected = std::vector<uint8_t>{
      15, 0,  0, 0,   0,  0, 0, 0, 45, 0,  0, 0, 0,  0, 0, 0, 0,  0, 0,
      0,  0,  0, 240, 63, 0, 0, 0, 0,  0,  0, 0, 64, 0, 0, 0, 0,  0, 0,
      8,  64, 0, 0,   0,  0, 0, 0, 16, 64, 0, 0, 0,  0, 0, 0, 20, 64};
  EXPECT_EQ(bytes, expected);
}

TEST(MaxColumn, FromBytes) {
  auto bytes = std::vector<uint8_t>{
      15, 0,  0, 0,   0,  0, 0, 0, 45, 0,  0, 0, 0,  0, 0, 0, 0,  0, 0,
      0,  0,  0, 240, 63, 0, 0, 0, 0,  0,  0, 0, 64, 0, 0, 0, 0,  0, 0,
      8,  64, 0, 0,   0,  0, 0, 0, 16, 64, 0, 0, 0,  0, 0, 0, 20, 64};
  auto read_column = std::static_pointer_cast<tskv::IReadColumn>(
      tskv::FromBytes(bytes, tskv::ColumnType::kMax));
  auto max_column = std::static_pointer_cast<tskv::MaxColumn>(read_column);
  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(max_column->GetValues(), expected);
  EXPECT_EQ(max_column->GetTimeRange(), tskv::TimeRange(45, 120));
  EXPECT_EQ(max_column->GetType(), tskv::ColumnType::kMax);
}

TEST(MaxColumn, ScaleBuckets) {
  {
    tskv::MaxColumn column(std::vector<double>{1, 2, 3, 4, 5},
                           tskv::TimePoint(1), 1);
    column.ScaleBuckets(2);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kMax);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 6));
    auto expected = std::vector<double>{1, 3, 5};
    EXPECT_EQ(column.GetValues(), expected);
  }
  {
    tskv::MaxColumn column(std::vector<double>{1, 4, 2, 3, 9, 15, 0, 1, 8, 5},
                           tskv::TimePoint(2), 2);
    column.ScaleBuckets(2);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kMax);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(2, 22));
    auto expected = std::vector<double>{1, 4, 2, 3, 9, 15, 0, 1, 8, 5};
    EXPECT_EQ(column.GetValues(), expected);
  }
  {
    tskv::MaxColumn column(std::vector<double>{1, 4, 2, 3, 9, 15, 0, 1, 8, 5},
                           tskv::TimePoint(2), 2);
    column.ScaleBuckets(6);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kMax);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 24));
    auto expected = std::vector<double>{4, 9, 15, 8};
    EXPECT_EQ(column.GetValues(), expected);
  }
  {
    tskv::MaxColumn column(std::vector<double>{1, 4, 2, 3, 9, 15, 0, 1, 8},
                           tskv::TimePoint(0), 2);
    column.ScaleBuckets(4);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kMax);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 20));
    auto expected = std::vector<double>{4, 3, 15, 1, 8};
    EXPECT_EQ(column.GetValues(), expected);
  }
  {
    auto neutral = std::numeric_limits<double>::lowest();
    tskv::MaxColumn column(std::vector<double>(3, neutral), tskv::TimePoint(0),
                           1);
    column.ScaleBuckets(2);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kMax);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 4));
    auto expected = std::vector<double>(2, neutral);
    EXPECT_EQ(column.GetValues(), expected);
  }
}

TEST(LastColumn, Basic) {
  tskv::LastColumn column(std::vector<double>{1, 2, 3, 4, 5},
                          tskv::TimePoint(1), 1);
  EXPECT_EQ(column.GetType(), tskv::ColumnType::kLast);
  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(column.GetValues(), expected);
  EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 6));
}

TEST(LastColumn, Write) {
  {
    tskv::LastColumn column(1);
    column.Write({{1, 1}, {2, 2}, {2, 1}, {3, 1}, {3, 10}, {4, 2}, {4, -1}});
    auto expected = std::vector<double>{1, 1, 10, -1};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 5));

    column.Write({{4, 3}, {5, 11}, {6, 8}, {6, 7}});
    expected = std::vector<double>{1, 1, 10, 3, 11, 7};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 7));

    column.Write({{7, 1}, {7, 2}, {7, 3}, {7, 4}});
    expected = std::vector<double>{1, 1, 10, 3, 11, 7, 4};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 8));
  }
  {
    tskv::LastColumn column(2);
    column.Write({{1, 1}, {2, 2}, {2, 1}, {3, 1}, {3, 10}, {4, 2}, {4, -1}});
    auto expected = std::vector<double>{1, 10, -1};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 6));

    column.Write({{4, 3}, {5, 11}, {6, 8}, {6, 7}});
    expected = std::vector<double>{1, 10, 11, 7};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 8));

    column.Write({{7, 1}, {7, 2}, {7, 3}, {7, 4}});
    expected = std::vector<double>{1, 10, 11, 4};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 8));
  }
}

TEST(LastColumn, Read) {
  {
    tskv::LastColumn column(std::vector<double>{1, 2, 3, 4, 5},
                            tskv::TimePoint(1), 1);
    auto expected = std::vector<double>{1, 2, 3, 4, 5};
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 6))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 6))->GetTimeRange(),
              tskv::TimeRange(1, 6));
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 6))->GetType(),
              tskv::ColumnType::kLast);

    expected = std::vector<double>{1, 2, 3, 4};
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 5))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 5))->GetTimeRange(),
              tskv::TimeRange(1, 5));
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 5))->GetType(),
              tskv::ColumnType::kLast);

    expected = std::vector<double>{2, 3, 4, 5};
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 6))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 6))->GetTimeRange(),
              tskv::TimeRange(2, 6));
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 6))->GetType(),
              tskv::ColumnType::kLast);

    expected = std::vector<double>{3};
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 4))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 4))->GetTimeRange(),
              tskv::TimeRange(3, 4));
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 4))->GetType(),
              tskv::ColumnType::kLast);
  }
  {
    tskv::LastColumn column(std::vector<double>{1, 2, 3, 4, 5},
                            tskv::TimePoint(2), 2);
    auto expected = std::vector<double>{1, 2, 3, 4, 5};
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 12))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 12))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 12))->GetType(),
              tskv::ColumnType::kLast);

    EXPECT_EQ(column.Read(tskv::TimeRange(3, 12))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 12))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 12))->GetType(),
              tskv::ColumnType::kLast);

    EXPECT_EQ(column.Read(tskv::TimeRange(1, 100))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 100))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 100))->GetType(),
              tskv::ColumnType::kLast);

    EXPECT_EQ(column.Read(tskv::TimeRange(2, 11))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 11))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 11))->GetType(),
              tskv::ColumnType::kLast);
  }
}

TEST(LastColumn, Merge) {
  {
    tskv::LastColumn column1(std::vector<double>{1, 2, 3, 4, 5},
                             tskv::TimePoint(1), 1);
    tskv::LastColumn column2(std::vector<double>{5, 4, 3}, tskv::TimePoint(3),
                             1);
    std::shared_ptr<tskv::IReadColumn> column2_read =
        std::make_shared<tskv::LastColumn>(column2);
    column1.Merge(column2_read);
    auto expected = std::vector<double>{1, 2, 5, 4, 3};
    EXPECT_EQ(column1.GetValues(), expected);
    EXPECT_EQ(column1.GetTimeRange(), tskv::TimeRange(1, 6));
  }
  {
    tskv::LastColumn column1(std::vector<double>{1, 2, 3}, tskv::TimePoint(3),
                             3);
    tskv::LastColumn column2(std::vector<double>{10, 20}, tskv::TimePoint(9),
                             3);
    std::shared_ptr<tskv::IReadColumn> column2_read =
        std::make_shared<tskv::LastColumn>(column2);
    column1.Merge(column2_read);
    auto expected = std::vector<double>{1, 2, 10, 20};
    EXPECT_EQ(column1.GetValues(), expected);
    EXPECT_EQ(column1.GetTimeRange(), tskv::TimeRange(3, 15));
  }
}

TEST(LastColumn, Extract) {
  tskv::LastColumn column(std::vector<double>{1, 2, 3, 4, 5},
                          tskv::TimePoint(5), 5);
  auto result = std::static_pointer_cast<tskv::IReadColumn>(column.Extract());
  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(result->GetValues(), expected);
  EXPECT_EQ(result->GetTimeRange(), tskv::TimeRange(5, 30));
  EXPECT_EQ(result->GetType(), tskv::ColumnType::kLast);
  EXPECT_TRUE(column.GetValues().empty());
  EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 0));
}

TEST(LastColumn, ToBytes) {
  tskv::LastColumn column(std::vector<double>{1, 2, 3, 4, 5},
                          tskv::TimePoint(45), 15);
  auto bytes = column.ToBytes();
  auto expected = std::vector<uint8_t>{
      15, 0,  0, 0,   0,  0, 0, 0, 45, 0,  0, 0, 0,  0, 0, 0, 0,  0, 0,
      0,  0,  0, 240, 63, 0, 0, 0, 0,  0,  0, 0, 64, 0, 0, 0, 0,  0, 0,
      8,  64, 0, 0,   0,  0, 0, 0, 16, 64, 0, 0, 0,  0, 0, 0, 20, 64};
  EXPECT_EQ(bytes, expected);
}

TEST(LastColumn, FromBytes) {
  auto bytes = std::vector<uint8_t>{
      15, 0,  0, 0,   0,  0, 0, 0, 45, 0,  0, 0, 0,  0, 0, 0, 0,  0, 0,
      0,  0,  0, 240, 63, 0, 0, 0, 0,  0,  0, 0, 64, 0, 0, 0, 0,  0, 0,
      8,  64, 0, 0,   0,  0, 0, 0, 16, 64, 0, 0, 0,  0, 0, 0, 20, 64};
  auto read_column = std::static_pointer_cast<tskv::IReadColumn>(
      tskv::FromBytes(bytes, tskv::ColumnType::kLast));
  auto last_column = std::static_pointer_cast<tskv::LastColumn>(read_column);
  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(last_column->GetValues(), expected);
  EXPECT_EQ(last_column->GetTimeRange(), tskv::TimeRange(45, 120));
  EXPECT_EQ(last_column->GetType(), tskv::ColumnType::kLast);
}

TEST(LastColumn, ScaleBuckets) {
  {
    tskv::LastColumn column(std::vector<double>{1, 2, 3, 4, 5},
                            tskv::TimePoint(1), 1);
    column.ScaleBuckets(2);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kLast);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 6));
    auto expected = std::vector<double>{1, 3, 5};
    EXPECT_EQ(column.GetValues(), expected);
  }
  {
    tskv::LastColumn column(std::vector<double>{1, 4, 2, 3, 9, 15, 0, 1, 8, 5},
                            tskv::TimePoint(2), 2);
    column.ScaleBuckets(2);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kLast);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(2, 22));
    auto expected = std::vector<double>{1, 4, 2, 3, 9, 15, 0, 1, 8, 5};
    EXPECT_EQ(column.GetValues(), expected);
  }
  {
    tskv::LastColumn column(std::vector<double>{1, 4, 2, 3, 9, 15, 0, 1, 8, 5},
                            tskv::TimePoint(2), 2);
    column.ScaleBuckets(6);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kLast);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 24));
    auto expected = std::vector<double>{4, 9, 1, 5};
    EXPECT_EQ(column.GetValues(), expected);
  }
  {
    tskv::LastColumn column(std::vector<double>{1, 4, 2, 3, 9, 15, 0, 1, 8},
                            tskv::TimePoint(0), 2);
    column.ScaleBuckets(4);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kLast);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 20));
    auto expected = std::vector<double>{4, 3, 15, 1, 8};
    EXPECT_EQ(column.GetValues(), expected);
  }
  {
    auto neutral = 0;
    tskv::LastColumn column(std::vector<double>(3, neutral), tskv::TimePoint(0),
                            1);
    column.ScaleBuckets(2);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kLast);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(0, 4));
    auto expected = std::vector<double>(2, neutral);
    EXPECT_EQ(column.GetValues(), expected);
  }
}

TEST(RawTimestamps, Basic) {
  tskv::RawTimestampsColumn column(std::vector<uint64_t>{1, 2, 3, 4, 5});
  EXPECT_EQ(column.GetType(), tskv::ColumnType::kRawTimestamps);
  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(column.GetValues(), expected);
}

TEST(RawTimestamps, Write) {
  tskv::RawTimestampsColumn column;
  column.Write({{1, 1}, {2, 2}, {2, 1}, {3, 1}, {3, 10}, {4, 2}, {4, -1}});
  auto expected = std::vector<double>{1, 2, 2, 3, 3, 4, 4};
  EXPECT_EQ(column.GetValues(), expected);
  EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 5));

  column.Write({{4, 3}, {5, 11}, {6, 8}, {6, 7}});
  expected = std::vector<double>{1, 2, 2, 3, 3, 4, 4, 4, 5, 6, 6};
  EXPECT_EQ(column.GetValues(), expected);
  EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 7));
}

TEST(RawTimestamps, Merge) {
  tskv::RawTimestampsColumn column1(std::vector<uint64_t>{1, 2, 3, 4, 5});
  tskv::RawTimestampsColumn column2(std::vector<uint64_t>{5, 5, 6, 8, 14});
  column1.Merge(std::make_shared<tskv::RawTimestampsColumn>(column2));
  auto expected = std::vector<double>{1, 2, 3, 4, 5, 5, 5, 6, 8, 14};
  EXPECT_EQ(column1.GetValues(), expected);
  EXPECT_EQ(column1.GetTimeRange(), tskv::TimeRange(1, 15));
}

TEST(RawTimestamps, Extract) {
  tskv::RawTimestampsColumn column(std::vector<uint64_t>{1, 2, 3, 4, 5});
  auto result = column.Extract();
  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(result->GetValues(), expected);
  EXPECT_EQ(result->GetType(), tskv::ColumnType::kRawTimestamps);
  EXPECT_TRUE(column.GetValues().empty());
  EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange());
}

TEST(RawTimestamps, ToBytes) {
  tskv::RawTimestampsColumn column(std::vector<uint64_t>{1, 2, 3, 4, 5});
  auto bytes = column.ToBytes();
  auto expected = std::vector<uint8_t>{1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0,
                                       0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0,
                                       0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0};
  EXPECT_EQ(bytes, expected);
}

TEST(RawTimestamps, FromBytes) {
  auto bytes = std::vector<uint8_t>{1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0,
                                    0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0,
                                    0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0};
  auto column = tskv::FromBytes(bytes, tskv::ColumnType::kRawTimestamps);
  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(column->GetValues(), expected);
  EXPECT_EQ(column->GetType(), tskv::ColumnType::kRawTimestamps);
  auto raw_column = std::static_pointer_cast<tskv::RawTimestampsColumn>(column);
  EXPECT_EQ(raw_column->GetTimeRange(), tskv::TimeRange(1, 6));
}

TEST(RawTimestamps, GetTimeRange) {
  tskv::RawTimestampsColumn column(std::vector<uint64_t>{1, 2, 4, 6, 8, 9});
  EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 10));
}

TEST(RawValues, Basic) {
  tskv::RawValuesColumn column(std::vector<double>{1, 2, 3, 4, 5});
  EXPECT_EQ(column.GetType(), tskv::ColumnType::kRawValues);
  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(column.GetValues(), expected);
}

TEST(RawValues, Write) {
  tskv::RawValuesColumn column;
  column.Write({{1, 1}, {2, 2}, {2, 1}, {3, 1}, {3, 10}, {4, 2}, {4, -1}});
  auto expected = std::vector<double>{1, 2, 1, 1, 10, 2, -1};
  EXPECT_EQ(column.GetValues(), expected);

  column.Write({{4, 3}, {5, 11}, {6, 8}, {6, 7}});
  expected = std::vector<double>{1, 2, 1, 1, 10, 2, -1, 3, 11, 8, 7};
  EXPECT_EQ(column.GetValues(), expected);
}

TEST(RawValues, Merge) {
  tskv::RawValuesColumn column1(std::vector<double>{1, 2, 3, 5});
  tskv::RawValuesColumn column2(std::vector<double>{5, 4, 11, 1});
  column1.Merge(std::make_shared<tskv::RawValuesColumn>(column2));
  auto expected = std::vector<double>{1, 2, 3, 5, 5, 4, 11, 1};
  EXPECT_EQ(column1.GetValues(), expected);
}

TEST(RawValues, Extract) {
  tskv::RawValuesColumn column(std::vector<double>{1, 2, 3, 4, 5});
  auto result = column.Extract();
  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(result->GetValues(), expected);
  EXPECT_EQ(result->GetType(), tskv::ColumnType::kRawValues);
  EXPECT_TRUE(column.GetValues().empty());
}

TEST(RawValues, ToBytes) {
  tskv::RawValuesColumn column(std::vector<double>{1, 2, 3, 4, 5});
  auto bytes = column.ToBytes();
  auto expected = std::vector<uint8_t>{
      0, 0, 0, 0,  0, 0, 240, 63, 0, 0, 0,  0,  0, 0, 0, 64, 0, 0, 0,  0,
      0, 0, 8, 64, 0, 0, 0,   0,  0, 0, 16, 64, 0, 0, 0, 0,  0, 0, 20, 64};
  EXPECT_EQ(bytes, expected);
}

TEST(RawValues, FromBytes) {
  auto bytes = std::vector<uint8_t>{
      0, 0, 0, 0,  0, 0, 240, 63, 0, 0, 0,  0,  0, 0, 0, 64, 0, 0, 0,  0,
      0, 0, 8, 64, 0, 0, 0,   0,  0, 0, 16, 64, 0, 0, 0, 0,  0, 0, 20, 64};
  auto column = tskv::FromBytes(bytes, tskv::ColumnType::kRawValues);
  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(column->GetValues(), expected);
  EXPECT_EQ(column->GetType(), tskv::ColumnType::kRawValues);
}

TEST(ReadRawColumn, Basic) {
  std::shared_ptr<tskv::RawTimestampsColumn> timestamps =
      std::make_shared<tskv::RawTimestampsColumn>(
          std::vector<uint64_t>{1, 2, 3, 4});
  std::shared_ptr<tskv::RawValuesColumn> values =
      std::make_shared<tskv::RawValuesColumn>(
          std::vector<double>{1, -4, 15, 2});
  tskv::ReadRawColumn column(timestamps, values);
  EXPECT_EQ(column.GetType(), tskv::ColumnType::kRawRead);
  auto expected_ts = std::vector<uint64_t>{1, 2, 3, 4};
  EXPECT_EQ(column.GetTimestamps(), expected_ts);
  auto expected_vals = std::vector<double>{1, -4, 15, 2};
  EXPECT_EQ(column.GetValues(), expected_vals);
  EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 5));
}

TEST(ReadRawColumn, Write) {
  tskv::ReadRawColumn column;
  column.Write({{1, 1}, {2, 2}, {2, 1}, {3, 1}, {3, 10}, {4, 2}, {4, -1}});
  auto expected_ts = std::vector<uint64_t>{1, 2, 2, 3, 3, 4, 4};
  EXPECT_EQ(column.GetTimestamps(), expected_ts);
  auto expected_vals = std::vector<double>{1, 2, 1, 1, 10, 2, -1};
  EXPECT_EQ(column.GetValues(), expected_vals);
  EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 5));

  column.Write({{4, 3}, {5, 11}, {6, 8}, {6, 7}});
  expected_ts = std::vector<uint64_t>{1, 2, 2, 3, 3, 4, 4, 4, 5, 6, 6};
  EXPECT_EQ(column.GetTimestamps(), expected_ts);
  expected_vals = std::vector<double>{1, 2, 1, 1, 10, 2, -1, 3, 11, 8, 7};
  EXPECT_EQ(column.GetValues(), expected_vals);
  EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 7));
}

TEST(ReadRawColumn, Read) {
  std::shared_ptr<tskv::RawTimestampsColumn> timestamps =
      std::make_shared<tskv::RawTimestampsColumn>(
          std::vector<uint64_t>{1, 2, 2, 3, 3, 4, 4, 4, 5, 6, 6});
  std::shared_ptr<tskv::RawValuesColumn> values =
      std::make_shared<tskv::RawValuesColumn>(
          std::vector<double>{1, 2, 1, 1, 10, 2, -1, 3, 11, 8, 7});
  tskv::ReadRawColumn column(timestamps, values);

  auto expected_ts = std::vector<uint64_t>{1, 2, 2, 3, 3, 4, 4, 4, 5, 6, 6};
  auto expected_vals = std::vector<double>{1, 2, 1, 1, 10, 2, -1, 3, 11, 8, 7};
  auto read_raw_column = std::static_pointer_cast<tskv::ReadRawColumn>(
      column.Read(tskv::TimeRange(1, 7)));
  EXPECT_EQ(read_raw_column->GetTimestamps(), expected_ts);
  EXPECT_EQ(read_raw_column->GetValues(), expected_vals);

  expected_ts = std::vector<uint64_t>{1, 2, 2, 3, 3, 4, 4, 4, 5};
  expected_vals = std::vector<double>{1, 2, 1, 1, 10, 2, -1, 3, 11};
  read_raw_column = std::static_pointer_cast<tskv::ReadRawColumn>(
      column.Read(tskv::TimeRange(1, 6)));
  EXPECT_EQ(read_raw_column->GetTimestamps(), expected_ts);
  EXPECT_EQ(read_raw_column->GetValues(), expected_vals);

  expected_ts = std::vector<uint64_t>{2, 2, 3, 3, 4, 4, 4, 5, 6, 6};
  expected_vals = std::vector<double>{2, 1, 1, 10, 2, -1, 3, 11, 8, 7};
  read_raw_column = std::static_pointer_cast<tskv::ReadRawColumn>(
      column.Read(tskv::TimeRange(2, 7)));
  EXPECT_EQ(read_raw_column->GetTimestamps(), expected_ts);
  EXPECT_EQ(read_raw_column->GetValues(), expected_vals);

  expected_ts = std::vector<uint64_t>{3, 3, 4, 4, 4};
  expected_vals = std::vector<double>{1, 10, 2, -1, 3};
  read_raw_column = std::static_pointer_cast<tskv::ReadRawColumn>(
      column.Read(tskv::TimeRange(3, 5)));
  EXPECT_EQ(read_raw_column->GetTimestamps(), expected_ts);
  EXPECT_EQ(read_raw_column->GetValues(), expected_vals);
}

TEST(ReadRawColumn, Merge) {
  std::shared_ptr<tskv::RawTimestampsColumn> timestamps1 =
      std::make_shared<tskv::RawTimestampsColumn>(
          std::vector<uint64_t>{1, 2, 2, 3, 3, 4, 4, 4, 5, 6, 6});
  std::shared_ptr<tskv::RawValuesColumn> values1 =
      std::make_shared<tskv::RawValuesColumn>(
          std::vector<double>{1, 2, 1, 1, 10, 2, -1, 3, 11, 8, 7});
  tskv::ReadRawColumn column1(timestamps1, values1);

  std::shared_ptr<tskv::RawTimestampsColumn> timestamps2 =
      std::make_shared<tskv::RawTimestampsColumn>(
          std::vector<uint64_t>{6, 6, 7, 8, 12, 13});
  std::shared_ptr<tskv::RawValuesColumn> values2 =
      std::make_shared<tskv::RawValuesColumn>(
          std::vector<double>{8, 7, 1, 2, 3, 4});
  tskv::ReadRawColumn column2(timestamps2, values2);

  column1.Merge(std::make_shared<tskv::ReadRawColumn>(column2));
  auto expected_ts = std::vector<uint64_t>{1, 2, 2, 3, 3, 4, 4,  4, 5,
                                           6, 6, 6, 6, 7, 8, 12, 13};
  auto expected_vals =
      std::vector<double>{1, 2, 1, 1, 10, 2, -1, 3, 11, 8, 7, 8, 7, 1, 2, 3, 4};
  EXPECT_EQ(column1.GetTimestamps(), expected_ts);
  EXPECT_EQ(column1.GetValues(), expected_vals);
}

TEST(ReadRawColumn, Extract) {
  std::shared_ptr<tskv::RawTimestampsColumn> timestamps =
      std::make_shared<tskv::RawTimestampsColumn>(
          std::vector<uint64_t>{1, 2, 2, 3, 3, 4, 4, 4, 5, 6, 6});
  std::shared_ptr<tskv::RawValuesColumn> values =
      std::make_shared<tskv::RawValuesColumn>(
          std::vector<double>{1, 2, 1, 1, 10, 2, -1, 3, 11, 8, 7});
  tskv::ReadRawColumn column(timestamps, values);

  auto result = std::static_pointer_cast<tskv::ReadRawColumn>(column.Extract());
  EXPECT_EQ(result->GetType(), tskv::ColumnType::kRawRead);
  auto expected_ts = std::vector<uint64_t>{1, 2, 2, 3, 3, 4, 4, 4, 5, 6, 6};
  EXPECT_EQ(result->GetTimestamps(), expected_ts);
  auto expected_vals = std::vector<double>{1, 2, 1, 1, 10, 2, -1, 3, 11, 8, 7};
  EXPECT_EQ(result->GetValues(), expected_vals);
  EXPECT_TRUE(column.GetTimestamps().empty());
  EXPECT_TRUE(column.GetValues().empty());
}

TEST(AvgColumn, Basic) {
  {
    tskv::AvgColumn column(std::vector<double>{1, 2, 3, 4, 5},
                           tskv::TimePoint(1), 1);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kAvg);
    auto expected = std::vector<double>{1, 2, 3, 4, 5};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 6));
  }
  {
    auto sum_column = std::make_shared<tskv::SumColumn>(
        std::vector<double>{1, 2, 3, 4, 5}, tskv::TimePoint(1), 1);
    auto count_column = std::make_shared<tskv::CountColumn>(
        std::vector<double>{2, 2, 1, 2, 1}, tskv::TimePoint(1), 1);
    tskv::AvgColumn column(sum_column, count_column);
    EXPECT_EQ(column.GetType(), tskv::ColumnType::kAvg);
    auto expected = std::vector<double>{0.5, 1, 3, 2, 5};
    EXPECT_EQ(column.GetValues(), expected);
    EXPECT_EQ(column.GetTimeRange(), tskv::TimeRange(1, 6));
  }
}

TEST(AvgColumn, Read) {
  {
    tskv::AvgColumn column(std::vector<double>{1, 2, 3, 4, 5},
                           tskv::TimePoint(1), 1);
    auto expected = std::vector<double>{1, 2, 3, 4, 5};
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 6))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 6))->GetTimeRange(),
              tskv::TimeRange(1, 6));
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 6))->GetType(),
              tskv::ColumnType::kAvg);

    expected = std::vector<double>{1, 2, 3, 4};
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 5))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 5))->GetTimeRange(),
              tskv::TimeRange(1, 5));
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 5))->GetType(),
              tskv::ColumnType::kAvg);

    expected = std::vector<double>{2, 3, 4, 5};
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 6))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 6))->GetTimeRange(),
              tskv::TimeRange(2, 6));
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 6))->GetType(),
              tskv::ColumnType::kAvg);

    expected = std::vector<double>{3};
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 4))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 4))->GetTimeRange(),
              tskv::TimeRange(3, 4));
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 4))->GetType(),
              tskv::ColumnType::kAvg);
  }
  {
    tskv::AvgColumn column(std::vector<double>{1, 2, 3, 4, 5},
                           tskv::TimePoint(2), 2);
    auto expected = std::vector<double>{1, 2, 3, 4, 5};
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 12))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 12))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 12))->GetType(),
              tskv::ColumnType::kAvg);

    EXPECT_EQ(column.Read(tskv::TimeRange(3, 12))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 12))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(3, 12))->GetType(),
              tskv::ColumnType::kAvg);

    EXPECT_EQ(column.Read(tskv::TimeRange(1, 100))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 100))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(1, 100))->GetType(),
              tskv::ColumnType::kAvg);

    EXPECT_EQ(column.Read(tskv::TimeRange(2, 11))->GetValues(), expected);
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 11))->GetTimeRange(),
              tskv::TimeRange(2, 12));
    EXPECT_EQ(column.Read(tskv::TimeRange(2, 11))->GetType(),
              tskv::ColumnType::kAvg);
  }
}
