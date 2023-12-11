#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>

#include "gmock/gmock.h"
#include "level/level.h"
#include "model/column.h"
#include "model/model.h"
#include "persistent-storage/persistent_storage.h"

class MockPersistentStorage : public tskv::IPersistentStorage {
 public:
  MockPersistentStorage() = default;

  MOCK_METHOD(tskv::IPersistentStorage::Metadata, GetMetadata, (),
              (const, override));
  MOCK_METHOD(tskv::PageId, CreatePage, (), (override));
  MOCK_METHOD(tskv::CompressedBytes, Read, (const tskv::PageId& page_id),
              (override));
  MOCK_METHOD(void, Write,
              (const tskv::PageId& page_id, const tskv::CompressedBytes& bytes),
              (override));
  MOCK_METHOD(void, DeletePage, (const tskv::PageId& page_id), (override));
};

TEST(Level, ReadWrite) {
  auto mock_storage = std::make_shared<MockPersistentStorage>();
  tskv::Level level(
      tskv::Level::Options{
          .bucket_interval = tskv::Duration::Seconds(40),
          .level_duration = tskv::Duration::Hours(20),
          .store_raw = true,
      },
      mock_storage);

  EXPECT_CALL(*mock_storage, CreatePage).Times(1);
  EXPECT_CALL(*mock_storage, Write).Times(1);
  auto column = std::make_shared<tskv::SumColumn>(
      std::vector<double>{1, 2, 3, 4, 5}, tskv::TimePoint(45), 15);
  level.Write(column);

  EXPECT_CALL(*mock_storage, Read)
      .Times(1)
      .WillOnce(testing::Return(std::vector<uint8_t>{
          15, 0,  0, 0,   0,  0, 0, 0, 45, 0,  0, 0, 0,  0, 0, 0, 0,  0, 0,
          0,  0,  0, 240, 63, 0, 0, 0, 0,  0,  0, 0, 64, 0, 0, 0, 0,  0, 0,
          8,  64, 0, 0,   0,  0, 0, 0, 16, 64, 0, 0, 0,  0, 0, 0, 20, 64}));
  auto read_column = std::static_pointer_cast<tskv::IReadColumn>(
      level.Read(tskv::TimeRange{0, 200}, tskv::StoredAggregationType::kSum));

  auto expected = std::vector<double>{1, 2, 3, 4, 5};
  EXPECT_EQ(read_column->GetValues(), expected);
  EXPECT_EQ(read_column->GetTimeRange(), tskv::TimeRange(45, 120));
}

// TODO: add MovePagesFrom test
