#pragma once

#include "persistent_storage.h"

namespace tskv {

class DiskStorage : public IPersistentStorage {
 public:
  struct Options {
    std::string path;
    bool create_if_not_exists = false;
  };

 public:
  explicit DiskStorage(const Options& options);
  Metadata GetMetadata() const override;
  PageId CreatePage() override;
  CompressedBytes Read(const PageId& page_id) override;
  void Write(const PageId& page_id, const CompressedBytes& bytes) override;
  void DeletePage(const PageId& page_id) override;

 private:
  std::string path_;
  PageId next_page_id_{0};
};

}  // namespace tskv
