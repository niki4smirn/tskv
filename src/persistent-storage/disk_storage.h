#pragma once

#include <filesystem>

#include "persistent_storage.h"

namespace tskv {

class DiskStorage : public IPersistentStorage {
 public:
  struct Options {
    std::string path;
  };

 public:
  explicit DiskStorage(const Options& options);
  Metadata GetMetadata() const override;
  PageId CreatePage() override;
  CompressedBytes Read(const PageId& page_id) override;
  void Write(const PageId& page_id, const CompressedBytes& bytes) override;
  void DeletePage(const PageId& page_id) override;
  static std::string GeneratePageId();

 private:
  std::filesystem::path path_;
};

}  // namespace tskv
