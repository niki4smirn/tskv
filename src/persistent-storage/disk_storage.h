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
  DiskStorage(const Options& options);
  Metadata GetMetadata() const override;
  std::shared_ptr<IPage> Read(const PageId& page_id) override;
  bool Write(const PageId& page_id, std::shared_ptr<IPage> page) override;

 private:
  std::string path_;
};

}  // namespace tskv
