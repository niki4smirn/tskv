#include "model/model.h"
#include "persistent-storage/persistent_storage.h"

namespace tskv {

class IPage {
 public:
  IPage(std::shared_ptr<IPersistentStorage> storage);
  virtual ~IPage() = default;

  TimeSeries Read(const TimeRange& time_range);
};

}  // namespace tskv
