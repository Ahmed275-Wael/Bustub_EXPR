#include <memory>
#include <string>

#include "buffer/buffer_pool_manager_instance.h"
#include "catalog/Lcatalog.h"
#include "common/config.h"
#include "common/logger.h"
//#include "common/dberr.h"
#include "storage/disk/disk_manager.h"
//#include"page/index_roots_page.h"
namespace bustub {
class DBStorageEngine {
public:
  explicit DBStorageEngine(std::string db_name, bool init = true,
                           uint32_t buffer_pool_size = 1024)
          : db_file_name_(std::move(db_name)), init_(init) {
    // Init database file if needed
    if (init_) {
      remove(db_file_name_.c_str());
    }
    // Initialize components
    disk_mgr_ = new DiskManager(db_file_name_);
    try {
    bpm_ = new BufferPoolManagerInstance(128, disk_mgr_, LRUK_REPLACER_K);
  } catch (NotImplementedException &e) {
    std::cerr << "BufferPoolManager is not implemented, only mock tables are supported." << std::endl;
    bpm_ = nullptr;
  }
    catalog_mgr_ = new CatalogManager(bpm_, nullptr, nullptr, init);
    // Allocate static page for db storage engine
    if (init) {
      page_id_t id ;
    //   BUSTUB_ASSERT(bpm_->IsPageFree(CATALOG_META_PAGE_ID), "Catalog meta page not free.");
    //   BUSTUB_ASSERT(bpm_->IsPageFree(INDEX_ROOTS_PAGE_ID), "Header page not free.");
    LOG_DEBUG("here");
    BUSTUB_ASSERT(bpm_->NewPage(&id) != nullptr && id == CATALOG_META_PAGE_ID, "Failed to allocate catalog meta page.");
    bpm_->UnpinPage(CATALOG_META_PAGE_ID, false);
  
     // BUSTUB_ASSERT(bpm_->NewPage(id) != nullptr && id == INDEX_ROOTS_PAGE_ID, "Failed to allocate header page.");
      
      
     // bpm_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
      
    } else {
     // BUSTUB_ASSERT(!bpm_->IsPageFree(CATALOG_META_PAGE_ID), "Invalid catalog meta page.");
    //  BUSTUB_ASSERT(!bpm_->IsPageFree(INDEX_ROOTS_PAGE_ID), "Invalid header page.");
    }
  }

  ~DBStorageEngine() {
    delete catalog_mgr_;
    delete bpm_;
    delete disk_mgr_;
  }

public:
  DiskManager *disk_mgr_;
  BufferPoolManager *bpm_;
  CatalogManager *catalog_mgr_;
  std::string db_file_name_;
  bool init_;
};
}