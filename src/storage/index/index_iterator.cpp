/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(int pageId, BufferPoolManager *buffer_pool_manager, int index) {
        currentPageId = pageId;
        buffer_pool_manager_ = buffer_pool_manager;
        currentIndex = index;
};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
   if (!IsEnd()) buffer_pool_manager_->UnpinPage(currentPageId,false);
    // buffer_pool_manager_->UnpinPage(currentPageId,false);
};  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return currentPageId == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {  
   assert (currentPageId != INVALID_PAGE_ID);
   
   Page* currentPage = buffer_pool_manager_->FetchPage(currentPageId);
   B_PLUS_TREE_LEAF_PAGE_TYPE * currentLeaf =  reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(currentPage->GetData());
   MappingType &myPair = currentLeaf->ItemAt(currentIndex);
 
   buffer_pool_manager_->UnpinPage(currentPageId, false);
   return myPair;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {   
      if (IsEnd()) {
     return *this;
         }

   int oldPageId = currentPageId;
   Page* currentPage = buffer_pool_manager_->FetchPage(oldPageId);
   B_PLUS_TREE_LEAF_PAGE_TYPE * currentLeaf =  reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(currentPage->GetData());
  
   currentIndex++;
   if (currentLeaf->GetSize() == currentIndex) {
    currentIndex = 0;
    currentPageId = currentLeaf->GetNextPageId();
   }
   buffer_pool_manager_->UnpinPage(oldPageId, false);
//    buffer_pool_manager_->UnpinPage(currentPageId, false);
   return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
