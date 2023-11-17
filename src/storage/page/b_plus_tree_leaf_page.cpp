//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
    this->SetPageType(IndexPageType::LEAF_PAGE);
    this->SetPageId(page_id);
    this->SetParentPageId(parent_id);
    this->SetMaxSize(max_size);
    this->SetSize(0); 
    this->SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, KeyComparator &comp)  -> bool { 
    for (int i = 0; i < this->GetSize(); i++) {
            if (comp(key, array_[i].first) == 0) {
              int size = GetSize();
              for (int j = i; j < size; j++) {
               array_[j] = array_[j + 1];
                 }
                 this->IncreaseSize(-1);
                 return true;
            }
        }
        return false;
 }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {next_page_id_ = next_page_id;}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key = {};
 
  if ( this->GetMaxSize() <= (index)) {
    return key;
  }
 
  key = array_[index].first;
  return key;
  
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &k,const ValueType &v,  KeyComparator &comp) -> bool {
        if (this->GetMaxSize() == this->GetSize()) {
          return false;
        }
        int i = 0;
        for (; i < this->GetSize(); i++) {
            if (comp(k, array_[i].first) == 1) {
              // i wanna smaller value
                continue;
            }
            if (comp(k, array_[i].first) == 0) {
              //I don't Allow Duplicate Key
              return false;
            }

            break;
        }
        
           for (int j = this->GetSize(); j > i; j--) {
            array_[j] = array_[j - 1];   
           }
            array_[i] = std::make_pair(k,v);
        this->IncreaseSize(1);
        return true;
}
 

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValue(KeyType k,KeyComparator comp, std::vector<ValueType> *result) -> bool {
      for (int i = 0; i < this->GetSize(); i++) {
            if (comp(k, array_[i].first) == 0) {
              result->push_back(array_[i].second);
              return true;
            }
        }
        return false;
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyExist(KeyType k,KeyComparator comp) -> bool {
      for (int i = 0; i < this->GetSize(); i++) {
            if (comp(k, array_[i].first) == 0) {
              return true;
            }
        }
        return false;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::pop() -> MappingType {
    int lastIndex = this->GetSize() - 1;
    MappingType m {};
    if (lastIndex < 0) return m;
    m = array_[lastIndex];
    this->IncreaseSize(-1);
    return m;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::popFront() -> MappingType {
    MappingType m {};
    if (GetSize() == 0) return m;
    int firstIndex = 0;
    
 
    m = array_[firstIndex];
    for (int i = 0; i < this->GetSize(); i++) {
      array_[i] = array_[i + 1];
    }
    this->IncreaseSize(-1);
    return m;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetSplittingPoint() -> int {
  return ceil((this->GetSize()) / 2);
}
 

 INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IsFull() -> bool {
  return this->GetMaxSize() == this->GetSize();
}

 INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IsMin()  -> bool {
  return this->GetMinSize() == this->GetSize();
 }
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::getFirstElement() -> MappingType {
    MappingType m {};
    if (this->GetSize() == 0) {
      return m;
    }
    return array_[0];
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ItemAt(int index) -> MappingType &{
 
    return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  int size = GetSize();

  int left = 0;
  int right = size - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    int ret = comparator(key, array_[mid].first);

    if (ret == 1) {
      left = mid + 1;
    } else if (ret == -1) {
      right = mid - 1;
    } else {
      return mid;
    }
  }

  return left;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
