//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */


 
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
    // this->page_id_ = page_id;
    this->SetPageType(IndexPageType::INTERNAL_PAGE);
    this->SetPageId(page_id);
    this->SetParentPageId(parent_id);
    this->SetMaxSize(max_size); 
    this->SetSize(0); 
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key{};
  if (index == 0 || index > this->GetSize())  {
    return key;
  }
  key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
    if (index == 0) return;

    if (  index > this->GetMaxSize() ) return;

    array_[index].first = key;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ChangeKeyOfValue(ValueType value, KeyType &key) {
        if (ValueAt(0) == value) return;
        for (int i = 1; i < GetArraySize(); i++) {
          if (ValueAt(i) == value) {
            SetKeyAt(i, key);
          }
        }
        
}

INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(ValueType value) {
        //Note the core assumption of the implementation is the left page never can be removed no matter what happened
        //as we move every thing in this direction <<<
      for (int i = 1; i < GetArraySize(); i++) {
        if (ValueAt(i) == value) {
               int size = GetArraySize() - 1;
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
bool B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const ValueType &leftPointer, const KeyType &key,const ValueType &RightPointer,KeyComparator &comparator) { 

  if (GetSize() == 0) {
      //First Insertion
      KeyType k {};
      std::pair<KeyType, ValueType> firstPair = std::make_pair(k, leftPointer);
      array_[0] = firstPair;
   }
  return InsertAndShift(key, RightPointer, comparator);
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAndShift(const KeyType &k,const ValueType &Pointer,KeyComparator &comp) -> bool {
      //This Function assume left pointer is fixed so if we split full node our original pointer alwayss points to the left (ALWAYSSS!!!)
  if (this->GetMaxSize() == this->GetSize()) {
          return false;
        }
        int i = 1;
        for (; i < this->GetSize() + 1; i++) {
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
        
           for (int j = this->GetSize() + 1; j > i; j--) {
            array_[j] = array_[j - 1];   
           }
            array_[i] = std::make_pair(k,Pointer);
        this->IncreaseSize(1);
        return true;
}

INDEX_TEMPLATE_ARGUMENTS 
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetArraySize() const -> int {
    return this->GetSize() + 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertInFullNode(const KeyType &k,const ValueType &Pointer,KeyComparator &comparator, BPlusTreeInternalPage* reciever) -> MappingType {
      BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> temporaryPage;
      temporaryPage.SetMaxSize(this->GetMaxSize() + 1); //Just For Holding all so i can split easily (This step is for simplification)
      temporaryPage.SetSize(0);
      temporaryPage.SetPageType(IndexPageType::INTERNAL_PAGE);

      int arrayLength = GetArraySize();
      // if (arrayLength != this->GetMaxSize() + 1) {
      //      KeyType k;
      //      ValueType v;
      //      return std::make_pair(k, v);
      // }
      for (int i = 1; i < arrayLength; i++) {
           ValueType leftPointer =  this->ValueAt(i - 1);
        ValueType rightPointer = this->ValueAt(i);
        KeyType key = this->KeyAt(i);
        temporaryPage.Insert(leftPointer, key, rightPointer, comparator);
      }
      temporaryPage.InsertAndShift(k, Pointer, comparator);
      for (int i = 0; i < arrayLength - 1; i++) {
        this->pop();
      }
  int median = floor(temporaryPage.GetSize() / 2.0);
 

  for (int i = 1; i < median + 1; i++) {
      this->Insert(temporaryPage.ValueAt(i - 1), temporaryPage.KeyAt(i), temporaryPage.ValueAt(i), comparator);
  }
   for (int i = median + 2; i < temporaryPage.GetSize() + 1; i++) {
      reciever->Insert(temporaryPage.ValueAt(i - 1), temporaryPage.KeyAt(i), temporaryPage.ValueAt(i), comparator);
  }
    //Median + 1 is sent to the caller so he insert it to the parent 
    MappingType returnPair = std::make_pair(temporaryPage.KeyAt(median + 1), reciever->GetPageId());
    return returnPair;
 }
/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
    ValueType v{};
    int size = GetArraySize();
    if (  index >= size ) return v;
    return array_[index].second;
 

 }
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::pushFront(KeyType &key, ValueType value) -> void {
    if (IsFull()) {
      return;
    }   
    int i = GetArraySize();
    for (; i > 1; i--) {
        array_[i] = array_[i - 1];
    }
    MappingType firstIndexPair = std::make_pair(key, array_[0].second);
    array_[1] = firstIndexPair;
    array_[0].second = value;
    this->IncreaseSize(1);
 }
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::pushBack(KeyType &key, ValueType value) -> void {
    if (IsFull()) {
      return;
    }   
    int length = GetArraySize();
    MappingType pair = std::make_pair(key, value);
    this->IncreaseSize(1);
    array_[length ] = pair;
     
 }
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IsFull()  -> bool {
  return this->GetMaxSize() == this->GetSize();
 }

 INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IsMin()  -> bool {
  return this->GetMinSize() == this->GetSize();
 }
 INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::getFirstElement() -> MappingType {
    MappingType m {};
    if (this->GetSize() == 0) {
      return m;
    }
    return array_[1];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::pop() -> MappingType {
    int lastIndex = this->GetSize(); 
    MappingType m {};
    if (lastIndex < 0) return m;
    m = array_[lastIndex];
    if (this->GetSize() > 0) {
    this->IncreaseSize(-1);
    }
    return m;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::popFront() -> MappingType {
     
    MappingType m {};
    if (GetSize() == 0) return m;
    m = std::make_pair(array_[1].first,array_[0].second);
    array_[0].second = array_[1].second;
    
    for (int i = 1; i < GetArraySize() - 1; i++) {
      array_[i] = array_[i + 1];
    }
     if (this->GetSize() > 0) {
    this->IncreaseSize(-1);
    }
    return m;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertInFirstIndex(ValueType leftPointer) -> void {
  KeyType k {};
  std::pair<KeyType,ValueType>m = std::make_pair(k, leftPointer);
  array_[0] = m;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetMedian() -> int {
  return ceil((this->GetSize() - 1) / 2) + 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetLeftSibling(page_id_t currentPageId) -> page_id_t {
    if (ValueAt(0) == currentPageId) return INVALID_PAGE_ID;
    for (int i = 1; i < GetArraySize(); i++) {
        if (ValueAt(i) == currentPageId) return ValueAt(i - 1);
    }
 
    return INVALID_PAGE_ID; 
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetRightSibling(page_id_t currentPageId) -> page_id_t {
    if (ValueAt(GetArraySize() - 1) == currentPageId) return INVALID_PAGE_ID;
    for (int i = 0; i < GetArraySize() - 1; i++) {
        if (ValueAt(i) == currentPageId) return ValueAt(i + 1);
    }
    return INVALID_PAGE_ID; 
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetKeyOfValue(ValueType value) -> KeyType {
     KeyType k{};
    if (ValueAt(0) == value) return k;
    for (int i = 1; i < GetArraySize(); i++) {
        if (ValueAt(i) == value) return KeyAt(i);
    }
    return k;
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
