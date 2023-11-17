//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>
 
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
   dir_.push_back(std::make_shared<Bucket>(bucket_size_, global_depth_));
    }

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  //Mask = (2^globalDepth) - 1
  //Subtract one is used to make all the global depth LSB has 1 so i can use it as a mask
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
   std::shared_lock<std::shared_mutex> lock(latch_);

  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
   std::shared_lock<std::shared_mutex> lock(latch_);

  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
   std::shared_lock<std::shared_mutex> lock(latch_);

  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::shared_lock<std::shared_mutex> lock(latch_);
  size_t bucketIndex = IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_[bucketIndex];
  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::unique_lock<std::shared_mutex> lock(latch_);
  return RemoveInternal(key);
 
}


template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RemoveInternal(const K &key) -> bool {
 


  size_t bucketIndex = IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_[bucketIndex];
  return bucket->Remove(key);
 
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::unique_lock<std::shared_mutex> lock(latch_);
  InsertInternal(key,value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::InsertInternal(const K &key, const V &value) {
 
    size_t bucketIndex = IndexOf(key);
    V val;
    if (dir_[bucketIndex]->Find(key, val)) {
    if (val == value) {
      //(k1,v1) (k1,v1) not allowed
      return;
    }
    // (k1,v1) (k1,v2) not allowed
    dir_[bucketIndex]->Remove(key);
    } 
    if (dir_[bucketIndex]->IsFull()) {
      //case 2: i need to split the directory and the buckets
      if (dir_[bucketIndex]->GetDepth() == this->GetGlobalDepthInternal()) {
        size_t dirSize = dir_.size();
          for (size_t i = 0; i < dirSize; i++) {
            dir_.push_back(dir_[i]);
          }
           this->IncrementGlobalDepth();
      } 
      //case 1 or 2: i need to split the buckets
          
          dir_[bucketIndex]->IncrementDepth();
          SplitBucket(bucketIndex);  
          this->InsertInternal(key, value);
          return;
    }
    //Normal Insert
    dir_[bucketIndex]->Insert(key, value);
  
}
template <typename K, typename V>
int ExtendibleHashTable<K, V>::getTest() {
      return  testValue;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::IncrementGlobalDepth() {
    this->global_depth_ = this->global_depth_ + 1;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::SplitBucket(int bucketIndex) {
          //This Function is Just Responsiple for Rehashing the inside buckets
          /*
            This Function Expects:
             bucketIndex has a new localDepth 
             hash table has a new globalDepth
          */
  std::shared_ptr<Bucket>bucket = dir_[bucketIndex];
  std::shared_ptr<Bucket> newBucket = std::make_shared<Bucket>(this->bucket_size_, bucket->GetDepth());
  // int newBucketIndex =  (bucketIndex + (dir_.size()/2)) % dir_.size();

  int newBucketIndex = bucketIndex ^ (1 << (bucket->GetDepth() - 1)) % dir_.size();
  testValue = newBucketIndex;
  
  dir_[newBucketIndex] = newBucket;
    int  pair_idx = bucketIndex ^ (1 << (bucket->GetDepth() - 1));
    int idx_diff = 1 << bucket->GetDepth();
    int dir_size = 1 << (GetGlobalDepthInternal());
    /*
    Why this nested loop?
    assmue 
    GlobalDepth = 3    Local depth = 1

    so there is 4 pointers from the directory to the packet on splitting we wanna make it
    2 on one and 2 on one?
    this is the nestd loop !!!
    */
     for (int i = pair_idx - idx_diff; i >= 0; i -= idx_diff) {
      dir_[i] = dir_[pair_idx];
    }
    for (int i = pair_idx + idx_diff; i < dir_size; i += idx_diff) {
      dir_[i] = dir_[pair_idx];
    }

  num_buckets_++; 
 
  //Rehashing Area
  std::list<std::pair<K, V>> copiedList(bucket->GetItems());
  bucket->GetItems().clear();
  for (auto pairElement : copiedList) {
      InsertInternal(pairElement.first, pairElement.second);
  }
  copiedList.clear();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetBucket(int bucketIndex) -> std::shared_ptr<Bucket> {
    return dir_[bucketIndex];
}
//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

//If You notice here there is no cosntant behind V ;)
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
    for (const auto& pairElement : list_) {
      if (pairElement.first == key) {
        value = pairElement.second;
        return true;
      }
    }
    value = {};
    return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
     V val = {};
     for (const auto& pairElement : list_) {
      if (pairElement.first == key) {
        val = pairElement.second;
        list_.remove(std::make_pair(key, val));
        return true;
      }
    }
    return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
    for (auto &pairElement : list_) {
      if (pairElement.first == key) {
        pairElement.second = value;
        return true;
      }
    }
    list_.push_back(std::make_pair(key,value));
    return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
