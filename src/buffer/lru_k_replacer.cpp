//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

  LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k): replacer_size_(num_frames), k_(k) {
    historyQueue = std::unordered_map < frame_id_t, std::list < size_t >> ();
    lastAccesedQueue = std::unordered_map < frame_id_t, size_t > ();

  }

  auto LRUKReplacer::Evict(frame_id_t * frame_id) -> bool {
    std::scoped_lock < std::mutex > lock(latch_);
    // initialize frame_id with an invalid value
    * frame_id = -1;
       IncrementTimeStamp();
    if (historyQueue.empty() && lastAccesedQueue.empty()) {
      return false;
    }
    size_t lastRecentlyUsedts = current_timestamp_;
    if (!historyQueue.empty()) {
        //This means we do have infinity time stamp in our queues shoud LRU is used!!
      for (auto kv_pair: historyQueue) {
        //Bring The one the LRU frame
        if (IsEvictable.find(kv_pair.first) != IsEvictable.end() && kv_pair.second.front() < lastRecentlyUsedts) {
          * frame_id = kv_pair.first;
            lastRecentlyUsedts = kv_pair.second.front();
        }
      }
     
    }

      if (historyQueue.find(*frame_id) != historyQueue.end()) {
      historyQueue.erase( * frame_id);
      IsEvictable.erase( * frame_id);
      return true;
  }

        //HERE WE DON'T HAVE INFINITY SO USE LRU-K
    for (auto kv_pair: lastAccesedQueue) {
      if (IsEvictable.find(kv_pair.first) != IsEvictable.end() && kv_pair.second  < lastRecentlyUsedts) {
        * frame_id = kv_pair.first;
        lastRecentlyUsedts = kv_pair.second;
      }
    }
    if ( * frame_id == -1) {
      return false;
    }
    lastAccesedQueue.erase( * frame_id);
    IsEvictable.erase( * frame_id);
    return true;

  }

  void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    std::scoped_lock < std::mutex > lock(latch_);
    IncrementTimeStamp();
    if (frame_id >= static_cast < int > (replacer_size_)) {
      return;
    }
    if (lastAccesedQueue.find(frame_id) != lastAccesedQueue.end()) {
      lastAccesedQueue[frame_id] = current_timestamp_;
      return;
    }
    if (historyQueue.find(frame_id) == historyQueue.end()) {
      historyQueue[frame_id] = std::list < size_t > ();
    }

    // insert into history queue
    historyQueue[frame_id].push_back(current_timestamp_);

    // move it to cache queue if accessed more than k times
    if (historyQueue[frame_id].size() >= k_) {
      lastAccesedQueue[frame_id] = current_timestamp_;
      historyQueue.erase(frame_id);
    }
  }

  void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    //This is sth like Pin, and UnPin in the ClockReplacer policy
    std::scoped_lock < std::mutex > lock(latch_);
    IncrementTimeStamp();
    if (set_evictable && historyQueue.find(frame_id) == historyQueue.end() &&
      lastAccesedQueue.find(frame_id) == lastAccesedQueue.end()) {
      return;
    }
    if (set_evictable) {
      IsEvictable[frame_id] = true;
    } else {
      IsEvictable.erase(frame_id);
    }
  }

  void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::scoped_lock < std::mutex > lock(latch_);
    current_timestamp_++;
    historyQueue.erase(frame_id);
    lastAccesedQueue.erase(frame_id);
    IsEvictable.erase(frame_id);

  }

  auto LRUKReplacer::Size() -> size_t {
    std::scoped_lock < std::mutex > lock(latch_);
    return IsEvictable.size();

  }
  void LRUKReplacer::IncrementTimeStamp() {
    current_timestamp_++;
  }

} // namespace bustub

 