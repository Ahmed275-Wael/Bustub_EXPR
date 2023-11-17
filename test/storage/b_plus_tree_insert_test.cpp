//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_insert_test.cpp
//
// Identification: test/storage/b_plus_tree_insert_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>

#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

  TEST(BPlusTreeTests, TestInternalPages) {
     auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());
    RID rid;
    RID rid2;
    int maxInternalNodeBucketSize = 2;
    GenericKey<8> index_key;
    GenericKey<8> index_key2;
    GenericKey<8> index_key3;
    GenericKey<8> index_key4;
    BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> internalPage = {};
    internalPage.Init((page_id_t)1, (page_id_t)0, maxInternalNodeBucketSize); 
    int64_t key = 1;
    int64_t key2 = 2;
    int64_t key3 = 3;
    int64_t key4 = 4;
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key), value);
    rid2.Set(static_cast<int32_t>(key2), value);
    index_key.SetFromInteger(key);
    index_key2.SetFromInteger(key2);
    index_key3.SetFromInteger(key3);
    index_key4.SetFromInteger(key4);
    page_id_t pageId1 = 1;
    page_id_t pageId2 = 2;
    page_id_t pageId3 = 3;
    page_id_t pageId4 = 4;
    page_id_t pageId5 = 5;
    page_id_t pageId6 = 6;
    //Scenario ::
    //Insert Page
    internalPage.Insert(pageId1, index_key, pageId2, comparator);
    ASSERT_EQ(internalPage.GetSize(), 1);
    ASSERT_EQ(internalPage.ValueAt(0), 1);
    ASSERT_EQ(internalPage.Insert(pageId2, index_key2, pageId3, comparator),true);
    ASSERT_EQ(internalPage.GetSize(), 2);
    ASSERT_EQ(internalPage.ValueAt(0), 1);
    ASSERT_EQ(internalPage.ValueAt(1), 2);
    ASSERT_EQ(internalPage.ValueAt(2), 3);
    ASSERT_EQ(comparator(internalPage.KeyAt(2), index_key2), 0);
    //Duplicate Not Allowed
    ASSERT_EQ(internalPage.Insert(pageId1, index_key, pageId2, comparator), false);
    ASSERT_EQ(internalPage.GetSize(), 2);
    //As Max Size 2 is reached we have to split!!
    BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> newInternalPage = {};
    newInternalPage.Init(pageId5, internalPage.GetPageId(), internalPage.GetMaxSize());
      // ASSERT_EQ(comparator(internalPage.InsertInFullNode(index_key3, pageId4, comparator, &newInternalPage).first,  index_key2),0);
    std::pair<GenericKey<8>, page_id_t> resultPair = internalPage.InsertInFullNode(index_key3, pageId4, comparator, &newInternalPage);
    ASSERT_EQ(resultPair.second,  pageId5);
    ASSERT_EQ(comparator(resultPair.first,  index_key2), 0);
    ASSERT_EQ(internalPage.GetSize(), 1);
    ASSERT_EQ(newInternalPage.ValueAt(0), pageId3);
    ASSERT_EQ(internalPage.Insert(pageId2, index_key4, pageId6, comparator),true);
    ASSERT_EQ(internalPage.ValueAt(1), pageId2);
    ASSERT_EQ(internalPage.ValueAt(2), pageId6);

}


  TEST(BPlusTreeTests, TestLeafPages) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
    GenericKey<8> index_key2;
     GenericKey<8> index_key3;
  RID rid;
  
  // create transaction
  auto *transaction = new Transaction(0);

 

  int64_t key1 = 1;
   int64_t key2 = 2;
    int64_t key3 = 3;
  int64_t value = key1 & 0xFFFFFFFF;
  int maxLeafBucketSize = 2;
  rid.Set(static_cast<int32_t>(key1), value);
  index_key.SetFromInteger(key1);
  index_key2.SetFromInteger(key2);
  index_key3.SetFromInteger(key3);
  BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> leafPage = {};
    BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *newLeafPage;
   
  // tree.SplitLeafNode(index_key, newLeafPage, transaction);
  leafPage.Init((page_id_t)1, (page_id_t)0, maxLeafBucketSize); 
  ASSERT_EQ(leafPage.Insert(index_key, rid, comparator),true);
  ASSERT_EQ(leafPage.GetSize(), 1);
  ASSERT_EQ(comparator(leafPage.KeyAt(0), index_key),0);
  leafPage.Insert(index_key2, rid, comparator);
  ASSERT_EQ(leafPage.GetSize(), 2);
    leafPage.Insert(index_key3, rid, comparator);
  ASSERT_EQ(leafPage.GetSize(), 2);
  std::pair<GenericKey<8>, RID>newPair  = std::make_pair(index_key3, rid);
  tree.SplitLeafNode(&leafPage, &newLeafPage, newPair);
  ASSERT_EQ(leafPage.GetSize(), 2);
   ASSERT_EQ(newLeafPage->GetSize() , 1);
  ASSERT_EQ(comparator(newLeafPage->KeyAt(0), index_key3), 0);
 ASSERT_EQ(leafPage.GetNextPageId(), newLeafPage->GetPageId());
 ASSERT_EQ(leafPage.GetParentPageId(), newLeafPage->GetParentPageId());
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");

}
//   TEST(BPlusTreeTests, SampleTestBPlusTree) {
//   // create KeyComparator and index schema
//   auto key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<8> comparator(key_schema.get());

//   auto *disk_manager = new DiskManager("test.db");
//   BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
//   // create b+ tree
//   BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
//   GenericKey<8> index_key;
//   GenericKey<8> index_key2;
//   // GenericKey<8> index_key3;
 
//   RID rid;
 
//   int64_t key5;
//   int64_t key4;
//   // create transaction
//   auto *transaction = new Transaction(0);

//     page_id_t pageId1 = 1;
//     page_id_t pageId2 = 2;
//     page_id_t pageId6 = 6;

//     key5 = 5;
//     key4 = 4;
//     int64_t value = key5 & 0xFFFFFFFF;
//     rid.Set(static_cast<int32_t>(key5), value);
//     index_key.SetFromInteger(key5);
//      index_key2.SetFromInteger(key4);
 
 
//   delete transaction;
//   delete disk_manager;
//   delete bpm;
//   remove("test.db");
//   remove("test.log");
// }

 
TEST(BPlusTreeTests, DISABLED_myTest) {
  /*
    This Test Assume it is binary tree 
    NO SPLITS CHECK
    NO CONCCURENCY CHECK
    NO OVERFLOW CHECK
  */
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  ASSERT_EQ(page_id, HEADER_PAGE_ID);
  (void)header_page;
  int64_t key = 42;
  int64_t value = key & 0xFFFFFFFF;
  rid.Set(static_cast<int32_t>(key), value);
  index_key.SetFromInteger(key);
  tree.Insert(index_key, rid, transaction);
  auto root_page_id = tree.GetRootPageId();
  auto root_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id)->GetData());
  ASSERT_NE(root_page, nullptr);
  ASSERT_TRUE(root_page->IsLeafPage());
    
 
  auto root_as_leaf = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(root_page);
  ASSERT_EQ(root_as_leaf->GetSize(), 1);
  // ASSERT_NE(root_as_leaf->KeyAt(0),  nullptr );
  ASSERT_EQ(comparator(root_as_leaf->KeyAt(0), index_key), 0);
  tree.Insert(index_key, rid, transaction);
  ASSERT_EQ(tree.Insert(index_key, rid, transaction), false);
  ASSERT_EQ(root_as_leaf->GetSize(), 1);
  int64_t key2 = 43;
  int64_t value2 = key2 & 0xFFFFFFFF;
  rid.Set(static_cast<int32_t>(key2), value2);
  index_key.SetFromInteger(key2);
  ASSERT_EQ(tree.Insert(index_key, rid, transaction),true);
  ASSERT_EQ(tree.Insert(index_key, rid, transaction),false);
  ASSERT_EQ(root_as_leaf->GetSize(), 2);
  int64_t key3 = 44;
  int64_t value3 = key2 & 0xFFFFFFFF;
  rid.Set(static_cast<int32_t>(key3), value3);
  index_key.SetFromInteger(key3);
  ASSERT_EQ(tree.Insert(index_key, rid, transaction),true);
  ASSERT_EQ(root_as_leaf->GetSize(), 1);
  //  std::vector<RID> rids;
  // ASSERT_EQ(tree.GetValue(index_key, &rids, transaction),true);
  // ASSERT_EQ(rids.size(), 1);
  bpm->UnpinPage(root_page_id, false);
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}
TEST(BPlusTreeTests, InsertTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  ASSERT_EQ(page_id, HEADER_PAGE_ID);
  (void)header_page;

  int64_t key = 42;
  int64_t value = key & 0xFFFFFFFF;
  rid.Set(static_cast<int32_t>(key), value);
  index_key.SetFromInteger(key);
  tree.Insert(index_key, rid, transaction);

  auto root_page_id = tree.GetRootPageId();
  auto root_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id)->GetData());
  ASSERT_NE(root_page, nullptr);
  ASSERT_TRUE(root_page->IsLeafPage());

  auto root_as_leaf = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(root_page);
  ASSERT_EQ(root_as_leaf->GetSize(), 1);
  ASSERT_EQ(comparator(root_as_leaf->KeyAt(0), index_key), 0);

  bpm->UnpinPage(root_page_id, false);
  bpm->UnpinPage(HEADER_PAGE_ID, true);
 
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}



TEST(BPlusTreeTests, InsertTest2Moidified) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
 
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    ASSERT_EQ(tree.Insert(index_key, rid, transaction),true);
  }
      // auto root_page_id = tree.GetRootPageId();
      // auto root_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id)->GetData());
      //  auto root_as_leaf = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(root_page);
 
  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    ASSERT_EQ(tree.GetValue(index_key, &rids, transaction),true);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

 
 
  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids, transaction);

    EXPECT_EQ(is_present, true);
    EXPECT_EQ(rids.size(), 1);
    EXPECT_EQ(rids[0].GetPageId(), 0);
    EXPECT_EQ(rids[0].GetSlotNum(), key);
    size = size + 1;
  }

  EXPECT_EQ(size, keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}
TEST(BPlusTreeTests, InsertTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    ASSERT_EQ(tree.Insert(index_key, rid, transaction),true);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    ASSERT_EQ(tree.GetValue(index_key, &rids),true);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    EXPECT_EQ(is_present, true);
    EXPECT_EQ(rids.size(), 1);
    EXPECT_EQ(rids[0].GetPageId(), 0);
    EXPECT_EQ(rids[0].GetSlotNum(), key);
    size = size + 1;
  }

  EXPECT_EQ(size, keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}
TEST(BPlusTreeTests, HardTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(10, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    ASSERT_EQ(tree.Insert(index_key, rid, transaction),true);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    ASSERT_EQ(tree.GetValue(index_key, &rids),true);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    EXPECT_EQ(is_present, true);
    EXPECT_EQ(rids.size(), 1);
    EXPECT_EQ(rids[0].GetPageId(), 0);
    EXPECT_EQ(rids[0].GetSlotNum(), key);
    size = size + 1;
  }

  EXPECT_EQ(size, keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  // EXPECT_EQ(bpm->GetFreeEvictableSize(), 50);
  // EXPECT_EQ(bpm->GetFreeListSize(), 50);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, InsertTest3) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  ASSERT_EQ(page_id, HEADER_PAGE_ID);
  (void)header_page;

  std::vector<int64_t> keys = {1,2,4,5,3,6,7};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }
  
  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  start_key = 3;
  current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
 
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, InsertTest4) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  ASSERT_EQ(page_id, HEADER_PAGE_ID);
  (void)header_page;

  std::vector<int64_t> keys = {1,2,4,5,3,6,7};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }
  
  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  

  bpm->UnpinPage(HEADER_PAGE_ID, true);
//  EXPECT_EQ(bpm->GetFreeEvictableSize(), 0);
//  EXPECT_EQ(bpm->GetFreeListSize(), 0);

  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}
}  // namespace bustub
