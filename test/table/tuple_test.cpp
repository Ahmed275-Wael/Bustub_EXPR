//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// tuple_test.cpp
//
// Identification: test/table/tuple_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "logging/common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"

namespace bustub {
// NOLINTNEXTLINE

void GetRandomString(char *str, int size) {
  //This size include that of the nullchar
char randomData[] = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqaaaaaaaazzabcdefghijklmnopqrstuvwxyzabcdefghijkl";
int randomDataSize = sizeof(randomData)/sizeof(char);
assert(BUSTUB_PAGE_SIZE <= randomDataSize);
assert(randomDataSize >= size);
 
 for (int i = 0; i < size - 1;i++) {
  str[i] = randomData[i];
 }
  str[size - 1] = 0;
}
TEST(TupleTest, TableHeapTest) {
  // test1: parse create sql statement
  std::string create_stmt = "a varchar(20), b smallint, c bigint, d bool, e varchar(16)";
  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  Column col3{"c", TypeId::BIGINT};
  Column col4{"d", TypeId::BOOLEAN};
  Column col5{"e", TypeId::VARCHAR, 16};
  std::vector<Column> cols{col1, col2, col3, col4, col5};
  Schema schema{cols};
  Tuple tuple = ConstructTuple(&schema);

  // create transaction
  auto *transaction = new Transaction(0);
  auto *disk_manager = new DiskManager("test.db");
  auto *buffer_pool_manager = new BufferPoolManagerInstance(50, disk_manager);
  auto *lock_manager = new LockManager();
  auto *log_manager = new LogManager(disk_manager);
  auto *table = new TableHeap(buffer_pool_manager, lock_manager, log_manager, transaction);

  std::vector<RID> rid_v;
  //  LOG_DEBUG("Tuble Size is %d", tuple.GetLength());
  for (int i = 0; i < 1; ++i) {
      // LOG_DEBUG("%d", i);
    RID rid;
    table->InsertTuple(tuple, &rid, transaction);
    rid_v.push_back(rid);
  }
  // LOG_DEBUG("Tuples are inserted sucessfullyy!!!");
  TableIterator itr = table->Begin(transaction);
  while (itr != table->End()) {
    // std::cout << itr->ToString(schema) << std::endl;
    ++itr;
  }

  // int i = 0;
  std::shuffle(rid_v.begin(), rid_v.end(), std::default_random_engine(0));
  for (const auto &rid : rid_v) {
    // std::cout << i++ << std::endl;
    BUSTUB_ENSURE(table->MarkDelete(rid, transaction) == 1, "");
  }
  disk_manager->ShutDown();
  remove("test.db");  // remove db file
  remove("test.log");
  delete table;
  delete buffer_pool_manager;
  delete disk_manager;
  delete transaction;
  delete log_manager;
  delete lock_manager;
}


TEST(TupleTest, TableHeapLargeInsertion) {
  // test1: parse create sql statement
  std::string create_stmt = "a varchar(20), b smallint, c bigint, d bool, e varchar(16)";
  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  Column col3{"c", TypeId::BIGINT};
  Column col4{"d", TypeId::BOOLEAN};
  Column col5{"e", TypeId::VARCHAR, 16};
  std::vector<Column> cols{col1, col2, col3, col4, col5};
  Schema schema{cols};



 
 //LETS ADD VALUES IN THE TUPLE 
 //string of length 300 (THIS TEST DEPENDS ON THE BUSTUB_PAGE_SIZE )
 
  int overFlowedSize = BUSTUB_PAGE_SIZE / 2;
 char overFlowedData[BUSTUB_PAGE_SIZE / 2];
  GetRandomString(overFlowedData, overFlowedSize);
  std::vector<Value> values;
  Value v(TypeId::INVALID);

  auto seed = std::chrono::system_clock::now().time_since_epoch().count();

  std::mt19937 generator(seed);  // mt19937 is a standard mersenne_twister_engine

  for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
    // get type
    const auto &col = schema.GetColumn(i);
    TypeId type = col.GetType();
    switch (type) {
      case TypeId::BOOLEAN:
        v = Value(type, 1);
        break;
      case TypeId::TINYINT:
        v = Value(type, 12);
        break;
      case TypeId::SMALLINT:
        v = Value(type, 13);
        break;
      case TypeId::INTEGER:
        v = Value(type, 14);
        break;
      case TypeId::BIGINT:
        v = Value(type, 17);
        break;
      case TypeId::VARCHAR: {
  
        v = Value(type, overFlowedData, overFlowedSize, true);
        break;
      }
      default:
        break;
    }
    values.emplace_back(v);
  }
  // END ADD VALUES
  Tuple tuple(values, &schema);




  // create transaction
  auto *transaction = new Transaction(0);
  auto *disk_manager = new DiskManager("test.db");
  auto *buffer_pool_manager = new BufferPoolManagerInstance(50, disk_manager);
  auto *lock_manager = new LockManager();
  auto *log_manager = new LogManager(disk_manager);
  auto *table = new TableHeap(buffer_pool_manager, lock_manager, log_manager, transaction);

  std::vector<RID> rid_v;
  //  LOG_DEBUG("Tuble Size is %d", tuple.GetLength());
  for (int i = 0; i < 1; ++i) { 
      // LOG_DEBUG("%d", i);
    RID rid;
    assert(table->InsertTuple(tuple, &rid, transaction) == true);
    rid_v.push_back(rid);
  }
  // LOG_DEBUG("Tuples are inserted sucessfullyy!!!");
  TableIterator itr = table->Begin(transaction);
 
    // std::cout << itr->ToString(schema) << std::endl;
    Tuple tuple2 = *itr;
 
    EXPECT_EQ(tuple2.GetValue(&schema,0).CompareEquals(values[0]), CmpBool::CmpTrue);
    EXPECT_EQ(tuple2.GetValue(&schema,1).CompareEquals(values[1]), CmpBool::CmpTrue);
    EXPECT_EQ(tuple2.GetValue(&schema,2).CompareEquals(values[2]), CmpBool::CmpTrue);
    EXPECT_EQ(tuple2.GetValue(&schema,3).CompareEquals(values[3]), CmpBool::CmpTrue);
    EXPECT_EQ(tuple2.GetValue(&schema,4).CompareEquals(values[4]), CmpBool::CmpTrue);
    // LOG_DEBUG("%s", tuple2.GetValue(&schema,4).GetData());
 
 
    ++itr;
 
  // // int i = 0;
  // std::shuffle(rid_v.begin(), rid_v.end(), std::default_random_engine(0));
  // for (const auto &rid : rid_v) {
  //   // std::cout << i++ << std::endl;
  //   BUSTUB_ENSURE(table->MarkDelete(rid, transaction) == 1, "");
  // }
  disk_manager->ShutDown();
  remove("test.db");  // remove db file
  remove("test.log");
  delete table;
  delete buffer_pool_manager;
  delete disk_manager;
  delete transaction;
  delete log_manager;
  delete lock_manager;
}

TEST(TupleTest,  TableHeapMultipleLargeInsertion) {
  // test1: parse create sql statement
  std::string create_stmt = "a varchar(20), b smallint, c bigint, d bool, e varchar(16)";
  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  Column col3{"c", TypeId::BIGINT};
  Column col4{"d", TypeId::BOOLEAN};
  Column col5{"e", TypeId::VARCHAR, 16};
  std::vector<Column> cols{col1, col2, col3, col4, col5};
  Schema schema{cols};



 
 //LETS ADD VALUES IN THE TUPLE 
 //string of length 300 (THIS TEST DEPENDS ON THE BUSTUB_PAGE_SIZE )
 
  int overFlowedSize = BUSTUB_PAGE_SIZE;
  char overFlowedData[BUSTUB_PAGE_SIZE];
  GetRandomString(overFlowedData, overFlowedSize);
 
  std::vector<Value> values;
  Value v(TypeId::INVALID);

  auto seed = std::chrono::system_clock::now().time_since_epoch().count();

  std::mt19937 generator(seed);  // mt19937 is a standard mersenne_twister_engine

  for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
    // get type
    const auto &col = schema.GetColumn(i);
    TypeId type = col.GetType();
    switch (type) {
      case TypeId::BOOLEAN:
        v = Value(type, 1);
        break;
      case TypeId::TINYINT:
        v = Value(type, 12);
        break;
      case TypeId::SMALLINT:
        v = Value(type, 13);
        break;
      case TypeId::INTEGER:
        v = Value(type, 14);
        break;
      case TypeId::BIGINT:
        v = Value(type, 17);
        break;
      case TypeId::VARCHAR: {
  
        v = Value(type, overFlowedData, overFlowedSize, true);
        break;
      }
      default:
        break;
    }
    values.emplace_back(v);
  }
  // END ADD VALUES
  Tuple tuple(values, &schema);




  // create transaction
  auto *transaction = new Transaction(0);
  auto *disk_manager = new DiskManager("test.db");
  auto *buffer_pool_manager = new BufferPoolManagerInstance(50, disk_manager);
  auto *lock_manager = new LockManager();
  auto *log_manager = new LogManager(disk_manager);
  auto *table = new TableHeap(buffer_pool_manager, lock_manager, log_manager, transaction);

  std::vector<RID> rid_v;
  //  LOG_DEBUG("Tuble Size is %d", tuple.GetLength());
  for (int i = 0; i < 1; ++i) { 
      // LOG_DEBUG("%d", i);
    RID rid;
    assert(table->InsertTuple(tuple, &rid, transaction) == true);
    rid_v.push_back(rid);
  }
  // LOG_DEBUG("Tuples are inserted sucessfullyy!!!");
  TableIterator itr = table->Begin(transaction);
 
  //   // std::cout << itr->ToString(schema) << std::endl;
    Tuple tuple2 = *itr;
 
    EXPECT_EQ(tuple2.GetValue(&schema,0).CompareEquals(values[0]), CmpBool::CmpTrue);
    EXPECT_EQ(tuple2.GetValue(&schema,1).CompareEquals(values[1]), CmpBool::CmpTrue);
    EXPECT_EQ(tuple2.GetValue(&schema,2).CompareEquals(values[2]), CmpBool::CmpTrue);
    EXPECT_EQ(tuple2.GetValue(&schema,3).CompareEquals(values[3]), CmpBool::CmpTrue);
    EXPECT_EQ(tuple2.GetValue(&schema,4).CompareEquals(values[4]), CmpBool::CmpTrue);
    // LOG_DEBUG("%s", tuple2.GetValue(&schema,4).GetData());
 
 
    // ++itr;
 
  // // int i = 0;
  // std::shuffle(rid_v.begin(), rid_v.end(), std::default_random_engine(0));
  // for (const auto &rid : rid_v) {
  //   // std::cout << i++ << std::endl;
  //   BUSTUB_ENSURE(table->MarkDelete(rid, transaction) == 1, "");
  // }
  disk_manager->ShutDown();
  remove("test.db");  // remove db file
  remove("test.log");
  delete table;
  delete buffer_pool_manager;
  delete disk_manager;
  delete transaction;
  delete log_manager;
  delete lock_manager;
}

TEST(TupleTest,  TableHeapUpdate) {
  // test1: parse create sql statement
  std::string create_stmt = "a varchar(20), b smallint, c bigint, d bool, e varchar(16)";
  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  Column col3{"c", TypeId::BIGINT};
  Column col4{"d", TypeId::BOOLEAN};
  Column col5{"e", TypeId::VARCHAR, 16};
  std::vector<Column> cols{col1, col2, col3, col4, col5};
  Schema schema{cols};
 //LETS ADD VALUES IN THE TUPLE 
 //string of length 300 (THIS TEST DEPENDS ON THE BUSTUB_PAGE_SIZE )
 

   int overFlowedSize = 15;
  char overFlowedData[15];
  GetRandomString(overFlowedData, overFlowedSize);
 
  std::vector<Value> values;
  Value v(TypeId::INVALID);
  for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
    // get type
    const auto &col = schema.GetColumn(i);
    TypeId type = col.GetType();
    switch (type) {
      case TypeId::BOOLEAN:
        v = Value(type, 1);
        break;
      case TypeId::TINYINT:
        v = Value(type, 12);
        break;
      case TypeId::SMALLINT:
        v = Value(type, 13);
        break;
      case TypeId::INTEGER:
        v = Value(type, 14);
        break;
      case TypeId::BIGINT:
        v = Value(type, 17);
        break;
      case TypeId::VARCHAR: {
  
        v = Value(type, overFlowedData, overFlowedSize, true);
        break;
      }
      default:
        break;
    }
    values.emplace_back(v);
  }
  // END ADD VALUES
  Tuple tuple(values, &schema);




  // create transaction
  auto *transaction = new Transaction(0);
  auto *disk_manager = new DiskManager("test.db");
  auto *buffer_pool_manager = new BufferPoolManagerInstance(50, disk_manager);
  auto *lock_manager = new LockManager();
  auto *log_manager = new LogManager(disk_manager);
  auto *table = new TableHeap(buffer_pool_manager, lock_manager, log_manager, transaction);

 
  //  LOG_DEBUG("Tuble Size is %d", tuple.GetLength());
  
      // LOG_DEBUG("%d", i);
    RID rid;
    assert(table->InsertTuple(tuple, &rid, transaction) == true);
 
   
  // LOG_DEBUG("Tuples are inserted sucessfullyy!!!");
  TableIterator itr = table->Begin(transaction);
 
  //   // std::cout << itr->ToString(schema) << std::endl;
    Tuple tuple2 = *itr;
 
    EXPECT_EQ(tuple2.GetValue(&schema,0).CompareEquals(values[0]), CmpBool::CmpTrue);
    EXPECT_EQ(tuple2.GetValue(&schema,1).CompareEquals(values[1]), CmpBool::CmpTrue);
    EXPECT_EQ(tuple2.GetValue(&schema,2).CompareEquals(values[2]), CmpBool::CmpTrue);
    EXPECT_EQ(tuple2.GetValue(&schema,3).CompareEquals(values[3]), CmpBool::CmpTrue);
    EXPECT_EQ(tuple2.GetValue(&schema,4).CompareEquals(values[4]), CmpBool::CmpTrue);
    // LOG_DEBUG("%s", tuple2.GetValue(&schema,4).GetData());
    values.clear();

 

   int secondDataSize = BUSTUB_PAGE_SIZE/2;
  char secondData[BUSTUB_PAGE_SIZE/2];
  GetRandomString(overFlowedData, overFlowedSize);
 
  v = Value(col1.GetType(), secondData, secondDataSize, true);
  values.push_back(v);
  values.push_back({col2.GetType(),15});
  values.push_back({col3.GetType(),16});
  values.push_back({col4.GetType(), 0});
  v = Value(col5.GetType(), secondData, secondDataSize, true);
  values.push_back(v);


   auto to_update_tuple = Tuple{values, &schema};

    bool updated =  table->UpdateTuple(to_update_tuple, rid, transaction);
  assert(updated==true);
  TableIterator itr2 = table->Begin(transaction);
 
  //   // std::cout << itr->ToString(schema) << std::endl;
    Tuple tuple3 = *itr2;
 
      EXPECT_EQ(tuple3.GetValue(&schema,0).CompareEquals(values[0]), CmpBool::CmpTrue);
    EXPECT_EQ(tuple3.GetValue(&schema,1).CompareEquals(values[1]), CmpBool::CmpTrue);
    EXPECT_EQ(tuple3.GetValue(&schema,2).CompareEquals(values[2]), CmpBool::CmpTrue);
    EXPECT_EQ(tuple3.GetValue(&schema,3).CompareEquals(values[3]), CmpBool::CmpTrue);
     
    EXPECT_EQ(tuple3.GetValue(&schema,4).CompareEquals(values[4]), CmpBool::CmpTrue);
    // ++itr;
 

  // LOG_DEBUG("%d", values[3].GetAs<bool>());
  // // int i = 0;
  // std::shuffle(rid_v.begin(), rid_v.end(), std::default_random_engine(0));
  // for (const auto &rid : rid_v) {
  //   // std::cout << i++ << std::endl;
  //   BUSTUB_ENSURE(table->MarkDelete(rid, transaction) == 1, "");
  // }
  disk_manager->ShutDown();
  remove("test.db");  // remove db file
  remove("test.log");
  delete table;
  delete buffer_pool_manager;
  delete disk_manager;
  delete transaction;
  delete log_manager;
  delete lock_manager;
}



}  // namespace bustub
