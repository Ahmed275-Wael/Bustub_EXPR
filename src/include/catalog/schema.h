//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// schema.h
//
// Identification: src/include/catalog/schema.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>
//#include "common/dberr.h"
#include "catalog/column.h"
#include "common/exception.h"
#include "type/type.h"

namespace bustub {

class Schema;
using SchemaRef = std::shared_ptr<const Schema>;

class Schema {
 public:
  /**
   * Constructs the schema corresponding to the vector of columns, read left-to-right.
   * @param columns columns that describe the schema's individual columns
   */
  explicit Schema(const std::vector<Column> &columns);
  explicit Schema(const std::vector<Column *> columns, int i){
    uint32_t curr_offset = 0;
  for (uint32_t index = 0; index < columns.size(); index++) {
    Column *column = columns[index];
    // handle uninlined column
    if (!column->IsInlined()) {
      tuple_is_inlined_ = false;
      uninlined_columns_.push_back(index);
    }
    // set column offset
    column->column_offset_ = curr_offset;
    curr_offset += column->GetFixedLength();

    // add column
    this->ncolumns_.push_back(column);
  }
  // set tuple length
  length_ = curr_offset;
  }

  // static Schema *DeepCopySchema(const Schema *from, MemHeap *heap) {
  //   std::vector<Column *> cols;
  //   for (uint32_t i = 0; i < from->GetColumnCount(); i++) {
  //     void *buf = heap->Allocate(sizeof(Column));
  //     cols.push_back(new(buf)Column(from->GetColumn(i)));
  //   }
  //   void *buf = heap->Allocate(sizeof(Schema));
  //   return new(buf) Schema(cols, 0);
  // }

  static Schema *CopySchema(const Schema *from, const std::vector<uint32_t> &attrs, MemHeap *heap){
    std::vector<Column*> cols;
    cols.reserve(attrs.size());
    for (const auto i : attrs) {
      cols.emplace_back(from->ncolumns_[i]);
    }
    void *buf = heap->Allocate(sizeof(Schema));
    return new(buf) Schema(cols ,0);
  }
  static Schema lolCopySchema(const Schema *from, const std::vector<uint32_t> &attrs){
    std::vector<Column *> cols;
    cols.reserve(attrs.size());
    for (const auto i : attrs) {
      cols.emplace_back(from->ncolumns_[i]);
    }
    return Schema(cols,0);
  }
  /** @return all the columns in the schema */
  auto GetColumns() const -> const std::vector<Column> & { return columns_; }
  auto nGetColumns() const -> const std::vector<Column*> & { return ncolumns_; }
  /**
   * Returns a specific column from the schema.
   * @param col_idx index of requested column
   * @return requested column
   */
  auto GetColumn(const uint32_t col_idx) const -> const Column& { return columns_[col_idx]; }
  auto nGetColumn(const uint32_t col_idx) const -> const Column* { return ncolumns_[col_idx]; }
  /**
   * Looks up and returns the index of the first column in the schema with the specified name.
   * If multiple columns have the same name, the first such index is returned.
   * @param col_name name of column to look for
   * @return the index of a column with the given name, throws an exception if it does not exist
   */
  auto GetColIdx(const std::string &col_name) const -> uint32_t {
    if (auto col_idx = TryGetColIdx(col_name)) {
      return *col_idx;
    }
    UNREACHABLE("Column does not exist");
  }
  auto GetColumnIndex(const std::string &col_name, uint32_t &index) const-> uint32_t {
    for (uint32_t i = 0; i < ncolumns_.size(); ++i) {
      if (ncolumns_[i]->GetName() == col_name) {
        index = i;
        return index;
      }
    }
    UNREACHABLE("Column does not exist");;
  }
  /**
   * Looks up and returns the index of the first column in the schema with the specified name.
   * If multiple columns have the same name, the first such index is returned.
   * @param col_name name of column to look for
   * @return the index of a column with the given name, `std::nullopt` if it does not exist
   */
  auto TryGetColIdx(const std::string &col_name) const -> std::optional<uint32_t> {
    for (uint32_t i = 0; i < columns_.size(); ++i) {
      if (columns_[i].GetName() == col_name) {
        return std::optional{i};
      }
    }
    return std::nullopt;
  }
  uint32_t SerializeTo(char *buf) const{
      uint32_t ofs = 0;
      std::vector<Column *> columns_ = this->nGetColumns();
      
      //1.Write the Magic Number
      MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM);
      ofs += sizeof(uint32_t);

      //2.Write the size of the columns
      MACH_WRITE_UINT32(buf + ofs, (columns_.size()));
      ofs += sizeof(uint32_t);

      //3.Write the Columns the into the buf
      for (uint32_t i = 0; i < columns_.size(); i++) {
        //Write the Serialized Size of the Each column
        MACH_WRITE_UINT32(buf + ofs, (columns_[i]->GetSerializedSize()));
        ofs += sizeof(uint32_t);
        //Write the Serialized Column into the buf
        columns_[i]->SerializeTo(buf + ofs);
        ofs += columns_[i]->GetSerializedSize();
      }
      
      return ofs;
}

  /**
   * Only used in table
   */
  uint32_t GetSerializedSize() const{
  std::vector<Column *> columns_ = this->nGetColumns();
  uint32_t LengthOfTable = columns_.size();
  uint32_t Size = 0;
  //1.Calculate the Magic Number and SizeOf(Columns)
  Size += 2 * sizeof(uint32_t);
  //2.Calculate the Total Column
  for (uint32_t i = 0; i < LengthOfTable; i++) {
    //The SerializedSize
    Size += sizeof(uint32_t);
    Size += columns_[i]->GetSerializedSize();
  }

  return Size;
}

  /**
   * Only used in table
   */
  static uint32_t DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap){
  // First if buf is nullptr, then nothing to deserialize from. And the returned offset is 0 as well.
  if (buf == nullptr) return 0;

  // Do the actual deserialization work.
  uint32_t ofs = 0;
  std::vector<Column *> columns_; // Which will be used to construct the schema

  // 1. Read the Magic_Number
  uint32_t Magic_Number = MACH_READ_FROM(uint32_t, (buf));
  ofs += sizeof(uint32_t);
  
  // If does not match---Error
  BUSTUB_ASSERT(Magic_Number == 200715, "MagicNumber does not match in schema deserialization");
  /** do the check of Magic_number.
  if (Magic_Number != 200715) {
    std::cerr << "MagicNumber does not match" << std::endl;
  }
  */
  // 2. Read the SizeOfColumns From the buf
  uint32_t LengthOfTable = MACH_READ_FROM(uint32_t, (buf + ofs));
  ofs += sizeof(uint32_t);
  // 3. Read the Columns in the Schema
  
  for (uint32_t i = 0; i < LengthOfTable; i++) {
    ofs += sizeof(uint32_t); // read the size of attributes out (actually redundant.)
    Column *tmp = nullptr;
    ofs += Column::DeserializeFrom(buf + ofs, tmp, heap);
    columns_.push_back(tmp);
    //schema->length_ += tmp->GetSerializedSize();
  }
  //setLength(length);
  void *mem = heap->Allocate(sizeof(Schema));
  schema = new (mem)Schema(columns_ , 0);
  return ofs;
}
  /** @return the indices of non-inlined columns */
  auto GetUnlinedColumns() const -> const std::vector<uint32_t> & { return uninlined_columns_; }

  /** @return the number of columns in the schema for the tuple */
  auto GetColumnCount() const -> uint32_t { return static_cast<uint32_t>(ncolumns_.size()); }

  /** @return the number of non-inlined columns */
  auto GetUnlinedColumnCount() const -> uint32_t { return static_cast<uint32_t>(uninlined_columns_.size()); }

  /** @return the number of bytes used by one tuple */
  inline auto GetLength() const -> uint32_t { return length_; }

  //inline auto setLength(uint32_t length) const -> uint32_t { tlength_ = length; }
  /** @return true if all columns are inlined, false otherwise */
  inline auto IsInlined() const -> bool { return tuple_is_inlined_; }

  /** @return string representation of this schema */
  auto ToString(bool simplified = true) const -> std::string;
  
 private:
  /** Fixed-length column size, i.e. the number of bytes used by one tuple. */
  
  static constexpr uint32_t SCHEMA_MAGIC_NUM = 200715;
  uint32_t length_;
  /** All the columns in the schema, inlined and uninlined. */
  std::vector<Column> columns_;
  std::vector<Column*> ncolumns_;
  /** True if all the columns are inlined, false otherwise. */
  bool tuple_is_inlined_{true};

  /** Indices of all uninlined columns. */
  std::vector<uint32_t> uninlined_columns_;
};
using IndexSchema = Schema;
using TableSchema = Schema; 

}  // namespace bustub

template <typename T>
struct fmt::formatter<T, std::enable_if_t<std::is_base_of<bustub::Schema, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const bustub::Schema &x, FormatCtx &ctx) const {
    return fmt::formatter<std::string>::format(x.ToString(), ctx);
  }
};

template <typename T>
struct fmt::formatter<std::shared_ptr<T>, std::enable_if_t<std::is_base_of<bustub::Schema, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const std::shared_ptr<T> &x, FormatCtx &ctx) const {
    if (x != nullptr) {
      return fmt::formatter<std::string>::format(x->ToString(), ctx);
    }
    return fmt::formatter<std::string>::format("", ctx);
  }
};

template <typename T>
struct fmt::formatter<std::unique_ptr<T>, std::enable_if_t<std::is_base_of<bustub::Schema, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const std::unique_ptr<T> &x, FormatCtx &ctx) const {
    if (x != nullptr) {
      return fmt::formatter<std::string>::format(x->ToString(), ctx);
    }
    return fmt::formatter<std::string>::format("", ctx);
  }
};
