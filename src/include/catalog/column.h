//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// column.h
//
// Identification: src/include/catalog/column.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "fmt/format.h"

#include "common/exception.h"
#include "common/macros.h"
#include "type/type.h"
#include "utils/mem_heap.h"

namespace bustub {
class AbstractExpression;

class Column {
  friend class Schema;

 public:
  /**
   * Non-variable-length constructor for creating a Column.
   * @param column_name name of the column
   * @param type type of the column
   */
  Column(std::string column_name, TypeId type , uint32_t index, bool nullable, bool unique)
      : column_name_(std::move(column_name)), column_type_(type), fixed_length_(TypeSize(type)) , column_offset_(index),
          nullable_(nullable), unique_(unique) {
    BUSTUB_ASSERT(type != TypeId::VARCHAR, "Wrong constructor for VARCHAR type.");
  }
  
  /**
   * Variable-length constructor for creating a Column.
   * @param column_name name of the column
   * @param type type of column
   * @param length length of the varlen
   * @param expr expression used to create this column
   */
  Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
      : column_name_(std::move(column_name)),
        column_type_(type),
        fixed_length_(TypeSize(type)),
        variable_length_(length) , column_offset_(index),
          nullable_(nullable), unique_(unique) {
   // BUSTUB_ASSERT(type == TypeId::VARCHAR, "Wrong constructor for non-VARCHAR type.");
  }

  /**
   * Replicate a Column with a different name.
   * @param column_name name of the column
   * @param column the original column
   */
  Column(std::string column_name, const Column &column)
      : column_name_(std::move(column_name)),
        column_type_(column.column_type_),
        fixed_length_(column.fixed_length_),
        variable_length_(column.variable_length_),
        column_offset_(column.column_offset_) {}

  /** @return column name */
  auto GetName() const -> std::string { return column_name_; }

  /** @return column length */
  auto GetLength() const -> uint32_t {
    if (IsInlined()) {
      return fixed_length_;
    }
    return variable_length_;
  }

  /** @return column fixed length */
  auto GetFixedLength() const -> uint32_t { return fixed_length_; }

  /** @return column variable length */
  auto GetVariableLength() const -> uint32_t { return variable_length_; }

  /** @return column's offset in the tuple */
  auto GetOffset() const -> uint32_t { return column_offset_; }

  /** @return column type */
  auto GetType() const -> TypeId { return column_type_; }

  /** @return true if column is inlined, false otherwise */
  auto IsInlined() const -> bool { return column_type_ != TypeId::VARCHAR; }

  /** @return a string representation of this column */
  auto ToString(bool simplified = true) const -> std::string;

  bool IsNullable() const { return nullable_; }

  bool IsUnique() const { return unique_; }

  void SetNullable(bool state) { nullable_ = state; };

  void SetUnique(bool state) { unique_ = state; };

  uint32_t SerializeTo(char *buf) const {

      uint32_t ofs=0;

      //1. Write the MagicNum
      MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
      ofs += sizeof(uint32_t);

      //2. Write the length for the Name
      MACH_WRITE_UINT32(buf+ofs, this->column_name_.length());
      ofs += sizeof(uint32_t);

      //*3. Write the string name to the buf (-> this is a string)
      MACH_WRITE_STRING(buf+ofs, this->column_name_);
      ofs += this->column_name_.length();

      //4. Write the type_ to the buf
      MACH_WRITE_TO(TypeId, (buf+ofs), (this->column_type_));
      ofs += sizeof(TypeId);

      //5. Write the len_ to the buf
      MACH_WRITE_UINT32(buf+ofs, this->GetLength());
      ofs += sizeof(uint32_t);

      //6. Write the table_ind_
      MACH_WRITE_UINT32(buf+ofs, this->column_offset_);
      ofs += sizeof(uint32_t);

      //7. Write the nullable_ to the buf
      MACH_WRITE_BOOL(buf+ofs, this->nullable_);
      ofs += sizeof(bool);

      //8. Write the unique_ to the buf
      MACH_WRITE_BOOL(buf + ofs, this->unique_);
      ofs += sizeof(bool);

      return ofs;
};

  uint32_t GetSerializedSize() const{ // calculate the serializedSize of column, maybe used in the upper level estimation.
  uint32_t ofs=0;
  if(this->column_name_.length()==0)
  {
    return 0;
    // The Column does not have a name, which means that the column does not exist actually. 
    // -> this require the upper level calling to this function must keep the rule that the attribute must have a name.
  }
  else
  {
    ofs = sizeof(uint32_t) * 4 + sizeof(bool) * 2 + sizeof(TypeId);
    ofs += this->column_name_.length();
  }
  return ofs;
};

static uint32_t DeserializeFrom(char *buf, Column *&column, MemHeap *heap){
  if (column != nullptr) {
    // std::cerr << "Pointer to column is not null in column deserialize." << std::endl;
  } // a warning of covering original column storage in memory. -> Maybe need to comment away for upper level transparent.
  if(buf==NULL) return 0; // nothing to deserialize from. 
  
  /* deserialize field from buf */

  //1.Read the Magic_Number
  uint32_t Magic_Number = MACH_READ_UINT32(buf);
  BUSTUB_ASSERT(Magic_Number == 210928, "COLUMN_MAGIC_NUM does not match"); // check using assert. -> This will automatically stop the program.
  /** A equivalent expression of using ASSERT macro. This is going to report a wrong message, but keep running the caller routine.
  if(Magic_Number!=210928)
  {
    std::cerr<<"COLUMN_MAGIC_NUM does not match"<<endl;
    return 0;
  }
  */
  buf += sizeof(uint32_t); // refresh buf to another member storage.

  //2.Read the length of the name_
  uint32_t length = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  //3.Read the Name from the buf
  std::string column_name;
  for(uint32_t i=0;i < length;i++)
  {
    column_name.push_back(buf[i]);
  }
  buf += length; // the storage of string is compact, so just add the length is OK.

  //4.Read the type
  TypeId type=MACH_READ_FROM(TypeId, (buf));
  buf += sizeof(TypeId);

  //5.Read the len_
  uint32_t len_ = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  //6.Read the col_ind
  uint32_t col_ind = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  //7.Read the nullable
  bool nullable=MACH_READ_FROM(bool,(buf));
  buf += sizeof(bool);

  //8.Read the unique
  bool unique=MACH_READ_FROM(bool,(buf));
  buf += sizeof(bool);

  // can be replaced by: 
  //		ALLOC_P(heap, Column)(column_name, type, col_ind, nullable, unique);
  
  void *mem = heap->Allocate(sizeof(Column));
  if (type != VARCHAR) {
    // type is the int or float
    column = new (mem) Column(column_name, type, col_ind, nullable, unique);
  } else {
    column = new (mem) Column(column_name, type, len_, col_ind, nullable, unique);
  }
  
  return sizeof(uint32_t) * 4 + sizeof(bool) * 2 + sizeof(TypeId) + length;
};

 private:
  /**
   * Return the size in bytes of the type.
   * @param type type whose size is to be determined
   * @return size in bytes
   */
  static auto TypeSize(TypeId type) -> uint8_t {
    switch (type) {
      case TypeId::BOOLEAN:
      case TypeId::TINYINT:
        return 1;
      case TypeId::SMALLINT:
        return 2;
      case TypeId::INTEGER:
        return 4;
      case TypeId::BIGINT:
      case TypeId::DECIMAL:
      case TypeId::TIMESTAMP:
        return 8;
      case TypeId::VARCHAR:
        // TODO(Amadou): Confirm this.
        return 12;
      default: {
        UNREACHABLE("Cannot get size of invalid type");
      }
    }
  }
  static constexpr uint32_t COLUMN_MAGIC_NUM = 210928;
  /** Column name. */
  std::string column_name_;

  /** Column value's type. */
  TypeId column_type_;

  /** For a non-inlined column, this is the size of a pointer. Otherwise, the size of the fixed length column. */
  uint32_t fixed_length_;

  /** For an inlined column, 0. Otherwise, the length of the variable length column. */
  uint32_t variable_length_{0};

  /** Column offset in the tuple. */
  uint32_t column_offset_{0};

  bool nullable_{false};  // whether the column can be null
  bool unique_{false}; 
};

}  // namespace bustub

template <typename T>
struct fmt::formatter<T, std::enable_if_t<std::is_base_of<bustub::Column, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const bustub::Column &x, FormatCtx &ctx) const {
    return fmt::formatter<std::string>::format(x.ToString(), ctx);
  }
};

template <typename T>
struct fmt::formatter<std::unique_ptr<T>, std::enable_if_t<std::is_base_of<bustub::Column, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const std::unique_ptr<bustub::Column> &x, FormatCtx &ctx) const {
    return fmt::formatter<std::string>::format(x->ToString(), ctx);
  }
};
