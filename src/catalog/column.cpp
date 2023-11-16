//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// column.cpp
//
// Identification: src/catalog/column.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/column.h"

#include <sstream>
#include <string>

namespace bustub {

auto Column::ToString(bool simplified) const -> std::string {
  if (simplified) {
    std::ostringstream os;
    os << GetName() << ":" << Type::TypeIdToString(GetType());
    return (os.str());
  }

  std::ostringstream os;

  os << "Column[" << GetName() << ", " << Type::TypeIdToString(GetType()) << ", "
     << "Offset:" << GetOffset() << ", ";

  if (IsInlined()) {
    os << "FixedLength:" << GetLength();
  } else {
    os << "VarLength:" << GetLength();
  }
  os << "]";
  return (os.str());
}

}  // namespace bustub
