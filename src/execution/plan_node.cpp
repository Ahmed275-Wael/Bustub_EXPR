#include <algorithm>
#include <memory>
#include <vector>
#include "binder/table_ref/bound_base_table_ref.h"
#include "catalog/Lcatalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"

namespace bustub {

auto SeqScanPlanNode::InferScanSchema(const BoundBaseTableRef &table) -> Schema {
  std::vector<Column*> schema;
  for (const auto &column : table.schema_.nGetColumns()) {
    auto col_name = fmt::format("{}.{}", table.GetBoundTableName(), column->GetName());
    Column *column1 = new Column{col_name, *column};
    schema.emplace_back(column1);
  }
  return Schema(schema,0);
}

auto NestedLoopJoinPlanNode::InferJoinSchema(const AbstractPlanNode &left, const AbstractPlanNode &right) -> Schema {
  std::vector<Column*> schema;
  for (const auto &column : left.OutputSchema().nGetColumns()) {
    schema.emplace_back(column);
  }
  for (const auto &column : right.OutputSchema().nGetColumns()) {
    schema.emplace_back(column);
  }
  return Schema(schema,0);
}

auto ProjectionPlanNode::InferProjectionSchema(const std::vector<AbstractExpressionRef> &expressions) -> Schema {
  std::vector<Column*> schema;
  for (const auto &expr : expressions) {
    auto type_id = expr->GetReturnType();
    if (type_id != TypeId::VARCHAR) {
      Column *column = new Column("<unnamed>", type_id,0,0,0);
      schema.emplace_back(column);
    } else {
      Column *column = new Column("<unnamed>", type_id, VARCHAR_DEFAULT_LENGTH,0,0,0);
      // TODO(chi): infer the correct VARCHAR length. Maybe it doesn't matter for executors?
      schema.emplace_back(column);
    }
  }
  return Schema(schema,0);
}

auto ProjectionPlanNode::RenameSchema(const Schema &schema, const std::vector<std::string> &col_names) -> Schema {
  std::vector<Column*> output;
  if (col_names.size() != schema.GetColumnCount()) {
    throw bustub::Exception("mismatched number of columns");
  }
  size_t idx = 0;
  for (const auto &column : schema.nGetColumns()) {
    Column *column1 = new Column(col_names[idx++], *column);
    output.emplace_back(column1);
  }
  return Schema(output,0);
}

auto AggregationPlanNode::InferAggSchema(const std::vector<AbstractExpressionRef> &group_bys,
                                         const std::vector<AbstractExpressionRef> &aggregates,
                                         const std::vector<AggregationType> &agg_types) -> Schema {
  std::vector<Column*> output;
  output.reserve(group_bys.size() + aggregates.size());
  for (const auto &column : group_bys) {
    // TODO(chi): correctly process VARCHAR column
    if (column->GetReturnType() == TypeId::VARCHAR) {
      Column *column1 = new Column("<unnamed>", column->GetReturnType(), 128,0,0,0);
      output.emplace_back(column1);
    } else {
      Column *column1 = new Column("<unnamed>", column->GetReturnType(),0,0,0);
      output.emplace_back(column1);
    }
  }
  for (size_t idx = 0; idx < aggregates.size(); idx++) {
    // TODO(chi): correctly infer agg call return type
    Column *column2 = new Column("<unnamed>", TypeId::INTEGER,0,0,0);
    output.emplace_back(column2);
  }
  return Schema(output,0);
}

}  // namespace bustub
