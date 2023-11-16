#include <optional>
#include <shared_mutex>
#include <string>
#include <tuple>

#include "binder/binder.h"
#include "binder/bound_expression.h"
#include "binder/bound_statement.h"
#include "binder/statement/create_statement.h"
#include "binder/statement/explain_statement.h"
#include "binder/statement/index_statement.h"
#include "binder/statement/select_statement.h"
#include "binder/statement/set_show_statement.h"
#include "buffer/buffer_pool_manager_instance.h"
#include "catalog/schema.h"
#include "catalog/table_generator.h"
#include "common/bustub_instance.h"
#include "common/enums/statement_type.h"
#include "common/exception.h"
#include "common/util/string_util.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/execution_engine.h"
#include "execution/executor_context.h"
#include "execution/executors/mock_scan_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "optimizer/optimizer.h"
#include "planner/planner.h"
#include "recovery/checkpoint_manager.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "type/value_factory.h"

namespace bustub {

auto BustubInstance::MakeExecutorContext(Transaction *txn) -> std::unique_ptr<ExecutorContext> {
  return std::make_unique<ExecutorContext>(txn, catalog_, buffer_pool_manager_, txn_manager_, lock_manager_);
}

BustubInstance::BustubInstance(const std::string &db_file_name) {
  enable_logging = false;

  // Storage related.
  disk_manager_ = new DiskManager(db_file_name);

  // Log related.
  log_manager_ = new LogManager(disk_manager_);

  // We need more frames for GenerateTestTable to work. Therefore, we use 128 instead of the default
  // buffer pool size specified in `config.h`.
  bool init = disk_manager_->GetInit();

  try {
    buffer_pool_manager_ = new BufferPoolManagerInstance(128, disk_manager_, LRUK_REPLACER_K, log_manager_);
  } catch (NotImplementedException &e) {
    std::cerr << "BufferPoolManager is not implemented, only mock tables are supported." << std::endl;
    buffer_pool_manager_ = nullptr;
  }

  // Transaction (txn) related.
  lock_manager_ = new LockManager();
  txn_manager_ = new TransactionManager(lock_manager_, log_manager_);

  
  // Checkpoint related.
  checkpoint_manager_ = new CheckpointManager(txn_manager_, log_manager_, buffer_pool_manager_);

  // Catalog.
  catalog_ = new CatalogManager(buffer_pool_manager_, lock_manager_, log_manager_, init);
  std::cerr<<init;
  if (init) {
    //   BUSTUB_ASSERT(bpm_->IsPageFree(CATALOG_META_PAGE_ID), "Catalog meta page not free.");
    //   BUSTUB_ASSERT(bpm_->IsPageFree(INDEX_ROOTS_PAGE_ID), "Header page not free.");
    ///LOG_DEBUG("here");
    page_id_t id ;

    // BUSTUB_ASSERT(buffer_pool_manager_->NewPage(&id) != nullptr && id == CATALOG_META_PAGE_ID, "Failed to allocate catalog meta page.");
    
    // buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
  
    BUSTUB_ASSERT(buffer_pool_manager_->NewPage(&id) != nullptr && id == HEADER_PAGE_ID, "Failed to allocate header page.");
    
    buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, false);
      
    }

  // Execution engine.
  execution_engine_ = new ExecutionEngine(buffer_pool_manager_, txn_manager_, catalog_);
}

BustubInstance::BustubInstance() {
  enable_logging = false;

  // Storage related.
  disk_manager_ = new DiskManagerUnlimitedMemory();

  // Log related.
  log_manager_ = new LogManager(disk_manager_);

  // We need more frames for GenerateTestTable to work. Therefore, we use 128 instead of the default
  // buffer pool size specified in `config.h`.
  try {
    buffer_pool_manager_ = new BufferPoolManagerInstance(128, disk_manager_, LRUK_REPLACER_K, log_manager_);
  } catch (NotImplementedException &e) {
    std::cerr << "BufferPoolManager is not implemented, only mock tables are supported." << std::endl;
    buffer_pool_manager_ = nullptr;
  }

  // Transaction (txn) related.
  lock_manager_ = new LockManager();
  txn_manager_ = new TransactionManager(lock_manager_, log_manager_);

  // Checkpoint related.
  checkpoint_manager_ = new CheckpointManager(txn_manager_, log_manager_, buffer_pool_manager_);

  // Catalog.
  catalog_ = new CatalogManager(buffer_pool_manager_, lock_manager_, log_manager_, true);

  // Execution engine.
  execution_engine_ = new ExecutionEngine(buffer_pool_manager_, txn_manager_, catalog_);
}

void BustubInstance::CmdDisplayTables(ResultWriter &writer) {
  std::vector<std::string> table_names; 
  catalog_->GetAllTableNames(table_names);
  writer.BeginTable(false);
  writer.BeginHeader();
  writer.WriteHeaderCell("oid");
  writer.WriteHeaderCell("name");
  writer.WriteHeaderCell("cols");
  writer.EndHeader();
  for (const auto &name : table_names) {
    writer.BeginRow();
    TableInfo *table_info = nullptr;
    catalog_->GetTable(name,table_info);
    writer.WriteCell(fmt::format("{}", table_info->GetTableId()));
    writer.WriteCell(table_info->GetTableName());
    writer.WriteCell(table_info->GetSchema()->ToString());
    writer.EndRow();
  }
  writer.EndTable();
}

void BustubInstance::CmdDisplayIndices(ResultWriter &writer) {
  std::vector<std::string> table_names; 
  catalog_->GetAllTableNames(table_names);
  writer.BeginTable(false);
  writer.BeginHeader();
  writer.WriteHeaderCell("table_name");
  writer.WriteHeaderCell("index_oid");
  writer.WriteHeaderCell("index_name");
  writer.WriteHeaderCell("index_cols");
  writer.EndHeader();
  for (const auto &table_name : table_names) {
    std::vector<Index_Info *> indexes;
    catalog_->GetTableIndexes(table_name, indexes);
    for (auto *index_info : indexes) {
      writer.BeginRow();
      writer.WriteCell(table_name);
      writer.WriteCell(fmt::format("{}", index_info->GetMetaData()->GetIndexId()));
      writer.WriteCell(index_info->GetIndexName());
      writer.WriteCell(index_info->GetIndexKeySchema()->ToString());
      writer.EndRow();
    }
  }
  writer.EndTable();
}

void BustubInstance::WriteOneCell(const std::string &cell, ResultWriter &writer) {
  writer.BeginTable(true);
  writer.BeginRow();
  writer.WriteCell(cell);
  writer.EndRow();
  writer.EndTable();
}

void BustubInstance::CmdDisplayHelp(ResultWriter &writer) {
  std::string help = R"(Welcome to the BusTub shell!

\dt: show all tables
\di: show all indices
\help: show this message again

BusTub shell currently only supports a small set of Postgres queries. We'll set
up a doc describing the current status later. It will silently ignore some parts
of the query, so it's normal that you'll get a wrong result when executing
unsupported SQL queries. This shell will be able to run `create table` only
after you have completed the buffer pool manager. It will be able to execute SQL
queries after you have implemented necessary query executors. Use `explain` to
see the execution plan of your query.
)";
  WriteOneCell(help, writer);
}

auto BustubInstance::ExecuteSql(const std::string &sql, ResultWriter &writer) -> bool {
  auto txn = txn_manager_->Begin();
  auto result = ExecuteSqlTxn(sql, writer, txn);
  txn_manager_->Commit(txn);
  delete txn;
  return result;
}

auto BustubInstance::ExecuteSqlTxn(const std::string &sql, ResultWriter &writer, Transaction *txn) -> bool {
  if (!sql.empty() && sql[0] == '\\') {
    // Internal meta-commands, like in `psql`.
    if (sql == "\\dt") {
      CmdDisplayTables(writer);
      return true;
    }
    if (sql == "\\di") {
      CmdDisplayIndices(writer);
      return true;
    }
    if (sql == "\\help") {
      CmdDisplayHelp(writer);
      return true;
    }
    throw Exception(fmt::format("unsupported internal command: {}", sql));
  }

  bool is_successful = true;

  std::shared_lock<std::shared_mutex> l(catalog_lock_);
  bustub::Binder binder(*catalog_);
  binder.ParseAndSave(sql);
  l.unlock();

  for (auto *stmt : binder.statement_nodes_) {
    auto statement = binder.BindStatement(stmt);
    switch (statement->type_) {
      case StatementType::CREATE_STATEMENT: {
        const auto &create_stmt = dynamic_cast<const CreateStatement &>(*statement);

        std::unique_lock<std::shared_mutex> l(catalog_lock_);
        TableInfo *table_info = nullptr;
        Schema *schema__ = new Schema(create_stmt.columns_,0);
        catalog_->CreateTable(create_stmt.table_, schema__, txn, table_info);
        l.unlock();

        if (table_info == nullptr) {
          throw bustub::Exception("Failed to create table");
        }
        WriteOneCell(fmt::format("Table created with id = {}", table_info->GetTableId()), writer);
        continue;
      }
      case StatementType::INDEX_STATEMENT: {
        const auto &index_stmt = dynamic_cast<const IndexStatement &>(*statement);

        std::vector<std::string> col_names;
        std::vector<uint32_t> col_ids;
        for (const auto &col : index_stmt.cols_) {
          uint32_t lol;
          auto idx = index_stmt.table_->schema_.GetColumnIndex(col->col_name_.back(),lol);
          col_names.push_back(col->col_name_.back());
          col_ids.push_back(idx);
          if (index_stmt.table_->schema_.GetColumn(idx).GetType() != TypeId::INTEGER) {
            throw NotImplementedException("only support creating index on integer column");
          }
        }
        if (col_ids.size() != 1) {
          throw NotImplementedException("only support creating index with exactly one column");
        }
        auto key_schema = Schema::lolCopySchema(&index_stmt.table_->schema_, col_ids);

        std::unique_lock<std::shared_mutex> l(catalog_lock_);
        Index_Info *index_info = nullptr;
         catalog_->CreateIndex(index_stmt.table_->table_,
             index_stmt.index_name_, col_names,txn,index_info);
        l.unlock();

        if (index_info == nullptr) {
          throw bustub::Exception("Failed to create index");
        }
        WriteOneCell(fmt::format("Index created with id = {}", index_info->GetMetaData()->GetIndexId()), writer);
        continue;
      }
      case StatementType::VARIABLE_SHOW_STATEMENT: {
        const auto &show_stmt = dynamic_cast<const VariableShowStatement &>(*statement);
        auto content = GetSessionVariable(show_stmt.variable_);
        WriteOneCell(fmt::format("{}={}", show_stmt.variable_, content), writer);
        continue;
      }
      case StatementType::VARIABLE_SET_STATEMENT: {
        const auto &set_stmt = dynamic_cast<const VariableSetStatement &>(*statement);
        session_variables_[set_stmt.variable_] = set_stmt.value_;
        continue;
      }
      case StatementType::EXPLAIN_STATEMENT: {
        const auto &explain_stmt = dynamic_cast<const ExplainStatement &>(*statement);
        std::string output;

        // Print binder result.
        if ((explain_stmt.options_ & ExplainOptions::BINDER) != 0) {
          output += "=== BINDER ===";
          output += "\n";
          output += explain_stmt.statement_->ToString();
          output += "\n";
        }

        std::shared_lock<std::shared_mutex> l(catalog_lock_);
        bustub::Planner planner(catalog_);
        planner.PlanQuery(*explain_stmt.statement_);

        bool show_schema = (explain_stmt.options_ & ExplainOptions::SCHEMA) != 0;

        // Print planner result.
        if ((explain_stmt.options_ & ExplainOptions::PLANNER) != 0) {
          output += "=== PLANNER ===";
          output += "\n";
          output += planner.plan_->ToString(show_schema);
          output += "\n";
        }

        // Print optimizer result.
        bustub::Optimizer optimizer(catalog_, IsForceStarterRule());
        auto optimized_plan = optimizer.Optimize(planner.plan_);

        l.unlock();

        if ((explain_stmt.options_ & ExplainOptions::OPTIMIZER) != 0) {
          output += "=== OPTIMIZER ===";
          output += "\n";
          output += optimized_plan->ToString(show_schema);
          output += "\n";
        }

        WriteOneCell(output, writer);

        continue;
      }
      default:
        break;
    }

    std::shared_lock<std::shared_mutex> l(catalog_lock_);

    // Plan the query.
    bustub::Planner planner(catalog_);
    planner.PlanQuery(*statement);

    // Optimize the query.
    bustub::Optimizer optimizer(catalog_, IsForceStarterRule());
    auto optimized_plan = optimizer.Optimize(planner.plan_);

    l.unlock();

    // Execute the query.
    auto exec_ctx = MakeExecutorContext(txn);
    std::vector<Tuple> result_set{};
    is_successful &= execution_engine_->Execute(optimized_plan, &result_set, txn, exec_ctx.get());

    // Return the result set as a vector of string.
    auto schema = planner.plan_->OutputSchema();

    // Generate header for the result set.
    writer.BeginTable(false);
    writer.BeginHeader();
    for (const auto &column : schema.GetColumns()) {
      writer.WriteHeaderCell(column.GetName());
    }
    writer.EndHeader();

    // Transforming result set into strings.
    for (const auto &tuple : result_set) {
      writer.BeginRow();
      for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
        writer.WriteCell(tuple.GetValue(&schema, i).ToString());
      }
      writer.EndRow();
    }
    writer.EndTable();
  }

  return is_successful;
}

/**
 * FOR TEST ONLY. Generate test tables in this BusTub instance.
 * It's used in the shell to predefine some tables, as we don't support
 * create / drop table and insert for now. Should remove it in the future.
 */
void BustubInstance::GenerateTestTable() {
  auto txn = txn_manager_->Begin();
  auto exec_ctx = MakeExecutorContext(txn);
  TableGenerator gen{exec_ctx.get()};

  std::shared_lock<std::shared_mutex> l(catalog_lock_);
  gen.GenerateTestTables();
  l.unlock();

  txn_manager_->Commit(txn);
  delete txn;
}

/**
 * FOR TEST ONLY. Generate test tables in this BusTub instance.
 * It's used in the shell to predefine some tables, as we don't support
 * create / drop table and insert for now. Should remove it in the future.
 */
void BustubInstance::GenerateMockTable() {
  // The actual content generated by mock scan executors are described in `mock_scan_executor.cpp`.
  auto txn = txn_manager_->Begin();

  std::shared_lock<std::shared_mutex> l(catalog_lock_);
  for (auto table_name = &mock_table_list[0]; *table_name != nullptr; table_name++) {
    TableInfo *table_info = nullptr;
    Schema mock_schema = GetMockTableSchemaOf(*table_name);
    Schema *mock_schema_ptr = &mock_schema;
    catalog_->CreateTable(*table_name, mock_schema_ptr, txn, table_info);
  }
  l.unlock();

  txn_manager_->Commit(txn);
  delete txn;
}

BustubInstance::~BustubInstance() {
  if (enable_logging) {
    log_manager_->StopFlushThread();
  }
  delete execution_engine_;
  delete catalog_;
  delete checkpoint_manager_;
  delete log_manager_;
  delete buffer_pool_manager_;
  delete lock_manager_;
  delete txn_manager_;
  delete disk_manager_;
}

}  // namespace bustub
