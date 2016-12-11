//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// orderbench_loader.cpp
//
// Identification: src/main/orderbench/orderbench_loader.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>

#include "benchmark/orderbench/orderbench_loader.h"
#include "benchmark/orderbench/orderbench_config.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/abstract_executor.h"
#include "executor/insert_executor.h"
#include "executor/executor_context.h"
#include "executor/executor_tests_util.h"
#include "expression/constant_value_expression.h"
#include "expression/expression_util.h"
#include "index/index_factory.h"
#include "planner/insert_plan.h"
#include "storage/tile.h"
#include "storage/tile_group.h"
#include "storage/data_table.h"
#include "storage/table_factory.h"
#include "storage/database.h"
#include "executor/executor_tests_util.h"

#include "parser/statement_insert.h"

namespace peloton {
namespace benchmark {
namespace orderbench {

storage::Database *orderbench_database = nullptr;

storage::DataTable *table = nullptr;

void CreateOrderBenchDatabase() {
  /////////////////////////////////////////////////////////
  // Create database & tables
  /////////////////////////////////////////////////////////
  // Clean up
  delete orderbench_database;
  orderbench_database = nullptr;
  table = nullptr;

  auto catalog = catalog::Catalog::GetInstance();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  auto id_col = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "id", true);
  auto sortkey_col = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "sortkey", true);
  auto shipdate_col = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "shipdate", true);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_col, sortkey_col, shipdate_col}));

  catalog->CreateDatabase(ORDERBENCH_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  // create left table
  txn = txn_manager.BeginTransaction();

  catalog->CreateTable(ORDERBENCH_DB_NAME, "TABLE",
                       std::move(table_schema), txn);
  txn_manager.CommitTransaction(txn);
}

void LoadHelper(parser::InsertStatement* insert_stmt){
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  planner::InsertPlan node(insert_stmt);

  // start the tasks
  auto txn = txn_manager.BeginTransaction();
  auto context = new executor::ExecutorContext(txn);

  executor::AbstractExecutor* executor = new executor::InsertExecutor(&node, context);
  executor->Execute();

  txn_manager.CommitTransaction(txn);

  for (auto val_list : *insert_stmt->insert_values){
    for (auto val : *val_list){
      delete val;
    }
    delete val_list;
  }

  insert_stmt->insert_values->clear();
}

void LoadOrderBenchDatabase() {
  std::unique_ptr<parser::InsertStatement> insert_stmt(nullptr);

  table = catalog::Catalog::GetInstance()->GetTableWithName(
      ORDERBENCH_DB_NAME, "TABLE");

  char *table_name_arr = new char[20]();
  strcpy(table_name_arr, "TABLE");
  char *db_name_arr = new char[20]();
  strcpy(db_name_arr, ORDERBENCH_DB_NAME);

  auto *table_info = new parser::TableInfo();
  table_info->table_name = table_name_arr;
  table_info->database_name = db_name_arr;

  char *col_1 = new char[5]();
  strcpy(col_1, "id");
  char *col_2 = new char[10]();
  strcpy(col_2, "sortkey");
  char *col_3 = new char[11]();
  strcpy(col_3, "shipdate");

  // insert to left table; build an insert statement
  insert_stmt.reset(new parser::InsertStatement(INSERT_TYPE_VALUES));
  insert_stmt->table_info_ = table_info;
  insert_stmt->columns = new std::vector<char *>;
  insert_stmt->columns->push_back(const_cast<char *>(col_1));
  insert_stmt->columns->push_back(const_cast<char *>(col_2));
  insert_stmt->columns->push_back(const_cast<char *>(col_3));
  insert_stmt->select = new parser::SelectStatement();
  insert_stmt->insert_values =
      new std::vector<std::vector<expression::AbstractExpression *> *>;

  srand(time(nullptr));

  for (int tuple_id = 0; tuple_id < TABLE_SIZE * state.scale_factor; tuple_id++) {
    auto values_ptr = new std::vector<expression::AbstractExpression *>;
    insert_stmt->insert_values->push_back(values_ptr);
    int shipdate = rand() % 60;
    int raw_sortkey = rand();
    int sortkey = raw_sortkey %
                  (1 << (sizeof(int32_t)*8 - ORDER_BY_SHIFT_OFFSET));

    values_ptr->push_back(new expression::ConstantValueExpression(
        common::ValueFactory::GetIntegerValue(tuple_id)));
    values_ptr->push_back(new expression::ConstantValueExpression(
        common::ValueFactory::GetIntegerValue(sortkey)));
    values_ptr->push_back(new expression::ConstantValueExpression(
        common::ValueFactory::GetIntegerValue(shipdate)));

    if ((tuple_id + 1) % INSERT_SIZE == 0) {
      LoadHelper(insert_stmt.get());
      LOG_ERROR("Table: Inserted %d out of %d tuples", tuple_id+1,
                TABLE_SIZE*state.scale_factor);    }
  }

  LOG_ERROR("Table size:%ld", table->GetTupleCount());
}

}  // namespace orderbench
}  // namespace benchmark
}  // namespace peloton