//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sortbench_loader.cpp
//
// Identification: src/main/sortbench/sortbench_loader.cpp
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

#include "benchmark/sortbench/sortbench_loader.h"
#include "benchmark/sortbench/sortbench_config.h"
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
namespace sortbench {

storage::Database *sortbench_database = nullptr;

storage::DataTable *left_table = nullptr;
storage::DataTable *right_table = nullptr;

void CreateSortBenchDatabase() {
  /////////////////////////////////////////////////////////
  // Create database & tables
  /////////////////////////////////////////////////////////
  // Clean up
  delete sortbench_database;
  sortbench_database = nullptr;
  left_table = nullptr;
  right_table = nullptr;

  auto catalog = catalog::Catalog::GetInstance();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  auto l_id_col = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "l_id", true);
  auto l_sortkey_col = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "l_sortkey", true);
  auto l_shipdate_col = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "l_shipdate", true);

  std::unique_ptr<catalog::Schema> left_table_schema(
      new catalog::Schema({l_id_col, l_sortkey_col, l_shipdate_col}));

  auto r_id_col = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "r_id", true);
  auto r_sortkey_col = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "r_sortkey", true);
  auto r_shipdate_col = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "r_shipdate", true);

  std::unique_ptr<catalog::Schema> right_table_schema(
      new catalog::Schema({r_id_col, r_sortkey_col, r_shipdate_col}));


  catalog->CreateDatabase(SORTBENCH_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  // create left table
  txn = txn_manager.BeginTransaction();

  catalog->CreateTable(SORTBENCH_DB_NAME, "LEFT TABLE",
                       std::move(left_table_schema), txn);
  txn_manager.CommitTransaction(txn);
  // create right table
  txn = txn_manager.BeginTransaction();
  catalog->CreateTable(SORTBENCH_DB_NAME, "RIGHT TABLE",
                       std::move(right_table_schema), txn);
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

void LoadSortBenchDatabase() {
  std::unique_ptr<parser::InsertStatement> insert_stmt(nullptr);

  left_table = catalog::Catalog::GetInstance()->GetTableWithName(
      SORTBENCH_DB_NAME, "LEFT TABLE");

  right_table = catalog::Catalog::GetInstance()->GetTableWithName(
      SORTBENCH_DB_NAME, "RIGHT TABLE");

  char *left_table_name_arr = new char[20]();
  strcpy(left_table_name_arr, "LEFT TABLE");
  char *left_db_name_arr = new char[20]();
  strcpy(left_db_name_arr, SORTBENCH_DB_NAME);

  char *right_table_name_arr = new char[20]();
  strcpy(right_table_name_arr, "RIGHT TABLE");
  char *right_db_name_arr = new char[20]();
  strcpy(right_db_name_arr, SORTBENCH_DB_NAME);

  auto *left_table_info = new parser::TableInfo();
  left_table_info->table_name = left_table_name_arr;
  left_table_info->database_name = left_db_name_arr;

  auto *right_table_info = new parser::TableInfo();
  right_table_info->table_name = right_table_name_arr;
  right_table_info->database_name = right_db_name_arr;

  char *l_col_1 = new char[5]();
  strcpy(l_col_1, "l_id");
  char *l_col_2 = new char[10]();
  strcpy(l_col_2, "l_sortkey");
  char *l_col_3 = new char[11]();
  strcpy(l_col_3, "l_shipdate");

  // insert to left table; build an insert statement
  insert_stmt.reset(new parser::InsertStatement(INSERT_TYPE_VALUES));
  insert_stmt->table_info_ = left_table_info;
  insert_stmt->columns = new std::vector<char *>;
  insert_stmt->columns->push_back(const_cast<char *>(l_col_1));
  insert_stmt->columns->push_back(const_cast<char *>(l_col_2));
  insert_stmt->columns->push_back(const_cast<char *>(l_col_3));
  insert_stmt->select = new parser::SelectStatement();
  insert_stmt->insert_values =
      new std::vector<std::vector<expression::AbstractExpression *> *>;


  for (int tuple_id = 0; tuple_id < LEFT_TABLE_SIZE * state.scale_factor; tuple_id++) {
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
      LOG_ERROR("Left Table: Inserted %d out of %d tuples", tuple_id+1,
                LEFT_TABLE_SIZE*state.scale_factor);    }
  }

  char *r_col_1 = new char[5]();
  strcpy(r_col_1, "r_id");
  char *r_col_2 = new char[10]();
  strcpy(r_col_2, "r_sortkey");
  char *r_col_3 = new char[11]();
  strcpy(r_col_3, "r_shipdate");

  // insert to right table; build an insert statement
  insert_stmt.reset(new parser::InsertStatement(INSERT_TYPE_VALUES));
  insert_stmt->table_info_ = right_table_info;
  insert_stmt->columns = new std::vector<char *>;
  insert_stmt->columns->push_back(const_cast<char *>(r_col_1));
  insert_stmt->columns->push_back(const_cast<char *>(r_col_2));
  insert_stmt->columns->push_back(const_cast<char *>(r_col_3));
  insert_stmt->select = new parser::SelectStatement();
  insert_stmt->insert_values =
      new std::vector<std::vector<expression::AbstractExpression *> *>;

  for (int tuple_id = 0; tuple_id < RIGHT_TABLE_SIZE * state.scale_factor; tuple_id++) {
    auto values_ptr = new std::vector<expression::AbstractExpression *>;
    insert_stmt->insert_values->push_back(values_ptr);
    int shipdate = rand() % 60;
    int sortkey = rand() %
        (1 << (sizeof(int32_t)*8 - ORDER_BY_SHIFT_OFFSET));

    values_ptr->push_back(new expression::ConstantValueExpression(
        common::ValueFactory::GetIntegerValue(tuple_id)));
    values_ptr->push_back(new expression::ConstantValueExpression(
        common::ValueFactory::GetIntegerValue(sortkey)));
    values_ptr->push_back(new expression::ConstantValueExpression(
        common::ValueFactory::GetIntegerValue(shipdate)));

    if ((tuple_id + 1) % INSERT_SIZE == 0) {
      LoadHelper(insert_stmt.get());
      LOG_ERROR("Right Table: Inserted %d out of %d tuples", tuple_id+1,
               RIGHT_TABLE_SIZE*state.scale_factor);
    }
  }

  LOG_ERROR("Left table size:%ld", left_table->GetTupleCount());
  LOG_ERROR("Right table size:%ld", right_table->GetTupleCount());
}

}  // namespace sortbench
}  // namespace benchmark
}  // namespace peloton