//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sortbench_workload.cpp
//
// Identification: src/main/sortbench/sortbench_workload.cpp
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
#include <thread>
#include <algorithm>
#include <random>
#include <cstddef>
#include <limits>
#include <include/expression/expression_util.h>

#include "catalog/catalog.h"
#include "common/logger.h"
#include "common/exception.h"
#include "common/container_tuple.h"

#include "executor/plan_executor.h"
#include "executor/seq_scan_executor.h"
#include "executor/order_by_executor.h"
#include "executor/merge_join_executor.h"
#include "expression/conjunction_expression.h"

#include "benchmark/sortbench/sortbench_workload.h"
#include "benchmark/sortbench/sortbench_config.h"
#include "benchmark/sortbench/sortbench_loader.h"

#include "planner/seq_scan_plan.h"
#include "planner/merge_join_plan.h"
#include "planner/order_by_plan.h"

namespace peloton {
namespace benchmark {
namespace sortbench {

extern storage::DataTable *left_table;
extern storage::DataTable *right_table;

/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

volatile bool is_running = true;

oid_t *abort_counts;
oid_t *commit_counts;

void RunWorkload() {
  // Execute the workload to build the log
  RunSortMergeJoin();
}

void ValidateJoinLogicalTile(executor::LogicalTile *logical_tile, int *prev_key) {
  PL_ASSERT(logical_tile);

  if (logical_tile->GetColumnCount() != 4) {
   throw Exception("Incorrect number of columns in join tile:" +
                       std::to_string(logical_tile->GetColumnCount()) + ", Expected: 4" );
  }

  // Check the attribute values
  // Go over the tile
  for (auto logical_tile_itr : *logical_tile) {
    const expression::ContainerTuple<executor::LogicalTile> join_tuple(
        logical_tile, logical_tile_itr);
    // Check the join fields
    common::Value left_tuple_join_attribute_val = (join_tuple.GetValue(0));
    common::Value right_tuple_join_attribute_val = (join_tuple.GetValue(1));
    common::Value cmp = (left_tuple_join_attribute_val.CompareEquals(
        right_tuple_join_attribute_val));
    if (!(cmp.IsNull() || cmp.IsTrue())) {
      throw Exception("Joining attributes do not match");
    }

    auto curr_key = left_tuple_join_attribute_val.GetAs<int>();
    if (curr_key < *prev_key) {
      throw Exception("Keys are not sorted - current:" + std::to_string(curr_key) + " previous:"
                      + std::to_string(*prev_key));
    }
    *prev_key = curr_key;
  }
}

void RunSortMergeJoin() {

  int result_tuple_count = 0;
  auto &txn_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  // Create the plan node
  TargetList target_list;
  DirectMapList direct_map_list;

  /////////////////////////////////////////////////////////
  // PROJECTION 0
  /////////////////////////////////////////////////////////

  // direct map
  direct_map_list.push_back(std::make_pair(0, std::make_pair(0, 1)));
  direct_map_list.push_back(std::make_pair(1, std::make_pair(1, 1)));
  direct_map_list.push_back(std::make_pair(2, std::make_pair(0, 0)));
  direct_map_list.push_back(std::make_pair(3, std::make_pair(1, 0)));
  auto projection = std::unique_ptr<const planner::ProjectInfo>(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));

  expression::TupleValueExpression *left_table_expr =
      new expression::TupleValueExpression(common::Type::INTEGER, 0, 1);
  expression::TupleValueExpression *right_table_expr =
      new expression::TupleValueExpression(common::Type::INTEGER, 1, 1);
  auto predicate = std::unique_ptr<const expression::AbstractExpression> (
      new expression::ComparisonExpression(EXPRESSION_TYPE_COMPARE_EQUAL,
                                          left_table_expr, right_table_expr));

  //schema
  auto l_sortkey_col = catalog::Column(common::Type::INTEGER,
                                       common::Type::GetTypeSize(common::Type::INTEGER),
                                       "l_sortkey", true);
  auto r_sortkey_col = catalog::Column(common::Type::INTEGER,
                                       common::Type::GetTypeSize(common::Type::INTEGER),
                                       "r_sortkey", true);
  auto l_id_col = catalog::Column(common::Type::INTEGER,
                                  common::Type::GetTypeSize(common::Type::INTEGER),
                                  "l_id", true);
  auto r_id_col = catalog::Column(common::Type::INTEGER,
                                  common::Type::GetTypeSize(common::Type::INTEGER),
                                  "r_id", true);
  auto schema = std::shared_ptr<const catalog::Schema>(
      new catalog::Schema({l_sortkey_col, r_sortkey_col, l_id_col, r_id_col}));

  // ================================
  //             Plans
  // ================================

  // Create seq scan node on left table
  planner::SeqScanPlan left_seq_scan_node(left_table, nullptr, std::vector<oid_t>({0,1,2}));

  // Create seq scan node on right table
  planner::SeqScanPlan right_seq_scan_node(right_table, nullptr, std::vector<oid_t>({0,1,2}));

  std::vector<oid_t> sort_keys({1});
  std::vector<bool> descend_flags({false});
  std::vector<oid_t> output_columns({0, 1, 2});
  planner::OrderByPlan left_order_node(sort_keys, descend_flags, output_columns);
  planner::OrderByPlan right_order_node(sort_keys, descend_flags, output_columns);

  std::vector<planner::MergeJoinPlan::JoinClause> join_clauses;
  auto left = expression::ExpressionUtil::TupleValueFactory(
      common::Type::INTEGER, 0, 1);
  auto right = expression::ExpressionUtil::TupleValueFactory(
      common::Type::INTEGER, 1, 1);
  join_clauses.emplace_back(left, right, true);

  // Create merge join plan node.
  planner::MergeJoinPlan merge_join_node(JOIN_TYPE_INNER, std::move(predicate),
                                 std::move(projection), schema, join_clauses);

  // ================================
  //         Executors
  // ================================

  // Start Timer
  auto begin = std::chrono::high_resolution_clock::now();

  // Create executor context with empty txn
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> left_seq_scan_context(
      new executor::ExecutorContext(txn));
  executor::SeqScanExecutor left_seq_scan_executor(&left_seq_scan_node,
                                                   left_seq_scan_context.get());
  std::unique_ptr<executor::ExecutorContext> right_seq_scan_context(
      new executor::ExecutorContext(txn));
  executor::SeqScanExecutor right_seq_scan_executor(&right_seq_scan_node,
                                                    right_seq_scan_context.get());

  std::unique_ptr<executor::ExecutorContext> left_order_context(
      new executor::ExecutorContext(txn));
  executor::OrderByExecutor left_order_executor(&left_order_node,
                                                left_order_context.get());
  std::unique_ptr<executor::ExecutorContext> right_order_context(
      new executor::ExecutorContext(txn));
  executor::OrderByExecutor right_order_executor(&right_order_node,
                                                 right_order_context.get());

  std::unique_ptr<executor::ExecutorContext> merge_join_context(
      new executor::ExecutorContext(txn));
  executor::MergeJoinExecutor merge_join_executor(&merge_join_node,
                                                  merge_join_context.get());

  merge_join_executor.AddChild(&left_order_executor);
  merge_join_executor.AddChild(&right_order_executor);

  left_order_executor.AddChild(&left_seq_scan_executor);
  right_order_executor.AddChild(&right_seq_scan_executor);

  merge_join_executor.Init();

  if (state.use_avx2_sort == true) {
    left_order_executor.UseAVX2Sort();
    right_order_executor.UseAVX2Sort();
  }

  int prev_key = INT_MIN;

  auto merge_start = static_cast<double>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count());

  while (merge_join_executor.Execute() == true) {
    std::unique_ptr<executor::LogicalTile> result_logical_tile(
        merge_join_executor.GetOutput());

    if (result_logical_tile != nullptr) {
      result_tuple_count += result_logical_tile->GetTupleCount();
      ValidateJoinLogicalTile(result_logical_tile.get(), &prev_key);
      LOG_TRACE("Prev_key:%d", prev_key);
      LOG_TRACE("%s", result_logical_tile->GetInfo().c_str());
    }
  }

  auto merge_end = static_cast<double>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count());
  LOG_ERROR("Merge Join Executor Time: %f", (merge_end-merge_start)/1000);

  txn_manager.CommitTransaction(txn);

  // End Time
  auto end = std::chrono::high_resolution_clock::now();

  state.execution_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end-begin).count();
  LOG_INFO("Result_Tuples: %d", result_tuple_count);
  LOG_INFO("Sort Merge Join took %ldms", state.execution_time_ms);
}

}  // namespace sortbench
}  // namespace benchmark
}  // namespace peloton