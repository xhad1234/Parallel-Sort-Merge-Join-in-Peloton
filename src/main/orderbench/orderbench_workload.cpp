//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// orderbench_workload.cpp
//
// Identification: src/main/orderbench/orderbench_workload.cpp
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

#include "benchmark/orderbench/orderbench_workload.h"
#include "benchmark/orderbench/orderbench_config.h"
#include "benchmark/orderbench/orderbench_loader.h"

#include "planner/seq_scan_plan.h"
#include "planner/order_by_plan.h"

namespace peloton {
namespace benchmark {
namespace orderbench {

extern storage::DataTable *table;


/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

volatile bool is_running = true;

oid_t *abort_counts;
oid_t *commit_counts;

void RunWorkload() {
  // Execute the workload to build the log
  RunOrderBench();
}

void ValidateOutputLogicalTile(executor::LogicalTile *logical_tile, int& prev_key) {
  PL_ASSERT(logical_tile);

  if (logical_tile->GetColumnCount() != 3) {
    throw Exception("Incorrect number of columns in output tile:" +
                    std::to_string(logical_tile->GetColumnCount()) + ", Expected: 3" );
  }

  // Check the attribute values
  // Go over the tile
  for (auto logical_tile_itr : *logical_tile) {
    const expression::ContainerTuple<executor::LogicalTile> output_tuple(
        logical_tile, logical_tile_itr);
    // Check the join fields
    auto curr_key = output_tuple.GetValue(1).GetAs<int>();
    if (curr_key < prev_key) {
      throw Exception("Keys are not sorted - current:" + std::to_string(curr_key) + " previous:"
                      + std::to_string(prev_key));
    }
    prev_key = curr_key;
  }
}

void RunOrderBench() {
  auto order_start = static_cast<double>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count());

  int result_tuple_count = 0;
  auto &txn_manager =
      concurrency::TransactionManagerFactory::GetInstance();
  // ================================
  //             Plans
  // ================================

  // Create seq scan node on left table
  planner::SeqScanPlan seq_scan_node(table, nullptr, std::vector<oid_t>({0,1,2}));

  std::vector<oid_t> sort_keys({1});
  std::vector<bool> descend_flags({false});
  std::vector<oid_t> output_columns({0, 1, 2});
  planner::OrderByPlan order_node(sort_keys, descend_flags, output_columns);

  // ================================
  //         Executors
  // ================================

  // Create executor context with empty txn
  auto txn = txn_manager.BeginReadonlyTransaction();

  std::unique_ptr<executor::ExecutorContext> seq_scan_context(
      new executor::ExecutorContext(txn));
  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node,
                                                   seq_scan_context.get());
  std::unique_ptr<executor::ExecutorContext> order_context(
      new executor::ExecutorContext(txn));
  executor::OrderByExecutor order_executor(&order_node,
                                                order_context.get());
  order_executor.AddChild(&seq_scan_executor);

  order_executor.Init();
  if (state.use_avx2_sort == true) {
    order_executor.UseAVX2Sort();
  }

  int prev_key = INT_MIN;

  while (order_executor.Execute() == true) {
    std::unique_ptr<executor::LogicalTile> result_logical_tile(
        order_executor.GetOutput());

    if (result_logical_tile != nullptr) {
      result_tuple_count += result_logical_tile->GetTupleCount();
      ValidateOutputLogicalTile(result_logical_tile.get(), prev_key);
      LOG_TRACE("Prev_key:%d", prev_key);
      LOG_TRACE("%s", result_logical_tile->GetInfo().c_str());
    }
  }

  txn_manager.CommitTransaction(txn);

  auto order_end = static_cast<double>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count());
  LOG_ERROR("Order Executor Time: %f", (order_end-order_start)/1000);

  state.execution_time_ms = (order_end-order_start)/1000;
  LOG_INFO("Result_Tuples: %d", result_tuple_count);
  LOG_INFO("Order by query took %ldms", state.execution_time_ms);
}

}  // namespace orderbench
}  // namespace benchmark
}  // namespace peloton