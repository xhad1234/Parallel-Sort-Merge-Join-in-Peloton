//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// orderbench_workload.h
//
// Identification: src/include/benchmark/orderbench/orderbench_workload.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "benchmark/benchmark_common.h"
#include "benchmark/orderbench/orderbench_config.h"
#include "storage/data_table.h"
#include "executor/abstract_executor.h"

namespace peloton {

namespace storage {
  class DataTable;
}

namespace benchmark {
namespace orderbench {

extern configuration state;

void RunWorkload();

void RunOrderBench();

}  // namespace orderbench
}  // namespace benchmark
}  // namespace peloton