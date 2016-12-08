//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sortbench_workload.h
//
// Identification: src/include/benchmark/sortbench/sortbench_workload.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "benchmark/benchmark_common.h"
#include "benchmark/sortbench/sortbench_config.h"
#include "storage/data_table.h"
#include "executor/abstract_executor.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace benchmark {
namespace sortbench {

extern configuration state;

void RunSortMergeJoin();

void RunWorkload();

}  // namespace sortbench
}  // namespace benchmark
}  // namespace peloton