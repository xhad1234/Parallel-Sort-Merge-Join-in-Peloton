//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sortbench_loader.h
//
// Identification: src/include/benchmark/sortbench/sortbench_loader.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "benchmark/sortbench/sortbench_configuration.h"

namespace peloton {
namespace benchmark {
namespace sortbench {

extern configuration state;

void CreateSortBenchDatabase();

void LoadSortBenchDatabase();

}  // namespace sortbench
}  // namespace benchmark
}  // namespace peloton