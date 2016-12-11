//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// orderbench_loader.h
//
// Identification: src/include/benchmark/orderbench/orderbench_loader.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "benchmark/orderbench/orderbench_config.h"

namespace peloton {
namespace benchmark {
namespace orderbench {

extern configuration state;

void CreateOrderBenchDatabase();

void LoadOrderBenchDatabase();

}  // namespace orderbench
}  // namespace benchmark
}  // namespace peloton