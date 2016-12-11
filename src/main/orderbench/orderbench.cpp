//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// orderbench.cpp
//
// Identification: src/main/orderbench/orderbench.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//w

#include <iostream>
#include <fstream>
#include <iomanip>

#include "common/init.h"
#include "common/logger.h"

#include "concurrency/epoch_manager_factory.h"

#include "benchmark/orderbench/orderbench_config.h"
#include "benchmark/orderbench/orderbench_loader.h"
#include "benchmark/orderbench/orderbench_workload.h"

#include "gc/gc_manager_factory.h"

namespace peloton {
namespace benchmark {
namespace orderbench {

configuration state;

// Main Entry Point
void RunBenchmark() {
  PelotonInit::Initialize();

  // Create the database
  CreateOrderBenchDatabase();

  // Load the databases
  LoadOrderBenchDatabase();

  RunWorkload();

  // Run the workload, 3 trials
  //  for (int i=0; i<4; i++) {
  //    if (i == 0) {
  //      RunSingleTupleSelectivityScan(dummy);
  //      Run1pcSelectivityScan(dummy);
  //      Run10pcSelectivityScan(dummy);
  //      Run50pcSelectivityScan(dummy);
  //    } else {
  //      ostream << "\n\nSingle Tuple Trial " << i;
  //      RunSingleTupleSelectivityScan(ostream);
  //      ostream << "\n\n1% Selectivity Trial " << i;
  //      Run1pcSelectivityScan(ostream);
  //      ostream << "\n\n10% Selectivity Trial " << i;
  //      Run10pcSelectivityScan(ostream);
  //      ostream << "\n\n50% Selectivity Trial " << i;
  //      Run50pcSelectivityScan(ostream);
  //    }
  //  }

  concurrency::EpochManagerFactory::GetInstance().StopEpoch();

  gc::GCManagerFactory::GetInstance().StopGC();

  PelotonInit::Shutdown();

  // Emit throughput
  WriteOutput();
}

}  // namespace orderbench
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {
  peloton::benchmark::orderbench::ParseArguments(argc, argv,
                                                peloton::benchmark::orderbench::state);

  peloton::benchmark::orderbench::RunBenchmark();

  return 0;
}