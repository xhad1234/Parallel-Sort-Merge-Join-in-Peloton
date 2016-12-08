//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sortbench.cpp
//
// Identification: src/main/sortbench/sortbench.cpp
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

#include "benchmark/sortbench/sortbench_config.h"
#include "benchmark/sortbench/sortbench_loader.h"
#include "benchmark/sortbench/sortbench_workload.h"

#include "gc/gc_manager_factory.h"

namespace peloton {
namespace benchmark {
namespace sortbench {

configuration state;

// Main Entry Point
void RunBenchmark() {
  PelotonInit::Initialize();

  // Create the database
  CreateSortBenchDatabase();

  // Load the databases
  LoadSortBenchDatabase();

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

}  // namespace sortbench
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {
  peloton::benchmark::sortbench::ParseArguments(argc, argv,
                                                peloton::benchmark::sortbench::state);

  peloton::benchmark::sortbench::RunBenchmark();

  return 0;
}