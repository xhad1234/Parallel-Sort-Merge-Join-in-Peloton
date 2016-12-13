//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sortbench_config.cpp
//
// Identification: src/main/sortbench/sortbench_config.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iomanip>
#include <algorithm>
#include <iostream>
#include <fstream>

#include "benchmark/sortbench/sortbench_config.h"
#include "common/logger.h"

namespace peloton {
namespace benchmark {
namespace sortbench {

void Usage(FILE *out) {
  fprintf(out,
          "Command line options : orderbench <options> \n"
              "   -h --help              :  print help message \n"
              "   -s --scale_factor      :  # of K tuples (default: 1)\n"
              "   -a --avx2              :  Use AVX2 implementation of sort\n");
}

static struct option opts[] = {
    {"scale_factor", optional_argument, NULL, 's'},
    {"avx2", optional_argument, NULL, 'a'},
    {NULL, 0, NULL, 0}};

void ValidateScaleFactor(const configuration &state) {
  if (state.scale_factor <= 0) {
    LOG_ERROR("Invalid scale_factor :: %d", state.scale_factor);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %d", "scale_factor", state.scale_factor);
}

void ParseArguments(int argc, char *argv[], configuration &state) {
  // Default Values
  state.scale_factor = 1;
  state.use_avx2_sort = false;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "has:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'h':
        Usage(stderr);
        exit(EXIT_FAILURE);
        break;
      case 'a':
        state.use_avx2_sort = true;
        break;
      case 's':
        state.scale_factor = atoi(optarg);
        break;
      default:
      LOG_ERROR("Unknown option: -%c-", c);
        Usage(stderr);
        exit(EXIT_FAILURE);
        break;
    }
  }
  // Print configuration
  ValidateScaleFactor(state);
}

void WriteOutput() {
  std::ofstream out("outputfile.summary",
                    std::ios_base::app | std::ios_base::out);

  LOG_INFO("----------------------------------------------------------");
  LOG_INFO("%d %s :: %ldms", state.scale_factor,
           state.use_avx2_sort ? "AVX2 enabled" : "AVX2 disabled",
           state.execution_time_ms);

  out << state.scale_factor << " ";
  out << state.execution_time_ms << " ";

  out.flush();
  out.close();
}

}  // namespace sortbench
}  // namespace benchmark
}  // namespace peloton