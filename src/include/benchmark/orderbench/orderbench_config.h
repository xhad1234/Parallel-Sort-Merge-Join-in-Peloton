//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// orderbench_config.h
//
// Identification: src/include/benchmark/orderbench/orderbench_config.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <cstring>
#include <getopt.h>
#include <vector>
#include <sys/time.h>
#include <iostream>

#include "common/types.h"

#define ORDERBENCH_DB_NAME "orderbench_db"

#define INSERT_SIZE 100000

#define TABLE_SIZE 100000


namespace peloton {
namespace benchmark {
namespace orderbench {

static const oid_t orderbench_database_oid = 100;

static const oid_t user_table_oid = 1001;

static const oid_t user_table_pkey_index_oid = 2001;

static const oid_t orderbench_field_length = 100;

class configuration {
public:
  // size of the table
  int scale_factor;

  // use avx2 sort implementation for ORDER BY
  bool use_avx2_sort;

  // time of the sort-merge join in milliseconds
  long execution_time_ms = 0;
};

extern configuration state;

void Usage(FILE *out);

void ParseArguments(int argc, char *argv[], configuration &state);

void ValidateScaleFactor(const configuration &state);

void WriteOutput();

}  // namespace orderbench
}  // namespace benchmark
}  // namespace peloton