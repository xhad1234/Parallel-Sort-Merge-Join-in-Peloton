//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sortbench_config.h
//
// Identification: src/include/benchmark/sortbench/sortbench_config.h
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

#define SORTBENCH_DB_NAME "sortbench_db"

#define INSERT_SIZE 100000

#define LEFT_TABLE_SIZE 100000

#define RIGHT_TABLE_SIZE 100000

namespace peloton {
namespace benchmark {
namespace sortbench {

// static const oid_t ycsb_database_oid = 100;
static const oid_t sortbench_database_oid = 100;

static const oid_t user_table_oid = 1001;

static const oid_t user_table_pkey_index_oid = 2001;

// static const oid_t ycsb_field_length = 100;
static const oid_t sortbench_field_length = 100;

class configuration {
public:
  // size of the table
  int scale_factor;

  // use a read only transaction for the sort-merge join
  bool read_only_txn;

  // partition the left table by the join key
  bool partition_left;

  // partition the right table by the join key
  bool partition_right;

  // time of the sort-merge join in milliseconds
  long execution_time_ms = 0;
};

extern configuration state;

void Usage(FILE *out);

void ParseArguments(int argc, char *argv[], configuration &state);

void ValidateScaleFactor(const configuration &state);

void WriteOutput();

}  // namespace sortbench
}  // namespace benchmark
}  // namespace peloton