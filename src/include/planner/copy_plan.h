//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_plan.h
//
// Identification: src/include/planner/copy_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_plan.h"
#include "parser/statement_copy.h"
#include "parser/statement_select.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace parser {
class CopyStatement;
}

namespace planner {
class CopyPlan : public AbstractPlan {
 public:
  CopyPlan() = delete;
  CopyPlan(const CopyPlan &) = delete;
  CopyPlan &operator=(const CopyPlan &) = delete;
  CopyPlan(CopyPlan &&) = delete;
  CopyPlan &operator=(CopyPlan &&) = delete;

  explicit CopyPlan(char *file_path, bool deserialize_parameters)
      : file_path(file_path), deserialize_parameters(deserialize_parameters) {
    LOG_DEBUG("Creating a Copy Plan");
  }

  inline PlanNodeType GetPlanNodeType() const { return PLAN_NODE_TYPE_COPY; }

  const std::string GetInfo() const { return "CopyPlan"; }

  // TODO: Implement copy mechanism
  std::unique_ptr<AbstractPlan> Copy() const { return nullptr; }

  // The path of the target file
  std::string file_path;

  // Whether the copying requires deserialization of parameters
  bool deserialize_parameters = false;
};

}  // namespace planner
}  // namespace peloton
