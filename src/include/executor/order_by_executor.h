//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// order_by_executor.h
//
// Identification: src/include/executor/order_by_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "common/types.h"
#include "common/varlen_pool.h"
#include "executor/abstract_executor.h"
#include "storage/tuple.h"

namespace peloton {
namespace executor {

/**
 * @warning This is a pipeline breaker and a materialization point.
 *
 * TODO Currently, we store all input tiles and sort result in memory
 * until this executor is destroyed, which is sometimes necessary.
 * But can we let it release the RAM earlier as long as the executor
 * is not needed any more (e.g., with a LIMIT sitting on top)?
 */
class OrderByExecutor : public AbstractExecutor {
 public:
  OrderByExecutor(const OrderByExecutor &) = delete;
  OrderByExecutor &operator=(const OrderByExecutor &) = delete;
  OrderByExecutor(const OrderByExecutor &&) = delete;
  OrderByExecutor &operator=(const OrderByExecutor &&) = delete;

  explicit OrderByExecutor(const planner::AbstractPlan *node,
                           ExecutorContext *executor_context);

  ~OrderByExecutor();

  void UseAVX2Sort() {use_simd_sort_ = true;}

 protected:
  bool DInit();

  bool DExecute();

 private:
  bool DoSort();

  bool sort_done_ = false;

  /**
   * IMPORTANT This type must be move-constructible and move-assignable
   * in order to be correctly sorted by STL sort
   */
  struct sort_buffer_entry_t {
    ItemPointer item_pointer;
    std::unique_ptr<storage::Tuple> tuple;

    sort_buffer_entry_t(ItemPointer ipt, std::unique_ptr<storage::Tuple> &&tp)
        : item_pointer(ipt), tuple(std::move(tp)) {}

    sort_buffer_entry_t(sort_buffer_entry_t &&rhs) {
      item_pointer = rhs.item_pointer;
      tuple = std::move(rhs.tuple);
    }

    sort_buffer_entry_t &operator=(sort_buffer_entry_t &&rhs) {
      item_pointer = rhs.item_pointer;
      tuple = std::move(rhs.tuple);
      return *this;
    }

    sort_buffer_entry_t(const sort_buffer_entry_t &) = delete;
    sort_buffer_entry_t &operator=(const sort_buffer_entry_t &) = delete;
  };

  struct simd_sort_entry_t {
    int32_t key;
    int32_t oid_hash;

    simd_sort_entry_t(int32_t key, oid_t tile_group_id, oid_t tuple_id) {
      serialize(key, tile_group_id, tuple_id);
    }

    void serialize(int32_t key, oid_t tile_group_id, oid_t tuple_id) {
      this->key = key;
      this->oid_hash = tile_group_id*DEFAULT_TUPLES_PER_TILEGROUP + tuple_id;
    }

    void serialize_pad() {
      this->key = INT_MAX;
      this->oid_hash = 0;
    }

    void deserialize(oid_t& tile_group_id, oid_t& tuple_id) {
      tuple_id = oid_hash % DEFAULT_TUPLES_PER_TILEGROUP;
      tile_group_id = oid_hash/DEFAULT_TUPLES_PER_TILEGROUP;
    }
  };

  /** All tiles returned by child. */
  std::vector<std::unique_ptr<LogicalTile>> input_tiles_;

  /** Physical (not logical) schema of input tiles */
  std::unique_ptr<catalog::Schema> input_schema_;

  /** All valid tuples in sorted order */
  std::vector<sort_buffer_entry_t> sort_buffer_;

  /** All valid tuples in sorted order
   * Note: Used when the sorting column is an integer
   * Can only sort tables that have oids <= 2^32
   */
  simd_sort_entry_t *simd_sort_buffer_;

  bool use_simd_sort_ = false;

  size_t simd_sort_buffer_size_;

  /** Size of each tile returned after sorting **/
  size_t output_tile_size_;

  /** Tuples in sort_buffer only contains the sort keys */
  std::unique_ptr<catalog::Schema> sort_key_tuple_schema_;

  std::vector<bool> descend_flags_;

  /** How many tuples have been returned to parent */
  size_t num_tuples_returned_ = 0;
};

} /* namespace executor */
} /* namespace peloton */
