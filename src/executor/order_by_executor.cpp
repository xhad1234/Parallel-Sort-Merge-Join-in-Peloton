//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// order_by_executor.cpp
//
// Identification: src/executor/order_by_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>

#include "common/logger.h"
#include "common/varlen_pool.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/order_by_executor.h"
#include "executor/executor_context.h"

#include "planner/order_by_plan.h"
#include "storage/tile.h"

#include "util/simd_merge_sort.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor
 * @param node  OrderByNode plan node corresponding to this executor
 */
OrderByExecutor::OrderByExecutor(const planner::AbstractPlan *node,
                                 ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

OrderByExecutor::~OrderByExecutor() {}

bool OrderByExecutor::DInit() {
  PL_ASSERT(children_.size() == 1);

  use_simd_sort_ = false;
  sort_done_ = false;
  num_tuples_returned_ = 0;

  return true;
}

bool OrderByExecutor::DExecute() {
  size_t sort_buffer_size = 0, tile_size = 0;
  std::shared_ptr<storage::Tile> ptile;

  LOG_TRACE("Order By executor ");

  if (!sort_done_) DoSort();

  if (int_sort_ == true) {
    if (!(num_tuples_returned_ < simd_sort_buffer_size_)) {
      return false;
    }

    PL_ASSERT(sort_done_);
    PL_ASSERT(input_schema_.get());
    PL_ASSERT(input_tiles_.size() > 0);

    // Returned tiles must be newly created physical tiles,
    // which have the same physical schema as input tiles.
    tile_size = std::min(size_t(DEFAULT_TUPLES_PER_TILEGROUP),
                                simd_sort_buffer_size_ - num_tuples_returned_);

    ptile.reset(storage::TileFactory::GetTile(
        BACKEND_TYPE_MM, INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID,
        nullptr, *input_schema_, nullptr, tile_size));

    for (size_t id=0; id < tile_size; id++) {
      oid_t source_tile_id, source_tuple_id;
      simd_sort_buffer_[num_tuples_returned_ + id].deserialize(source_tile_id,
                                                               source_tuple_id);
      // Insert a physical tuple into physical tile
      for (oid_t col = 0; col < input_schema_->GetColumnCount(); col++) {
        common::Value val = (
            input_tiles_[source_tile_id]->GetValue(source_tuple_id, col));
        ptile.get()->SetValue(val, id, col);
      }
    }
    PL_ASSERT(num_tuples_returned_+tile_size <= simd_sort_buffer_size_);
  } else {
    sort_buffer_size = sort_buffer_.size();
    if (!(num_tuples_returned_ < sort_buffer_size)) {
      return false;
    }

    PL_ASSERT(sort_done_);
    PL_ASSERT(input_schema_.get());
    PL_ASSERT(input_tiles_.size() > 0);

    // Returned tiles must be newly created physical tiles,
    // which have the same physical schema as input tiles.
    tile_size = std::min(size_t(DEFAULT_TUPLES_PER_TILEGROUP),
                                sort_buffer_size - num_tuples_returned_);

    ptile.reset(storage::TileFactory::GetTile(
        BACKEND_TYPE_MM, INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID,
        nullptr, *input_schema_, nullptr, tile_size));

    for (size_t id = 0; id < tile_size; id++) {
      oid_t source_tile_id =
          sort_buffer_[num_tuples_returned_ + id].item_pointer.block;
      oid_t source_tuple_id =
          sort_buffer_[num_tuples_returned_ + id].item_pointer.offset;
      // Insert a physical tuple into physical tile
      for (oid_t col = 0; col < input_schema_->GetColumnCount(); col++) {
        common::Value val = (
            input_tiles_[source_tile_id]->GetValue(source_tuple_id, col));
        ptile.get()->SetValue(val, id, col);
      }
    }

    PL_ASSERT(num_tuples_returned_+tile_size <= sort_buffer_size);
  }

  // Create an owner wrapper of this physical tile
  std::vector<std::shared_ptr<storage::Tile>> singleton({ptile});
  std::unique_ptr<LogicalTile> ltile(LogicalTileFactory::WrapTiles(singleton));
  PL_ASSERT(ltile->GetTupleCount() == tile_size);

  SetOutput(ltile.release());

  num_tuples_returned_ += tile_size;

  return true;
}

bool OrderByExecutor::DoSort() {
  PL_ASSERT(children_.size() == 1);
  PL_ASSERT(children_[0] != nullptr);
  PL_ASSERT(!sort_done_);
  PL_ASSERT(executor_context_ != nullptr);

  auto start = static_cast<double>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count());

  // Extract all data from child
  while (children_[0]->Execute()) {
    input_tiles_.emplace_back(children_[0]->GetOutput());
  }

  /** Number of valid tuples to be sorted. */
  size_t count = 0;
  for (auto &tile : input_tiles_) {
    count += tile->GetTupleCount();
  }

  if (count == 0) return true;

  // Grab data from plan node
  const planner::OrderByPlan &node = GetPlanNode<planner::OrderByPlan>();
  descend_flags_ = node.GetDescendFlags();

  // Extract the schema for sort keys.
  input_schema_.reset(input_tiles_[0]->GetPhysicalSchema());
  std::vector<catalog::Column> sort_key_columns;
  for (auto id : node.GetSortKeys()) {
    sort_key_columns.push_back(input_schema_->GetColumn(id));
  }

  // use simd sort if we have a single integer column
  // being sorted in ascending order
  if (sort_key_columns.size() == 1  && descend_flags_[0] == false &&
      sort_key_columns[0].GetType() == common::Type::INTEGER) {
    int_sort_ = true;
    simd_sort_buffer_size_= count;
    size_t padded_count = count;
    simd_sort_entry_t *temp;

    if (count%64 != 0) {
      padded_count = ((count+64)/64)*64;
    }

    if (posix_memalign((void **)&simd_sort_buffer_, 32,
                       padded_count*sizeof(simd_sort_entry_t)) != 0) {
      throw std::bad_alloc();
    }

    if (posix_memalign((void **)&temp, 32,
                       padded_count*sizeof(simd_sort_entry_t)) != 0) {
      throw std::bad_alloc();
    }

    size_t i=0;
    for (oid_t tile_id = 0; tile_id < input_tiles_.size(); tile_id++) {
      for (oid_t tuple_id : *input_tiles_[tile_id]) {
        common::Value value =
            input_tiles_[tile_id]->GetValue(tuple_id, node.GetSortKeys()[0]);
        PL_ASSERT(tuple_id < (oid_t)DEFAULT_TUPLES_PER_TILEGROUP);
        simd_sort_buffer_[i++].serialize(value.GetAs<int32_t>(),
                                       tile_id, tuple_id);
      }
    }

    while (i < padded_count) {
      simd_sort_buffer_[i++].serialize_pad();
    }

    PL_ASSERT(simd_sort_buffer_size_ == count);

    if (use_simd_sort_ == true) {
      // TODO:insert sort function here
      auto result = util::simd_merge_sort(
          reinterpret_cast<util::sort_ele_type *>(simd_sort_buffer_),
          reinterpret_cast<util::sort_ele_type*>(temp), padded_count);

      simd_sort_buffer_ = reinterpret_cast<simd_sort_entry_t*>(result.first);
      delete result.second;
    } else {
      struct KeyComparer {
        bool operator ()(const simd_sort_entry_t& a, const simd_sort_entry_t& b) {
          return a.ele < b.ele;
        }
      };
      std::sort(simd_sort_buffer_, simd_sort_buffer_+padded_count, KeyComparer());
    }

  } else {
    sort_key_tuple_schema_.reset(new catalog::Schema(sort_key_columns));
    auto executor_pool = executor_context_->GetExecutorContextPool();
    // Extract all valid tuples into a single std::vector (the sort buffer)
    sort_buffer_.reserve(count);

    for (oid_t tile_id = 0; tile_id < input_tiles_.size(); tile_id++) {
      for (oid_t tuple_id : *input_tiles_[tile_id]) {
        // Extract the sort key tuple
        std::unique_ptr<storage::Tuple> tuple(
            new storage::Tuple(sort_key_tuple_schema_.get(), true));
        for (oid_t id = 0; id < node.GetSortKeys().size(); id++) {
          common::Value val = (
              input_tiles_[tile_id]->GetValue(tuple_id, node.GetSortKeys()[id]));
          tuple->SetValue(id, val, executor_pool);
        }
        // Inert the sort key tuple into sort buffer
        sort_buffer_.emplace_back(sort_buffer_entry_t(
            ItemPointer(tile_id, tuple_id), std::move(tuple)));
      }
    }

    PL_ASSERT(count == sort_buffer_.size());

    // Prepare the compare function
    // Note: This is a less-than comparer, NOT an equality comparer.
    struct TupleComparer {
      TupleComparer(std::vector<bool> &_descend_flags)
          : descend_flags(_descend_flags) { }

      bool operator()(const storage::Tuple *ta, const storage::Tuple *tb) {
        for (oid_t id = 0; id < descend_flags.size(); id++) {
          common::Value va = (ta->GetValue(id));
          common::Value vb = (tb->GetValue(id));
          if (!descend_flags[id]) {
            common::Value cmp_lt = (va.CompareLessThan(vb));
            if (cmp_lt.IsTrue())
              return true;
            else {
              common::Value cmp_gt = (va.CompareGreaterThan(vb));
              if (cmp_gt.IsTrue())
                return false;
            }
          }
          else {
            common::Value cmp_lt = (vb.CompareLessThan(va));
            if (cmp_lt.IsTrue())
              return true;
            else {
              common::Value cmp_gt = (vb.CompareGreaterThan(va));
              if (cmp_gt.IsTrue())
                return false;
            }
          }
        }
        return false;  // Will return false if all keys equal
      }

      std::vector<bool> descend_flags;
    };

    TupleComparer comp(descend_flags_);

    // Finally ... sort it !
    std::sort(
        sort_buffer_.begin(), sort_buffer_.end(),
        [&comp](const sort_buffer_entry_t &a, const sort_buffer_entry_t &b) {
          return comp(a.tuple.get(), b.tuple.get());
        });
  }

  sort_done_ = true;

  auto end = static_cast<double>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count());

  LOG_ERROR("Sort time:%f", (end-start)/1000);
  return true;
}

} /* namespace executor */
} /* namespace peloton */
