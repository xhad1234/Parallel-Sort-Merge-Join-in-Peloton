//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// simd_merge_sort.h
//
// Identification: src/util/simd_merge_sort.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <immintrin.h>
#include <utility>
#include <string>
#include <vector>

#define SIMD_SIZE 8
#define SORT_SIZE 64

namespace peloton {
namespace util {

typedef unsigned int sort_ele_type;


inline __m256i load_reg256(sort_ele_type *a) {
  return *(__m256i*)a;
}

inline void store_reg256(sort_ele_type *a, __m256i& b) {
  *((__m256i*)a) = b;
}

typedef struct masks {
  __m256i rev_idx_mask;
  __m256i swap_128;

  masks() {
    alignas(32) sort_ele_type rev_idx_mask[8] = {7, 6, 5, 4, 3, 2, 1, 0};
    alignas(32) sort_ele_type swap_128[8] = {4, 5, 6, 7, 0, 1, 2, 3};
    this->rev_idx_mask = load_reg256(&rev_idx_mask[0]);
    this->swap_128 = load_reg256(&swap_128[0]);
  }
} masks;

extern masks global_masks;

std::pair<sort_ele_type *, sort_ele_type *> simd_merge_sort(sort_ele_type *a,
                                                            sort_ele_type *b,
                                                            size_t len);

}
}