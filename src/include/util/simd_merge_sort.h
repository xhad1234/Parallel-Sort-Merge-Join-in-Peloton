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

#define SIMD_SIZE 4
#define SORT_SIZE 16

namespace peloton {
namespace util {

typedef long long sort_ele_type;

inline __m256i load_reg256(sort_ele_type *a) {
  return _mm256_stream_load_si256((__m256i*)a);
}

inline void store_reg256(sort_ele_type *a, __m256i& b) {
  _mm256_stream_si256((__m256i*)a, b);
}

std::pair<sort_ele_type *, sort_ele_type *> simd_merge_sort(sort_ele_type *a,
                                                            sort_ele_type *b,
                                                            size_t len);

}
}