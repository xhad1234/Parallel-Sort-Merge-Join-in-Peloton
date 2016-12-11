//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// simd_merge_sort.cpp
//
// Identification: src/util/simd_merge_sort.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "util/simd_merge_sort.h"
#include "common/macros.h"
#include <stdlib.h>
#include <algorithm>
#include <cstring>
#include <climits>
#include <cassert>

namespace peloton {
namespace util {


inline __m256i reverse(__m256i& v) {
  return _mm256_permute4x64_epi64(v, 0x1b);
}

inline void minmax(const __m256i& a, const __m256i& b, __m256i& c, __m256i& d) {
  auto mask = _mm256_cmpgt_epi32 (a, b);
  mask = _mm256_shuffle_epi32(mask, 0xA0);
  c = _mm256_blendv_epi8(a, b, mask);
  d = _mm256_blendv_epi8(b, a, mask);
}

inline void minmax(__m256i& a, __m256i& b) {
  auto mask = _mm256_cmpgt_epi32 (a, b);
  mask = _mm256_shuffle_epi32(mask, 0xA0);
  auto t = a;
  a = _mm256_blendv_epi8(a, b, mask);
  b = _mm256_blendv_epi8(b, t, mask);
}

inline void transpose8_64i(__m256i *rows) {
  __m256i __t0 = _mm256_unpacklo_epi64(rows[0], rows[1]);
  __m256i __t1 = _mm256_unpackhi_epi64(rows[0], rows[1]);
  __m256i __t2 = _mm256_unpacklo_epi64(rows[2], rows[3]);
  __m256i __t3 = _mm256_unpackhi_epi64(rows[2], rows[3]);

  __m256i __t4 = _mm256_unpacklo_epi64(rows[4], rows[5]);
  __m256i __t5 = _mm256_unpackhi_epi64(rows[4], rows[5]);
  __m256i __t6 = _mm256_unpacklo_epi64(rows[6], rows[7]);
  __m256i __t7 = _mm256_unpackhi_epi64(rows[6], rows[7]);

  rows[0] = _mm256_permute2x128_si256(__t0, __t2, 0x20);
  rows[2] = _mm256_permute2x128_si256(__t1, __t3, 0x20);
  rows[4] = _mm256_permute2x128_si256(__t0, __t2, 0x31);
  rows[6] = _mm256_permute2x128_si256(__t1, __t3, 0x31);

  rows[1] = _mm256_permute2x128_si256(__t4, __t6, 0x20);
  rows[3] = _mm256_permute2x128_si256(__t5, __t7, 0x20);
  rows[5] = _mm256_permute2x128_si256(__t4, __t6, 0x31);
  rows[7] = _mm256_permute2x128_si256(__t5, __t7, 0x31);
}

inline void transpose4_64i(__m256i *rows) {
  __m256i __t0 = _mm256_unpacklo_epi64(rows[0], rows[1]);
  __m256i __t1 = _mm256_unpackhi_epi64(rows[0], rows[1]);
  __m256i __t2 = _mm256_unpacklo_epi64(rows[2], rows[3]);
  __m256i __t3 = _mm256_unpackhi_epi64(rows[2], rows[3]);

  rows[0] = _mm256_permute2x128_si256(__t0, __t2, 0x20);
  rows[1] = _mm256_permute2x128_si256(__t1, __t3, 0x20);
  rows[2] = _mm256_permute2x128_si256(__t0, __t2, 0x31);
  rows[3] = _mm256_permute2x128_si256(__t1, __t3, 0x31);
}

void sort_columns_64i(__m256i& row0, __m256i& row1,
                      __m256i& row2, __m256i& row3) {
  minmax(row0,row1);
  minmax(row2,row3);
  minmax(row0,row2);
  minmax(row1,row3);
  minmax(row1,row2);
}

void merge8_64i(__m256i *rows) {
  minmax(rows[0], rows[4]);
  minmax(rows[1], rows[5]);
  minmax(rows[2], rows[6]);
  minmax(rows[3], rows[7]);

  minmax(rows[2], rows[4]);
  minmax(rows[3], rows[5]);

  minmax(rows[1], rows[2]);
  minmax(rows[3], rows[4]);
  minmax(rows[5], rows[6]);
}

void sort16_64i(__m256i *rows) {
  sort_columns_64i(rows[0], rows[1], rows[2], rows[3]);
  transpose4_64i(rows);
}

void sort32_64i(__m256i *rows) {
  sort_columns_64i(rows[0], rows[1], rows[2], rows[3]);
  sort_columns_64i(rows[4], rows[5], rows[6], rows[7]);

  merge8_64i(rows);
  transpose8_64i(rows);
}

void intra_register_sort(__m256i& a) {
  __m256i b, c, d;

  // 2-by-2 merge
  b = _mm256_permute4x64_epi64(a, 0x4e);
  minmax(a, b, c, d);
  // pick top-2 and last-2 64 bit elements from
  // corresponding registers
  a = _mm256_blend_epi32(c, d, 0xf0);

  // 1-by-1 merge
  b = _mm256_shuffle_epi32(a, 0x4e);
  minmax(a, b, c, d);
  // pick alternate elements from registers
  a = _mm256_blend_epi32(c, d, 0xcc);
}


// 4-by-4 merge
void bitonic_merge(__m256i& a, __m256i& b) {
  // 4-by-4 minmax
  b = reverse(b);
  minmax(a, b);

  // intra-register minmax
  intra_register_sort(a);
  intra_register_sort(b);
}

// the two input arrays are a[start, mid] and a[mid+1, end]
void merge_phase(sort_ele_type *a, sort_ele_type *out,
                 int start, int mid, int end) {
  int i=start, j=mid+1, k=start;
  int i_end = i + mid - start + 1;
  int j_end = j + end - mid;

  auto ra = load_reg256(&a[i]);
  i += SIMD_SIZE;
  auto rb = load_reg256(&a[j]);
  j += SIMD_SIZE;

  if (i < i_end && j < j_end) {
    do {
      bitonic_merge(ra, rb);

      // save the smaller half
      store_reg256(&out[k], ra);
      k += SIMD_SIZE;

      // use the larger half for the next comparison
      ra = rb;

      // select the input with the lowest value at the current pointer
      // use the lower 32-bits for comparison
      if (*((int *)&a[i]) < *((int *)&a[j])) {
        rb = load_reg256(&a[i]);
        i += SIMD_SIZE;
      } else {
        rb = load_reg256(&a[j]);
        j += SIMD_SIZE;
      }
    } while (i < i_end && j < j_end);
  }

  // merge the final pair of registers from each input
  bitonic_merge(ra, rb);
  store_reg256(&out[k], ra);
  k += SIMD_SIZE;
  ra = rb;

  // consume remaining data from a, if left
  while (i < i_end) {
    rb = load_reg256(&a[i]);
    i += SIMD_SIZE;
    bitonic_merge(ra, rb);
    store_reg256(&out[k], ra);
    k += SIMD_SIZE;
    ra = rb;
  }

  // consume remaining data from b, if left
  while (j < j_end) {
    rb = load_reg256(&a[j]);
    j += SIMD_SIZE;
    bitonic_merge(ra, rb);
    store_reg256(&out[k], ra);
    k += SIMD_SIZE;
    ra = rb;
  }

  // store the final batch
  store_reg256(&out[k], ra);
  k += SIMD_SIZE;
}

// minimum merge_size=16, minimum n=2*merge_size
void merge_pass(sort_ele_type *in, sort_ele_type *out,
                int n, int merge_size) {
  for (int i=0; i < n-1; i+=2*merge_size) {
    auto mid = i + merge_size - 1;
    auto end = std::min(i+2*merge_size-1, n-1);
    // check if there are 2 sub-arrays to merge
    if (mid < end) {
      // merge two merge_size arrays per iteration
      merge_phase(in, out, i, mid, end);
    } else {
      // copy the leftover data to output
      std::memcpy(out+i, in+i, (n-i)*sizeof(sort_ele_type));
    }
  }
}

// assume first sort phase has finished
std::pair<sort_ele_type*, sort_ele_type*>
    merge(sort_ele_type *a, sort_ele_type *b, size_t len) {
  int i=0;
  /*
   * even iterations: a->b
   * odd iterations: b->a
   */
  // start from 4-4 merge
  for (size_t pass_size=SIMD_SIZE; pass_size<len; pass_size*=2, i++) {
    if (i%2 == 0) {
      merge_pass(a, b, len, pass_size);
    } else {
      merge_pass(b, a, len, pass_size);
    }
  }

  if (i%2 == 0)
    return std::make_pair(a, b);
  return std::make_pair(b, a);
}

std::pair<sort_ele_type*, sort_ele_type*>
    simd_merge_sort(sort_ele_type *a, sort_ele_type *b, size_t len) {

  __m256i rows[SORT_SIZE/SIMD_SIZE];
  PL_ASSERT(len % SORT_SIZE == 0);

  for (size_t i=0; i < len; i+=SORT_SIZE) {
    for (int j=0; j < SORT_SIZE/SIMD_SIZE; j++) {
      rows[j] = load_reg256(&a[i+j*SIMD_SIZE]);
    }
    sort16_64i(rows);
    for (int j = 0; j < SORT_SIZE/SIMD_SIZE; j++) {
      store_reg256(&a[i+j*SIMD_SIZE], rows[j]);
    }
  }

  return merge(a, b, len);
}


}  // namespace util
}  // namespace peloton