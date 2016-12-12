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

inline void minmax(const __m256i& a, const __m256i& a1, __m256i& a2, __m256i& a3,
                   const __m256i& b, const __m256i& b1, __m256i& b2, __m256i& b3) {
  auto maska = _mm256_cmpgt_epi32 (a, a1);
  auto maskb = _mm256_cmpgt_epi32 (b, b1);
  maska = _mm256_shuffle_epi32(maska, 0xA0);
  maskb = _mm256_shuffle_epi32(maskb, 0xA0);
  a2 = _mm256_blendv_epi8(a, a1, maska);
  b2 = _mm256_blendv_epi8(b, b1, maskb);
  a3 = _mm256_blendv_epi8(a1, a, maska);
  b3 = _mm256_blendv_epi8(b1, b, maskb);
}

inline void minmax(
    const __m256i& a, const __m256i& a1, __m256i& a2, __m256i& a3,
    const __m256i& b, const __m256i& b1, __m256i& b2, __m256i& b3,
    const __m256i& c, const __m256i& c1, __m256i& c2, __m256i& c3,
    const __m256i& d, const __m256i& d1, __m256i& d2, __m256i& d3) {
  auto maska = _mm256_cmpgt_epi32 (a, a1);
  auto maskb = _mm256_cmpgt_epi32 (b, b1);
  auto maskc = _mm256_cmpgt_epi32 (c, c1);
  auto maskd = _mm256_cmpgt_epi32 (d, d1);

  maska = _mm256_shuffle_epi32(maska, 0xA0);
  maskb = _mm256_shuffle_epi32(maskb, 0xA0);
  maskc = _mm256_shuffle_epi32(maskc, 0xA0);
  maskd = _mm256_shuffle_epi32(maskd, 0xA0);

  a2 = _mm256_blendv_epi8(a, a1, maska);
  b2 = _mm256_blendv_epi8(b, b1, maskb);
  c2 = _mm256_blendv_epi8(c, c1, maskc);
  d2 = _mm256_blendv_epi8(d, d1, maskd);

  a3 = _mm256_blendv_epi8(a1, a, maska);
  b3 = _mm256_blendv_epi8(b1, b, maskb);
  c3 = _mm256_blendv_epi8(c1, c, maskc);
  d3 = _mm256_blendv_epi8(d1, d, maskd);
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


void sort32_64i(__m256i *rows) {
  sort_columns_64i(rows[0], rows[1], rows[2], rows[3]);
  sort_columns_64i(rows[4], rows[5], rows[6], rows[7]);

  merge8_64i(rows);
  transpose8_64i(rows);
}

void intra_register_sort(__m256i& a, __m256i& b, __m256i& c, __m256i& d) {
  __m256i a1, a2, a3, b1, b2, b3, c1, c2, c3, d1, d2, d3;

  // 2-by-2 merge
  a1 = _mm256_permute4x64_epi64(a, 0x4e);
  b1 = _mm256_permute4x64_epi64(b, 0x4e);
  c1 = _mm256_permute4x64_epi64(c, 0x4e);
  d1 = _mm256_permute4x64_epi64(d, 0x4e);
  minmax(a, a1, a2, a3);
  minmax(b, b1, b2, b3);
  minmax(c, c1, c2, c3);
  minmax(d, d1, d2, d3);

  // pick top-2 and last-2 64 bit elements from
  // corresponding registers
  a = _mm256_blend_epi32(a2, a3, 0xf0);
  b = _mm256_blend_epi32(b2, b3, 0xf0);
  c = _mm256_blend_epi32(c2, c3, 0xf0);
  d = _mm256_blend_epi32(d2, d3, 0xf0);

  // 1-by-1 merge
  a1 = _mm256_shuffle_epi32(a, 0x4e);
  b1 = _mm256_shuffle_epi32(b, 0x4e);
  c1 = _mm256_shuffle_epi32(c, 0x4e);
  d1 = _mm256_shuffle_epi32(d, 0x4e);
  minmax(a, a1, a2, a3);
  minmax(b, b1, b2, b3);
  minmax(c, c1, c2, c3);
  minmax(d, d1, d2, d3);

  // pick alternate elements from registers
  a = _mm256_blend_epi32(a2, a3, 0xcc);
  b = _mm256_blend_epi32(b2, b3, 0xcc);
  c = _mm256_blend_epi32(c2, c3, 0xcc);
  d = _mm256_blend_epi32(d2, d3, 0xcc);
}


// 8-by-8 merge
void bitonic_merge(__m256i& a, __m256i& b, __m256i& c, __m256i& d) {
  __m256i a1, b1, c1, d1;
  // 8-by-8 minmax
  auto cr = reverse(c);
  auto dr = reverse(d);
  minmax(a, dr, a1, c1);
  minmax(b, cr, b1, d1);

  // 4-by-4 minmax
  minmax(a1, b1, a, b);
  minmax(c1, d1, c, d);

  // intra-register minmax
  intra_register_sort(a, b, c, d);
}

// 16-by-16 merge
void bitonic_merge(__m256i& a, __m256i& b, __m256i& c, __m256i& d,
                   __m256i& e, __m256i& f, __m256i& g, __m256i& h) {
  __m256i a1, a2, b1, b2, c1, c2, d1, d2,
      e1, e2, f1, f2, g1, g2, h1, h2;

  // 16-by-16 minmax
  auto er = reverse(e);
  auto fr = reverse(f);
  auto gr = reverse(g);
  auto hr = reverse(h);
  minmax(a, hr, a1, h1);
  minmax(b, gr, b1, g1);
  minmax(c, fr, c1, f1);
  minmax(d, er, d1, e1);

  // 8-by-8 minmax
  minmax(a1, c1, a2, c2);
  minmax(b1, d1, b2, d2);
  minmax(e1, g1, e2, g2);
  minmax(f1, h1, f2, h2);

  // 4-by-4 minmax
  minmax(a2, b2, a, b);
  minmax(c2, d2, c, d);
  minmax(e2, f2, e, f);
  minmax(g2, h2, g, h);

  // intra-register minmax
  intra_register_sort(a, b, c, d);
  intra_register_sort(e, f, g, h);
}

// the two input arrays are a[start, mid] and a[mid+1, end]
void merge_phase(sort_ele_type *a, sort_ele_type *out,
                 int start, int mid, int end) {
  int i=start, j=mid+1, k=start;
  int i_end = i + mid - start + 1;
  int j_end = j + end - mid;

  auto ra = load_reg256(&a[i]);
  i += SIMD_SIZE;
  auto rb = load_reg256(&a[i]);
  i += SIMD_SIZE;
  auto rc = load_reg256(&a[i]);
  i += SIMD_SIZE;
  auto rd = load_reg256(&a[i]);
  i += SIMD_SIZE;
  auto re = load_reg256(&a[j]);
  j += SIMD_SIZE;
  auto rf = load_reg256(&a[j]);
  j += SIMD_SIZE;
  auto rg = load_reg256(&a[j]);
  j += SIMD_SIZE;
  auto rh = load_reg256(&a[j]);
  j += SIMD_SIZE;

  if (i < i_end && j < j_end) {
    do {
      bitonic_merge(ra, rb, rc, rd,
                    re, rf, rg, rh);

      // save the smaller half
      store_reg256(&out[k], ra);
      k += SIMD_SIZE;
      store_reg256(&out[k], rb);
      k += SIMD_SIZE;
      store_reg256(&out[k], rc);
      k += SIMD_SIZE;
      store_reg256(&out[k], rd);
      k += SIMD_SIZE;

      // use the larger half for the next comparison
      ra = re;
      rb = rf;
      rc = rg;
      rd = rh;

      // select the input with the lowest value at the current pointer
      // use the lower 32-bits for comparison
      if (*((int *)&a[i]) < *((int *)&a[j])) {
        re = load_reg256(&a[i]);
        i += SIMD_SIZE;
        rf = load_reg256(&a[i]);
        i += SIMD_SIZE;
        rg = load_reg256(&a[i]);
        i += SIMD_SIZE;
        rh = load_reg256(&a[i]);
        i += SIMD_SIZE;
      } else {
        re = load_reg256(&a[j]);
        j += SIMD_SIZE;
        rf = load_reg256(&a[j]);
        j += SIMD_SIZE;
        rg = load_reg256(&a[j]);
        j += SIMD_SIZE;
        rh = load_reg256(&a[j]);
        j += SIMD_SIZE;
      }
    } while (i < i_end && j < j_end);
  }

  // merge the final pair of registers from each input
  bitonic_merge(ra, rb, rc, rd,
                re, rf, rg, rh);

  store_reg256(&out[k], ra);
  k += SIMD_SIZE;
  store_reg256(&out[k], rb);
  k += SIMD_SIZE;
  store_reg256(&out[k], rc);
  k += SIMD_SIZE;
  store_reg256(&out[k], rd);
  k += SIMD_SIZE;

  ra = re;
  rb = rf;
  rc = rg;
  rd = rh;

  // consume remaining data from a, if left
  while (i < i_end) {
    re = load_reg256(&a[i]);
    i += SIMD_SIZE;
    rf = load_reg256(&a[i]);
    i += SIMD_SIZE;
    rg = load_reg256(&a[i]);
    i += SIMD_SIZE;
    rh = load_reg256(&a[i]);
    i += SIMD_SIZE;

    bitonic_merge(ra, rb, rc, rd,
                  re, rf, rg, rh);

    store_reg256(&out[k], ra);
    k += SIMD_SIZE;
    store_reg256(&out[k], rb);
    k += SIMD_SIZE;
    store_reg256(&out[k], rc);
    k += SIMD_SIZE;
    store_reg256(&out[k], rd);
    k += SIMD_SIZE;

    ra = re;
    rb = rf;
    rc = rg;
    rd = rh;
  }

  // consume remaining data from b, if left
  while (j < j_end) {
    re = load_reg256(&a[j]);
    j += SIMD_SIZE;
    rf = load_reg256(&a[j]);
    j += SIMD_SIZE;
    rg = load_reg256(&a[j]);
    j += SIMD_SIZE;
    rh = load_reg256(&a[j]);
    j += SIMD_SIZE;
    bitonic_merge(ra, rb, rc, rd,
                  re, rf, rg, rh);

    store_reg256(&out[k], ra);
    k += SIMD_SIZE;
    store_reg256(&out[k], rb);
    k += SIMD_SIZE;
    store_reg256(&out[k], rc);
    k += SIMD_SIZE;
    store_reg256(&out[k], rd);
    k += SIMD_SIZE;

    ra = re;
    rb = rf;
    rc = rg;
    rd = rh;
  }

  store_reg256(&out[k], ra);
  k += SIMD_SIZE;
  store_reg256(&out[k], rb);
  k += SIMD_SIZE;
  store_reg256(&out[k], rc);
  k += SIMD_SIZE;
  store_reg256(&out[k], rd);
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
  // first do a 8-8 merge
  for (size_t j=0; j < len-1; j+=16) {
    auto r1 = load_reg256(&a[j]);
    auto r2 = load_reg256(&a[j+4]);
    auto r3 = load_reg256(&a[j+8]);
    auto r4 = load_reg256(&a[j+12]);
    bitonic_merge(r1, r2, r3, r4);
    store_reg256(&b[j], r1);
    store_reg256(&b[j+4], r2);
    store_reg256(&b[j+8], r3);
    store_reg256(&b[j+12], r4);
  }
  i++;
  // start from 16-16 merge
  for (size_t pass_size=4*SIMD_SIZE; pass_size<len; pass_size*=2, i++) {
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
    sort32_64i(rows);
    for (int j = 0; j < SORT_SIZE/SIMD_SIZE; j++) {
      store_reg256(&a[i+j*SIMD_SIZE], rows[j]);
    }
  }

  return merge(a, b, len);
}


}  // namespace util
}  // namespace peloton