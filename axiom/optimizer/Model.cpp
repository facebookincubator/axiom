/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <folly/container/F14Set.h>
#include <algorithm>
#include <cmath>

#include "axiom/optimizer/Model.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::optimizer {

namespace {

/// For purposes of the model being within 1/1M of the range of a
/// dimension is as good as equal. A normalized distance > 1.e-6 is
/// a match.
bool nearZero(float f) {
  return f < 1e-6 && f > -1e-6;
}

template <typename T>
float distance(const std::vector<T>& p1, const std::vector<T>& p2) {
  float sum = 0.0F;
  for (size_t i = 0; i < p1.size(); ++i) {
    sum += static_cast<float>(std::pow(p1[i] - p2[i], 2));
  }
  return std::sqrt(sum);
}

std::vector<float> midPoint(
    const std::vector<float>& p1,
    const std::vector<float>& p2) {
  auto r = p1;
  for (auto i = 0; i < p1.size(); ++i) {
    r[i] = (r[i] + p2[i]) / 2;
  }
  return r;
}

} // namespace

void Model::insert(std::vector<float> dimensions, float measure) {
  VELOX_CHECK_EQ(dimensions.size(), rank_, "Bad umber of dimensions");
  entries_.emplace_back(std::move(dimensions), measure);
}

void Model::precompute() {
  int32_t numCells = 1;
  for (auto dim = 0; dim < rank_; ++dim) {
    folly::F14FastSet<float> set;
    for (auto& e : entries_) {
      set.insert(e.coordinates[dim]);
    }
    std::vector<float> values;
    VELOX_CHECK_GT(
        set.size(),
        1,
        "A dimension must have more than one"
        "values: dim={}",
        dim);
    values.reserve(set.size());
    for (auto v : set) {
      values.push_back(v);
    }
    std::ranges::sort(values, [](float l, float r) { return l < r; });
    stride_.push_back(numCells);
    sizes_.push_back(static_cast<int32_t>(values.size()));
    numCells *= sizes_.back();
    axis_.push_back(std::move(values));
  }
  measures_.resize(numCells, std::nanf(""));
  for (auto& e : entries_) {
    auto dims = findDims(e.coordinates);
    normalizedIndices_.push_back(dims);
    normalizedPoints_.push_back(normalizePoint(e.coordinates));
    auto linIdx = linearIdx(dims);
    measures_[linIdx] = e.measure;
  }
  entries_.clear();

  // For each dimension, the set of intervals where the end points differ only
  // along the dimension.
  fillIntervals();
  for (auto i = 0; i < measures_.size(); ++i) {
    if (std::isnan(measures_[i])) {
      measures_[i] = guessIntermediate(i);
    }
  }
}

void Model::fillIntervals() {
  intervals_.resize(rank_);
  for (auto i = 0; i < measures_.size(); ++i) {
    if (!std::isnan(measures_[i])) {
      auto point = pointAtLinIdx(i);
      auto measure = measures_[i];
      for (auto i = 0; i < rank_; ++i) {
        auto cursor = point;
        int32_t startIdx = point[i];
        for (int32_t idx = startIdx + 1; idx < sizes_[i]; ++idx) {
          cursor[i] = idx;
          float m = at(cursor);
          if (!std::isnan(m)) {
            intervals_[i].push_back(
                Interval{.point = point, .idx2 = idx, .m1 = measure, .m2 = m});
            break;
          }
        }
      }
    }
  }
  for (auto i = 0; i < rank_; ++i) {
    for (auto& interval : intervals_[i]) {
      interval.k = (interval.m2 - interval.m1) /
          (axis_[i][interval.idx2] - axis_[i][interval.point[i]]);
      interval.normalizedLow = normalizePoint(coordinatesAt(interval.point));
      auto highPoint = interval.point;
      highPoint[i] = interval.idx2;
      interval.normalizedHigh = normalizePoint(coordinatesAt(highPoint));
      interval.mid = midPoint(interval.normalizedLow, interval.normalizedHigh);
    }
  }
}

std::vector<int32_t> Model::closestNormalized(
    const std::vector<float>& npoint) const {
  int32_t idx = -1;
  float dist = -1;
  for (auto i = 0; i < normalizedPoints_.size(); ++i) {
    auto d = distance(npoint, normalizedPoints_[i]);
    if (d < dist || idx == -1) {
      dist = d;
      idx = i;
    }
  }
  return normalizedIndices_[idx];
}

float Model::guessIntermediate(int32_t linIdx) {
  auto indices = pointAtLinIdx(linIdx);
  auto point = normalizePoint(coordinatesAt(indices));
  auto closest = closestNormalized(point);
  float m = at(closest);
  for (auto i = 0; i < rank_; ++i) {
    auto k = gradientAt(i, point);
    auto pos = axis_[i][closest[i]];
    auto posToFind = axis_[i][indices[i]];
    m += k * (posToFind - pos);
  }
  normalizedPoints_.push_back(point);
  normalizedIndices_.push_back(indices);
  return m;
}

int32_t Model::closestSlope(
    int32_t dim,
    float cutoff,
    const std::vector<float>& npoint,
    bool above) const {
  int32_t best = -1;
  float bestDist = -1;
  for (auto i = 0; i < intervals_[dim].size(); ++i) {
    auto& slope = intervals_[dim][i];
    bool isAbove = cutoff < slope.mid[dim];
    if (isAbove != above) {
      continue;
    }
    float d = distance(npoint, slope.mid);
    if (best == -1 || d < bestDist) {
      best = i;
      bestDist = d;
    }
  }
  return best;
}

float Model::gradientAt(int32_t dim, const std::vector<float>& npoint) const {
  folly::F14FastSet<int32_t> slopeIdx;
  for (auto i = 0; i < rank_; ++i) {
    auto above = closestSlope(i, npoint[i], npoint, true);
    auto below = closestSlope(i, npoint[i], npoint, false);
    if (above != -1) {
      slopeIdx.insert(above);
    }
    if (below != -1) {
      slopeIdx.insert(below);
    }
  }

  float sumWeight = 0;
  float sum = 0;
  for (auto idx : slopeIdx) {
    auto& slope = intervals_[dim][idx];
    auto d = distance(slope.mid, npoint);
    if (nearZero(d)) {
      return slope.k;
    }
    float w = 1.0F / d;
    sum += slope.k * w;
    sumWeight += w;
  }
  return sum / sumWeight;
}

int32_t Model::linearIdx(const std::vector<int32_t>& indices) const {
  int32_t idx = 0;
  for (auto i = 0; i < indices.size(); ++i) {
    VELOX_CHECK_LT(indices[i], sizes_[i]);
    idx += indices[i] * stride_[i];
  }
  return idx;
}

float Model::at(const std::vector<int32_t>& point) const {
  return measures_[linearIdx(point)];
}

std::vector<float> Model::normalizePoint(
    const std::vector<float>& point) const {
  std::vector<float> result(point.size());
  for (auto i = 0; i < point.size(); ++i) {
    result[i] = (point[i] - axis_[i][0]) / (axis_[i].back() - axis_[i][0]);
  }
  return result;
}

std::vector<float> Model::coordinatesAt(
    const std::vector<int32_t>& point) const {
  std::vector<float> result(point.size());
  for (auto i = 0; i < point.size(); ++i) {
    result[i] = axis_[i][point[i]];
  }
  return result;
}

std::vector<int32_t> Model::pointAtLinIdx(int32_t linIdx) const {
  std::vector<int32_t> point(rank_);
  for (auto i = rank_ - 1; i >= 0; --i) {
    point[i] = linIdx / stride_[i];
    linIdx = linIdx % stride_[i];
  }
  return point;
}

std::vector<int32_t> Model::findDims(const std::vector<float>& point) const {
  std::vector<int32_t> result;
  result.reserve(rank_);
  for (auto i = 0; i < rank_; ++i) {
    auto it = std::ranges::lower_bound(axis_[i], point[i]);
    result.push_back(
        static_cast<int32_t>(
            it == axis_[i].end() ? axis_[i].size() - 1
                                 : it - axis_[i].begin()));
  }
  return result;
}

std::vector<float> Model::normalizedGridPoint(
    const std::vector<int32_t>& dims) const {
  std::vector<float> result(rank_);
  for (auto i = 0; i < rank_; ++i) {
    result[i] =
        (axis_[i][dims[i]] - axis_[i][0]) / (axis_[i].back() - axis_[i][0]);
  }
  return result;
}

float Model::normalizedDim(int32_t dim, int32_t idx) const {
  return (axis_[dim][idx] - axis_[dim][0]) / (axis_[0].back() - axis_[dim][0]);
}

void Model::gradientsAtGridPoint(
    const std::vector<int32_t>& dims,
    float d,
    const bool* outOfRange,
    float* gradient,
    float* gradientWeight) const {
  for (auto i = 0; i < rank_; ++i) {
    if (outOfRange[i]) {
      auto linIdx = linearIdx(dims);
      float k = (measures_[linIdx] - measures_[linIdx - stride_[i]]) /
          (normalizedDim(i, dims[i]) - normalizedDim(i, dims[i] - 1));
      if (nearZero(d)) {
        gradient[i] = k;
      } else {
        gradient[i] += k * (1.0F / d);
        gradientWeight[i] += 1.0F / d;
      }
    }
  }
}

void Model::neighbors(
    const std::vector<int32_t>& dims,
    const std::vector<float>& npoint,
    int32_t dim,
    float& sum,
    float& sumWeight,
    bool& exact,
    bool* outOfRange,
    float* gradient,
    float* gradientWeight) const {
  if (dim == rank_) {
    auto normalizedCorner = normalizedGridPoint(dims);
    float d = 0;
    d = distance(npoint, normalizedCorner);
    float measure = at(dims);
    if (nearZero(d)) {
      exact = true;
      sum = measure;
      gradientsAtGridPoint(dims, 0, outOfRange, gradient, gradientWeight);
      return;
    }
    sum += measure * (1.0F / d);
    sumWeight += 1.0F / d;
    gradientsAtGridPoint(dims, d, outOfRange, gradient, gradientWeight);
    return;
  }
  neighbors(
      dims,
      npoint,
      dim + 1,
      sum,
      sumWeight,
      exact,
      outOfRange,
      gradient,
      gradientWeight);

  // Return if exact match found or if there is no lower value of 'dim' or if
  // coordinate of dim is outside of cube.
  if (exact || dims[dim] == 0 || outOfRange[dim]) {
    return;
  }

  // See the cell below the coordinate on the axis of dim.
  auto corner = dims;
  corner = dims;
  --corner[dim];
  neighbors(
      corner,
      npoint,
      dim + 1,
      sum,
      sumWeight,
      exact,
      outOfRange,
      gradient,
      gradientWeight);
}

float Model::query(const std::vector<float>& coords) const {
  auto dims = findDims(coords);
  auto npoint = normalizePoint(coords);
  // Point where out of range dims are cropped to the boundary.
  auto innerPoint = npoint;
  constexpr int32_t kMaxRank = 12;
  bool outOfRange[kMaxRank] = {};
  float gradient[kMaxRank] = {};
  float gradientWeight[kMaxRank] = {};
  float sum = 0;
  float sumWeight = 0;
  for (auto i = 0; i < rank_; ++i) {
    VELOX_CHECK_GE(
        coords[i], axis_[i][0], "Points below samples range not allowed");
    if (coords[i] > axis_[i].back()) {
      outOfRange[i] = true;
      innerPoint[i] = 1;
    }
  }
  bool exact = false;
  neighbors(
      dims,
      innerPoint,
      0,
      sum,
      sumWeight,
      exact,
      outOfRange,
      gradient,
      gradientWeight);
  if (!exact) {
    sum /= sumWeight;
  }
  // for the dims with coordinate outside of the cube, follow the gradient.
  for (auto dim = 0; dim < rank_; ++dim) {
    if (outOfRange[dim]) {
      float k = exact ? gradient[dim] : gradient[dim] / gradientWeight[dim];
      sum += k * (npoint[dim] - 1);
    }
  }
  return sum;
}

} // namespace facebook::axiom::optimizer
