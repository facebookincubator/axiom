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

#pragma once
#include <cstdint>
#include <vector>

namespace facebook::axiom::optimizer {

///  A linear model for predicting cost of operations from samples
///  over n dimensions. The dataset has measures in n dimensional
///  space. The model predicts the value at an arbitrary point in
///  the space based on the measures of the beighboring points and
///  the slope along each dimension between the closest points on
///  either side of the point. If the point is outside of the range
///  of the dimension, the measure is estimated according to the
///  slope between the two outermost values along the dimension.
class Model {
 public:
  /// Describes a position of a point to query along one dimension.
  struct DimSample {
    // index along the dimension to the point below.
    int32_t idx1{0};
    // index along the dimension to the point above.
    int32_t idx2{0};
    // Coordinate at idx1
    float coord1{0};
    // Coordinate at idx2
    float coord2{0};
    // measure at idx1.
    float measure1{0};
    // measure at idx2.
    float measure2{0};
  };

  explicit Model(int32_t rank) : rank_(rank) {}

  void insert(std::vector<float> dimensions, float measure);

  void precompute();

  float query(const std::vector<float>& coords) const;

  /// Returns the linear index into 'measures' from indices along each
  /// dimension.
  int32_t linearIdx(const std::vector<int32_t>& indices) const;

  /// Returns the measures from the two closest points along each dimension.
  std::vector<DimSample> slopes(
      const std::vector<int32_t>& point,
      const std::vector<float>& coords) const;

  /// Returns the index along each dimension to the closest value that is
  /// greater or equal   to the corresponding coordinate of position.
  std::vector<int32_t> findDims(const std::vector<float>& point) const;

  /// Maps coords to a  0..1 range along their dimension. The lowest value maps
  /// to 0, the highest to 1.
  std::vector<float> normalizePoint(const std::vector<float>& point) const;

  std::vector<float> coordinatesAt(const std::vector<int32_t>& point) const;

  int32_t rank() const {
    return rank_;
  }

 private:
  struct Entry {
    Entry(std::vector<float> coordinates, float measure)
        : coordinates(std::move(coordinates)), measure(measure) {}

    std::vector<float> coordinates;
    float measure;
  };

  // Represents a pair of points that are offset on only one axis.
  struct Interval {
    // The point with measure 'm1'
    std::vector<int32_t> point;
    // The index of m2, such that all other coordinates are the same.
    int32_t idx2;
    float m1;
    float m2;
    float k;
    std::vector<float> normalizedLow;
    std::vector<float> normalizedHigh;
    std::vector<float> mid;
  };

  void fillIntervals();

  int32_t closestSlope(
      int32_t dim,
      float cutoff,
      const std::vector<float>& npoint,
      bool above) const;

  float gradientAt(int32_t dim, const std::vector<float>& npoint) const;

  float guessIntermediate(int32_t linIdx);

  std::vector<int32_t> pointAtLinIdx(int32_t linIdx) const;

  float at(const std::vector<int32_t>& point) const;

  std::vector<int32_t> closestNormalized(
      const std::vector<float>& npoint) const;

  void neighbors(
      const std::vector<int32_t>& dims,
      const std::vector<float>& npoint,
      int32_t dim,
      float& sum,
      float& sumWeight,
      bool& exact,
      bool* outOfRange,
      float* gradient,
      float* gradientWeight) const;

  void gradientsAtGridPoint(
      const std::vector<int32_t>& dims,
      float d,
      const bool* outOfRange,
      float* gradient,
      float* gradientWeight) const;

  float normalizedDim(int32_t dim, int32_t idx) const;

  std::vector<float> normalizedGridPoint(
      const std::vector<int32_t>& dims) const;

  const int32_t rank_;

  std::vector<Entry> entries_;

  // For each dimension, sorted distinct values.
  std::vector<std::vector<float>> axis_;

  // stride along each dimension. For 3x4x5 the strides are 1, 3, 12 and the
  // size of 'measures' is 60.
  std::vector<int32_t> stride_;

  // The size aalong each dimension. For 3x4x5 this is 3, 4, 5.
  std::vector<int32_t> sizes_;

  // Measures. The size is the product of the sizes of  the vectors in axix_;
  std::vector<float> measures_;

  // Original dataset points normalized so dims are 0..1.
  std::vector<std::vector<float>> normalizedPoints_;
  // cube  indices of each in 'normalizedPoints.
  std::vector<std::vector<int32_t>> normalizedIndices_;

  std::vector<std::vector<Interval>> intervals_;
};

} // namespace facebook::axiom::optimizer
