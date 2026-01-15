# Copyright (c) Meta Platforms, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_library(axiom_optimizer_test_utils DfFunctions.cpp)

target_link_libraries(axiom_optimizer_test_utils axiom_optimizer axiom_logical_plan_builder)

# Velox UDF Utils
add_library(axiom_velox_udf_utils Utils.cpp)
target_link_libraries(axiom_velox_udf_utils
  Folly::folly
  velox_expression
  velox_type
  velox_vector
)

# ComputeSigridHash (header-only base)
add_library(axiom_compute_sigrid_hash INTERFACE)
target_link_libraries(axiom_compute_sigrid_hash INTERFACE
  Folly::folly
  velox_exception
)

# ComputeSigridHash AVX2 implementation
add_library(axiom_compute_sigrid_hash_avx2 ComputeSigridHashAvx2.cpp)
target_compile_options(axiom_compute_sigrid_hash_avx2 PRIVATE -mavx2 -mfma -mavx -mf16c)
target_link_libraries(axiom_compute_sigrid_hash_avx2 axiom_compute_sigrid_hash)

# ComputeSigridHash AVX512 implementation
add_library(axiom_compute_sigrid_hash_avx512 ComputeSigridHashAvx512.cpp)
target_compile_options(axiom_compute_sigrid_hash_avx512 PRIVATE
  -mavx512f -mavx512cd -mavx512dq -mavx512bw -mavx2 -mfma -mavx -mf16c)
target_link_libraries(axiom_compute_sigrid_hash_avx512 axiom_compute_sigrid_hash)

# SigridHash UDF
add_library(axiom_sigrid_hash SigridHash.cpp)
target_link_libraries(axiom_sigrid_hash
  axiom_compute_sigrid_hash
  axiom_compute_sigrid_hash_avx2
  axiom_compute_sigrid_hash_avx512
  axiom_velox_udf_utils
  velox_expression
  velox_vector
)

# Bucketize UDF
add_library(axiom_bucketize Bucketize.cpp)
target_link_libraries(axiom_bucketize
  velox_expression
  velox_type
  velox_vector
)

# FirstX UDF
add_library(axiom_first_x FirstX.cpp)
target_link_libraries(axiom_first_x
  axiom_velox_udf_utils
  velox_expression
  velox_type
  velox_vector
)

# ConvertFormat UDF
add_library(axiom_convert_format ConvertFormat.cpp)
target_link_libraries(axiom_convert_format
  velox_expression
  velox_vector
)

# Combined Velox UDFs registration
add_library(axiom_velox_udfs Register.cpp)
target_link_libraries(axiom_velox_udfs
  axiom_sigrid_hash
  axiom_bucketize
  axiom_first_x
  axiom_convert_format
)
