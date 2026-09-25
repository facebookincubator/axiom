#!/usr/bin/env bash
#
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

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "${SCRIPT_DIR}/.." && pwd)

source "${ROOT_DIR}/velox/scripts/setup-versions.sh"

ICEBERG_CPP_REVISION=87a11de7e83785d925cf3dcc31d84100dccfb62f
ICEBERG_CPP_SHA256=2167ea59c84db01249888530ebfd8bd30e72edd9fad1c94178f7f9aa0dd8eb4a
ICEBERG_CPP_URL="https://github.com/apache/iceberg-cpp/archive/${ICEBERG_CPP_REVISION}.tar.gz"
VELOX_ARROW_SHA256=9c473f2c9914c59ab571761c9497cf0e5cfd3ea335f7782ccc6121f5cb99ae9b
VELOX_ARROW_URL="https://github.com/apache/arrow/archive/apache-arrow-${ARROW_VERSION}.tar.gz"

INSTALL_PREFIX=${ICEBERG_CPP_INSTALL_PREFIX:-${INSTALL_PREFIX:-"${ROOT_DIR}/deps-install"}}
BUILD_DIR=${ICEBERG_CPP_BUILD_DIR:-"${ROOT_DIR}/_build/iceberg-cpp"}
SOURCE_DIR="${BUILD_DIR}/source"
ARCHIVE_PATH="${BUILD_DIR}/iceberg-cpp.tar.gz"
NUM_THREADS=${NUM_THREADS:-$(getconf _NPROCESSORS_CONF 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)}
ARROW_BUILD_DIR="${BUILD_DIR}/arrow"
ARROW_SOURCE_DIR="${ARROW_BUILD_DIR}/source"
ARROW_ARCHIVE_PATH="${ARROW_BUILD_DIR}/arrow.tar.gz"
ARROW_PREFIX=${ARROW_PREFIX:-"${INSTALL_PREFIX}"}

build_velox_arrow_with_parquet() {
  if [[ -f "${ARROW_PREFIX}/lib/cmake/Arrow/ArrowConfig.cmake" &&
        -f "${ARROW_PREFIX}/lib/cmake/Parquet/ParquetConfig.cmake" ]]; then
    return
  fi

  mkdir -p "${ARROW_BUILD_DIR}"
  curl --fail --location --silent --show-error "${VELOX_ARROW_URL}" --output "${ARROW_ARCHIVE_PATH}"

  local actual_sha256
  actual_sha256=$(shasum -a 256 "${ARROW_ARCHIVE_PATH}" | awk '{print $1}')
  if [[ "${actual_sha256}" != "${VELOX_ARROW_SHA256}" ]]; then
    echo "Arrow archive checksum does not match Velox's pinned revision" >&2
    exit 1
  fi

  rm -rf "${ARROW_SOURCE_DIR}"
  mkdir -p "${ARROW_SOURCE_DIR}"
  tar -xzf "${ARROW_ARCHIVE_PATH}" --strip-components=1 -C "${ARROW_SOURCE_DIR}"

  (
    cd "${ARROW_SOURCE_DIR}" || exit 1
    patch -p1 -i "${ROOT_DIR}/velox/CMake/resolve_dependency_modules/arrow/arrow-testing-boost.patch"
    patch -p1 -i "${ROOT_DIR}/velox/CMake/resolve_dependency_modules/arrow/cmake-compatibility.patch"
  )

  cmake \
    -S "${ARROW_SOURCE_DIR}/cpp" \
    -B "${ARROW_BUILD_DIR}/build" \
    -DARROW_PARQUET=ON \
    -DARROW_IPC=ON \
    -DARROW_FILESYSTEM=ON \
    -DARROW_JSON=ON \
    -DARROW_DEPENDENCY_SOURCE=AUTO \
    -DARROW_WITH_LZ4=ON \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_WITH_ZSTD=ON \
    -DARROW_JEMALLOC=OFF \
    -DARROW_SIMD_LEVEL=NONE \
    -DARROW_RUNTIME_SIMD_LEVEL=NONE \
    -DARROW_WITH_UTF8PROC=OFF \
    -DARROW_TESTING=ON \
    -DARROW_BUILD_STATIC=ON \
    -DARROW_BUILD_SHARED=ON \
    -DBUILD_WARNING_LEVEL=PRODUCTION \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DCMAKE_INSTALL_PREFIX="${ARROW_PREFIX}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -DARROW_CXXFLAGS=-Wno-documentation \
    -DBOOST_ROOT="${ROOT_DIR}/deps-install"

  cmake --build "${ARROW_BUILD_DIR}/build" --target install -j "${NUM_THREADS}"
}

mkdir -p "${BUILD_DIR}"
curl --fail --location --silent --show-error "${ICEBERG_CPP_URL}" --output "${ARCHIVE_PATH}"

ACTUAL_SHA256=$(shasum -a 256 "${ARCHIVE_PATH}" | awk '{print $1}')
if [[ "${ACTUAL_SHA256}" != "${ICEBERG_CPP_SHA256}" ]]; then
  echo "iceberg-cpp archive checksum does not match the pinned revision" >&2
  exit 1
fi

rm -rf "${SOURCE_DIR}"
mkdir -p "${SOURCE_DIR}"
tar -xzf "${ARCHIVE_PATH}" --strip-components=1 -C "${SOURCE_DIR}"
(
  cd "${SOURCE_DIR}" || exit 1
  patch -p1 -i "${ROOT_DIR}/CMake/resolve_dependency_modules/iceberg-cpp/arrow18-compat.patch"
)

build_velox_arrow_with_parquet

CONFIGURE_ARGS=(
  -S "${SOURCE_DIR}"
  -B "${BUILD_DIR}/build"
  -DICEBERG_BUILD_STATIC=ON
  -DICEBERG_BUILD_SHARED=OFF
  -DICEBERG_BUILD_TESTS=OFF
  -DICEBERG_BUILD_BENCHMARKS=OFF
  -DICEBERG_BUILD_REST=ON
  -DICEBERG_BUILD_REST_INTEGRATION_TESTS=OFF
  -DICEBERG_BUILD_HIVE=OFF
  -DICEBERG_BUILD_SQL_CATALOG=OFF
  -DICEBERG_BUILD_BUNDLE=ON
  -DICEBERG_BUNDLE_AWSSDK=OFF
  -DICEBERG_BUNDLE_THRIFT=OFF
  -DICEBERG_S3=OFF
  -DICEBERG_SIGV4=OFF
  -DFETCHCONTENT_TRY_FIND_PACKAGE_MODE=ALWAYS
  -DCMAKE_FIND_PACKAGE_PREFER_CONFIG=ON
  -DArrow_DIR="${ARROW_PREFIX}/lib/cmake/Arrow"
  -DParquet_DIR="${ARROW_PREFIX}/lib/cmake/Parquet"
  -DCMAKE_PREFIX_PATH="${ARROW_PREFIX};${ROOT_DIR}/deps-install"
  -DCMAKE_POLICY_VERSION_MINIMUM=3.5
  -DCMAKE_CXX_FLAGS="${CMAKE_CXX_FLAGS:-} -Wno-deprecated-declarations"
  -DCMAKE_BUILD_TYPE=Release
  -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}"
)

if [[ -n "${EXTRA_CMAKE_FLAGS:-}" ]]; then
  read -r -a EXTRA_ARGS <<<"${EXTRA_CMAKE_FLAGS}"
  CONFIGURE_ARGS+=("${EXTRA_ARGS[@]}")
fi

cmake "${CONFIGURE_ARGS[@]}"
cmake --build "${BUILD_DIR}/build" --target install -j "${NUM_THREADS}"

echo "iceberg-cpp installed to ${INSTALL_PREFIX}"
echo "Headers are available under ${INSTALL_PREFIX}/include/iceberg"
echo "Arrow and Parquet are resolved from ${ARROW_PREFIX}"
