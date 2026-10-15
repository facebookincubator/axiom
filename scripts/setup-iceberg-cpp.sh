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

ICEBERG_CPP_REVISION=${ICEBERG_CPP_REVISION:-274d1bbb4f46114806dc6515b2493483b47b3702}
ICEBERG_CPP_SHA256=${ICEBERG_CPP_SHA256:-e669feef10f7154e2b39bf96cb88610febd857f44e6c3233d3bd0ab0bcb50313}
ICEBERG_CPP_URL=${ICEBERG_CPP_URL:-"https://github.com/apache/iceberg-cpp/archive/${ICEBERG_CPP_REVISION}.tar.gz"}
ICEBERG_ARROW_VERSION=${ICEBERG_ARROW_VERSION:-25.0.0}
ICEBERG_ARROW_SHA256=${ICEBERG_ARROW_SHA256:-7ecdd404862c3d312601457861a11c30bb8f520328693e1323e3db1db985dbe5}
ICEBERG_ARROW_URL=${ICEBERG_ARROW_URL:-"https://github.com/apache/arrow/archive/apache-arrow-${ICEBERG_ARROW_VERSION}.tar.gz"}

# Sources land next to the Velox dependencies. Iceberg's newer Arrow/Parquet
# must stay isolated: overwriting deps-install would replace Velox's Arrow 18.
DEPENDENCY_DIR=${DEPENDENCY_DIR:-"${ROOT_DIR}/deps-download"}
INSTALL_PREFIX=${ICEBERG_CPP_INSTALL_PREFIX:-"${ROOT_DIR}/deps-install/iceberg-cpp"}
SOURCE_DIR="${DEPENDENCY_DIR}/iceberg-cpp"
BUILD_DIR="${SOURCE_DIR}/_build"
ARCHIVE_PATH="${SOURCE_DIR}/iceberg-cpp.tar.gz"
NUM_THREADS=${NUM_THREADS:-$(getconf _NPROCESSORS_CONF 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)}
# Iceberg needs Arrow built with Parquet. Its separate source and install
# prefix prevent it from changing the Arrow used by Velox. Axiom looks for it
# in the arrow directory of the iceberg-cpp prefix, apart from iceberg-cpp's
# headers.
ARROW_SOURCE_DIR="${DEPENDENCY_DIR}/iceberg-arrow"
ARROW_BUILD_DIR="${ARROW_SOURCE_DIR}/_build"
ARROW_ARCHIVE_PATH="${ARROW_SOURCE_DIR}/arrow.tar.gz"
ARROW_PREFIX="${INSTALL_PREFIX}/arrow"

# Re-extract the sources on every run, keeping the build directory so its CMake
# and FetchContent caches survive.
clean_source_dir() {
  local source_dir=$1
  mkdir -p "${source_dir}"
  find "${source_dir}" -mindepth 1 -maxdepth 1 -not -name _build -exec rm -rf {} +
}

copy_local_iceberg_source() {
  local local_source_dir
  local_source_dir=$(cd "${ICEBERG_CPP_SOURCE_DIR}" && pwd)
  if [[ "${local_source_dir}" == "${SOURCE_DIR}" ]]; then
    echo "ICEBERG_CPP_SOURCE_DIR must not be ${SOURCE_DIR}" >&2
    exit 1
  fi

  clean_source_dir "${SOURCE_DIR}"
  rsync -a --delete --exclude .git --exclude _build/ \
    "${local_source_dir}/" "${SOURCE_DIR}/"
}

# Prints the version of the Arrow package installed in ARROW_PREFIX.
installed_arrow_version() {
  sed -n 's/^set(PACKAGE_VERSION "\([^"]*\)")$/\1/p' \
    "${ARROW_PREFIX}/lib/cmake/Arrow/ArrowConfigVersion.cmake" | head -n 1
}

build_iceberg_arrow_with_parquet() {
  if [[ -f "${ARROW_PREFIX}/lib/cmake/Arrow/ArrowConfig.cmake" &&
        -f "${ARROW_PREFIX}/lib/cmake/Parquet/ParquetConfig.cmake" ]]; then
    # An Arrow left by an earlier pin must not be reused: iceberg-cpp would be
    # compiled against a release it was not written for.
    local installed_version
    installed_version=$(installed_arrow_version || true)
    if [[ "${installed_version}" == "${ICEBERG_ARROW_VERSION}" ]]; then
      return
    fi
    echo "${ARROW_PREFIX} holds Arrow ${installed_version:-of an unknown version}," \
      "but iceberg-cpp needs Arrow ${ICEBERG_ARROW_VERSION}." \
      "Remove ${ARROW_PREFIX} and rerun this script." >&2
    exit 1
  fi

  clean_source_dir "${ARROW_SOURCE_DIR}"
  curl --fail --location --silent --show-error "${ICEBERG_ARROW_URL}" --output "${ARROW_ARCHIVE_PATH}"

  local actual_sha256
  actual_sha256=$(shasum -a 256 "${ARROW_ARCHIVE_PATH}" | awk '{print $1}')
  if [[ "${actual_sha256}" != "${ICEBERG_ARROW_SHA256}" ]]; then
    echo "Arrow archive checksum does not match the pinned Arrow ${ICEBERG_ARROW_VERSION}" >&2
    exit 1
  fi

  tar -xzf "${ARROW_ARCHIVE_PATH}" --strip-components=1 -C "${ARROW_SOURCE_DIR}"

  # Arrow builds its own dependencies, so they go into the archive that Axiom
  # hides with Arrow. A library found on this machine, such as Thrift, which
  # Velox also carries, would be shared with the rest of the process.
  cmake \
    -S "${ARROW_SOURCE_DIR}/cpp" \
    -B "${ARROW_BUILD_DIR}" \
    -DARROW_PARQUET=ON \
    -DARROW_IPC=ON \
    -DARROW_FILESYSTEM=ON \
    -DARROW_JSON=ON \
    -DARROW_DEPENDENCY_SOURCE=BUNDLED \
    -DARROW_WITH_LZ4=ON \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_WITH_ZSTD=ON \
    -DARROW_JEMALLOC=OFF \
    -DARROW_SIMD_LEVEL=NONE \
    -DARROW_RUNTIME_SIMD_LEVEL=NONE \
    -DARROW_WITH_UTF8PROC=OFF \
    -DARROW_TESTING=OFF \
    -DARROW_BUILD_STATIC=ON \
    -DARROW_BUILD_SHARED=OFF \
    -DARROW_POSITION_INDEPENDENT_CODE=ON \
    -DBUILD_WARNING_LEVEL=PRODUCTION \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DCMAKE_INSTALL_PREFIX="${ARROW_PREFIX}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -DARROW_CXXFLAGS=-Wno-documentation

  cmake --build "${ARROW_BUILD_DIR}" --target install -j "${NUM_THREADS}"
}

if [[ -n "${ICEBERG_CPP_SOURCE_DIR:-}" ]]; then
  copy_local_iceberg_source
else
  clean_source_dir "${SOURCE_DIR}"
  curl --fail --location --silent --show-error "${ICEBERG_CPP_URL}" --output "${ARCHIVE_PATH}"

  ACTUAL_SHA256=$(shasum -a 256 "${ARCHIVE_PATH}" | awk '{print $1}')
  if [[ "${ACTUAL_SHA256}" != "${ICEBERG_CPP_SHA256}" ]]; then
    echo "iceberg-cpp archive checksum does not match the pinned revision" >&2
    exit 1
  fi

  tar -xzf "${ARCHIVE_PATH}" --strip-components=1 -C "${SOURCE_DIR}"
fi

build_iceberg_arrow_with_parquet

# Axiom compiles as C++20. ICEBERG_CXX20_COMPAT makes the installed public
# headers usable from C++20; iceberg-cpp's own sources still build as C++23.
# Axiom links the static libraries, and Arrow's, into one shared library, so
# all of them are built position independent.
CONFIGURE_ARGS=(
  -S "${SOURCE_DIR}"
  -B "${BUILD_DIR}"
  -DICEBERG_CXX20_COMPAT=ON
  -DICEBERG_BUILD_STATIC=ON
  -DICEBERG_BUILD_SHARED=OFF
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON
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
  -DCMAKE_INSTALL_LIBDIR=lib
  -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}"
)

if [[ -n "${EXTRA_CMAKE_FLAGS:-}" ]]; then
  read -r -a EXTRA_ARGS <<<"${EXTRA_CMAKE_FLAGS}"
  CONFIGURE_ARGS+=("${EXTRA_ARGS[@]}")
fi

cmake "${CONFIGURE_ARGS[@]}"
cmake --build "${BUILD_DIR}" -j "${NUM_THREADS}"

# Installing over an earlier revision would keep the files it no longer ships,
# such as headers removed upstream. Clear iceberg-cpp's own files first, once
# the build has succeeded; the Arrow in ${ARROW_PREFIX} is untouched.
rm -rf "${INSTALL_PREFIX}/include/iceberg" \
  "${INSTALL_PREFIX}/lib/cmake/iceberg" \
  "${INSTALL_PREFIX}/share/doc/iceberg"
rm -f "${INSTALL_PREFIX}"/lib/libiceberg*
cmake --install "${BUILD_DIR}"

echo "iceberg-cpp built in ${BUILD_DIR}"
echo "iceberg-cpp installed to ${INSTALL_PREFIX}"
echo "Headers are available under ${INSTALL_PREFIX}/include/iceberg"
echo "Arrow and Parquet are resolved from ${ARROW_PREFIX}"
