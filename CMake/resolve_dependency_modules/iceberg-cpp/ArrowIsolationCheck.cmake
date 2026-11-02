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

# Checks the Arrow/Parquet C++ symbol boundary of axiom_iceberg_cpp after linking.
#
#   cmake -DNM=<nm> -DLIBRARY=<axiom_iceberg_cpp> -P ArrowIsolationCheck.cmake
#
# Why this is needed:
# Velox and iceberg-cpp use different Arrow releases. Their C++ symbols have
# the same names even when class layouts differ. Binding code compiled against
# one release to the other's implementation can corrupt memory despite a
# successful compile and link. Separate install prefixes select the correct
# headers and archives, but do not make the linked symbols private.
#
# The linker rules in CMakeLists.txt hide iceberg-cpp's Arrow and Parquet.
# This script checks the resulting library independently of those rules:
#   - An exported Arrow/Parquet symbol makes iceberg-cpp's implementation
#     available to other code in the process, including Velox.
#   - An imported Arrow/Parquet symbol means the library still needs an
#     implementation from outside, instead of using its private copy.
# A successful link alone is insufficient: a shared dependency can satisfy a
# reference at link time while leaving the symbol imported in this library.
#
# Checking only the Arrow archives would miss template and inline definitions
# emitted into iceberg-cpp's own object files. Check the complete shared
# library, including symbols with Arrow types in their demangled signatures.
# Demangling also keeps this check independent of the mangled-name patterns
# used by the linker rules, so dependency or toolchain changes that escape
# those patterns fail the build with a concrete symbol to investigate.
#
# Scope: this checks symbol names, not every aspect of ABI compatibility. It
# does not validate header selection, detect Arrow objects passed through an
# opaque interface, or isolate dependencies outside these two namespaces.
foreach(_required IN ITEMS NM LIBRARY)
  if(NOT DEFINED ${_required})
    message(FATAL_ERROR "${_required} is not set.")
  endif()
endforeach()

# arrow:: or parquet:: not preceded by another name, which leaves out
# iceberg::arrow:: and iceberg::parquet::.
set(_arrow_pattern "(^|[^A-Za-z0-9_:])(arrow|parquet)::")

if(CMAKE_HOST_APPLE)
  # Mach-O: external defined symbols, then undefined symbols.
  set(_exported_flags -gU)
  set(_imported_flags -u)
else()
  # ELF: inspect the dynamic symbol table used for runtime symbol resolution.
  # Local symbols in the ordinary symbol table are allowed to mention Arrow.
  set(_exported_flags -D --defined-only)
  set(_imported_flags -D --undefined-only)
endif()

# Appends to 'violations' in the caller when LIBRARY has such symbols. The
# listing goes through a file because the symbol table is too large to search
# as one string.
function(check_symbols description)
  set(_listing "${LIBRARY}.symbols")
  execute_process(
    COMMAND "${NM}" -C ${ARGN} "${LIBRARY}"
    OUTPUT_FILE "${_listing}"
    ERROR_VARIABLE _error
    RESULT_VARIABLE _result
  )
  if(NOT _result EQUAL 0)
    message(FATAL_ERROR "Cannot read symbols of ${LIBRARY}: ${_error}")
  endif()
  file(STRINGS "${_listing}" _matches REGEX "${_arrow_pattern}")
  file(REMOVE "${_listing}")
  list(LENGTH _matches _count)
  if(_count GREATER 0)
    list(GET _matches 0 _first)
    string(STRIP "${_first}" _first)
    list(APPEND violations "${description} ${_count} Arrow symbols, such as: ${_first}")
    set(violations "${violations}" PARENT_SCOPE)
  endif()
endfunction()

set(violations "")
check_symbols("exports" ${_exported_flags})
check_symbols("imports" ${_imported_flags})

if(violations)
  list(JOIN violations "\n  " _report)
  message(FATAL_ERROR "iceberg-cpp's Arrow is visible outside ${LIBRARY}:\n  ${_report}")
endif()
