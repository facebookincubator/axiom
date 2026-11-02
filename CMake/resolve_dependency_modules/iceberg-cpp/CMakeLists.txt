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

# Builds axiom_iceberg_cpp, the only way Axiom targets link iceberg-cpp.
#
# iceberg-cpp is compiled against a newer Arrow than the one inside Velox. The
# two define the same symbols with different class layouts, so code compiled
# against one must never run functions from the other. Symbols are bound by
# name, both when a binary is linked and when a shared library is loaded, so
# the two Arrows must not be inputs to the same link or visible to each other
# afterwards.
#
# axiom_iceberg_cpp is therefore a shared library holding all of iceberg-cpp and
# its Arrow. iceberg-cpp's Arrow calls are bound when this library is linked,
# and it exports nothing that mentions Arrow. That includes iceberg-cpp
# functions with an Arrow type in their signature: they cannot be called from
# outside the library. All of iceberg-cpp is inside, so its registries exist
# once. Code that links the library sees iceberg-cpp's headers but not its
# Arrow's.

# iceberg-cpp is installed in the prefix and its Arrow in the prefix's arrow
# directory. Point each package at its copy, and only at it: another
# iceberg-cpp or Arrow package found on the search path, such as Velox's Arrow
# or an older iceberg-cpp in the common prefix, would build against the wrong
# release. Neither prefix goes on CMAKE_PREFIX_PATH: Velox finds its own Arrow
# by library and header name, and would otherwise pick up this library while
# compiling against the headers in the common prefix. These are normal
# variables, so find_package() uses them without recording them in the cache.
set(iceberg_DIR "${AXIOM_ICEBERG_CPP_PREFIX}/lib/cmake/iceberg")
set(Arrow_DIR "${AXIOM_ICEBERG_CPP_PREFIX}/arrow/lib/cmake/Arrow")
set(Parquet_DIR "${AXIOM_ICEBERG_CPP_PREFIX}/arrow/lib/cmake/Parquet")
foreach(
  _iceberg_package_config
  IN
  ITEMS
    "${iceberg_DIR}/iceberg-config.cmake"
    "${Arrow_DIR}/ArrowConfig.cmake"
    "${Parquet_DIR}/ParquetConfig.cmake"
)
  if(NOT EXISTS "${_iceberg_package_config}")
    message(
      FATAL_ERROR
      "iceberg-cpp package not found. Run scripts/setup-iceberg-cpp.sh, or set "
      "AXIOM_ICEBERG_CPP_PREFIX to its install prefix. Missing: "
      "${_iceberg_package_config}"
    )
  endif()
endforeach()

# Code that uses iceberg-cpp gets its include directory. Arrow headers there
# would let that code compile against iceberg-cpp's Arrow while it runs
# Velox's.
foreach(_iceberg_arrow_headers IN ITEMS arrow parquet)
  if(EXISTS "${AXIOM_ICEBERG_CPP_PREFIX}/include/${_iceberg_arrow_headers}")
    message(
      FATAL_ERROR
      "iceberg-cpp shares its include directory with Arrow. Remove "
      "AXIOM_ICEBERG_CPP_PREFIX and rerun scripts/setup-iceberg-cpp.sh, which "
      "installs Arrow in a directory of its own. Found: "
      "${AXIOM_ICEBERG_CPP_PREFIX}/include/${_iceberg_arrow_headers}"
    )
  endif()
endforeach()

# The package config sets ICEBERG_CXX20_COMPAT. Start from OFF so that a cache
# entry of the same name cannot stand in for a package that does not set it.
set(ICEBERG_CXX20_COMPAT OFF)
find_package(iceberg CONFIG REQUIRED COMPONENTS REST)

set(
  _iceberg_targets
  iceberg::iceberg_static
  iceberg::iceberg_data_static
  iceberg::iceberg_rest_static
  iceberg::iceberg_bundle_static
)
foreach(_iceberg_target IN LISTS _iceberg_targets)
  if(NOT TARGET ${_iceberg_target})
    message(
      FATAL_ERROR
      "Installed iceberg-cpp package is missing a library Axiom needs to plan "
      "Iceberg manifests: the static core, data, REST and bundled Avro "
      "libraries are required. Run scripts/setup-iceberg-cpp.sh, or set "
      "AXIOM_ICEBERG_CPP_PREFIX to its install prefix. Missing: "
      "${_iceberg_target}"
    )
  endif()
endforeach()

# Axiom includes iceberg-cpp's public headers from C++20 translation units.
# iceberg-cpp fixes its public API mode when it is built and exports the choice
# from its package config, so reject a default (C++23-only) package here rather
# than at the first compile error.
if(NOT ICEBERG_CXX20_COMPAT)
  message(
    FATAL_ERROR
    "Installed iceberg-cpp package was not built with "
    "ICEBERG_CXX20_COMPAT=ON, so its public headers require C++23. "
    "Reinstall it using scripts/setup-iceberg-cpp.sh."
  )
endif()

if(TARGET iceberg::arrow_static OR TARGET iceberg::parquet_static)
  message(
    FATAL_ERROR
    "Installed iceberg-cpp package still exports vendored Arrow or Parquet. "
    "Reinstall it using scripts/setup-iceberg-cpp.sh so iceberg-cpp uses its "
    "isolated Arrow package."
  )
endif()

if(
  NOT EXISTS "${AXIOM_ICEBERG_CPP_PREFIX}/include/iceberg/catalog/rest/rest_catalog.h"
  OR NOT EXISTS "${AXIOM_ICEBERG_CPP_PREFIX}/include/iceberg/avro/avro_register.h"
)
  message(
    FATAL_ERROR
    "Installed iceberg-cpp package is missing REST or Avro headers. "
    "Reinstall it using scripts/setup-iceberg-cpp.sh."
  )
endif()

# The libraries that hold iceberg-cpp's Arrow. Arrow installs its bundled
# dependencies as an archive of their own only when it bundled any.
set(_iceberg_arrow_targets Arrow::arrow_static Parquet::parquet_static)
get_target_property(_arrow_dependencies Arrow::arrow_static INTERFACE_LINK_LIBRARIES)
if("Arrow::arrow_bundled_dependencies" IN_LIST _arrow_dependencies)
  list(APPEND _iceberg_arrow_targets Arrow::arrow_bundled_dependencies)
endif()

file(
  CONFIGURE
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/axiom_iceberg_cpp.cpp"
  CONTENT "// axiom_iceberg_cpp is made of iceberg-cpp's static libraries.\n"
)
add_library(axiom_iceberg_cpp SHARED "${CMAKE_CURRENT_BINARY_DIR}/axiom_iceberg_cpp.cpp")

# Users of the library compile with the usage requirements of iceberg-cpp's
# core, data and REST libraries. The bundle library is left out because its
# requirements include its Arrow's headers; its own headers are in the same
# directory as the others.
target_link_libraries(
  axiom_iceberg_cpp
  PRIVATE ${_iceberg_targets} ${_iceberg_arrow_targets}
  INTERFACE
    "$<COMPILE_ONLY:iceberg::iceberg_rest_static>"
    "$<COMPILE_ONLY:iceberg::iceberg_data_static>"
)

# iceberg-cpp's libraries go in whole, whichever dependency names them, since
# nothing in this library refers to them.
foreach(_iceberg_target IN LISTS _iceberg_targets)
  set_property(
    TARGET axiom_iceberg_cpp
    PROPERTY LINK_LIBRARY_OVERRIDE_${_iceberg_target} WHOLE_ARCHIVE
  )
endforeach()

# Its Arrow stays inside. iceberg-cpp's own objects hold Arrow templates and
# inline functions compiled from the newer headers, so hiding the Arrow
# libraries is not enough: every symbol that mentions the arrow or parquet
# namespace is kept from the exports as well. A mangled name spells such a
# namespace as N, then the qualifiers of a member function (up to four of r, V,
# K, R and O), then the namespace's length and name.
set(_iceberg_private_symbols "")
foreach(_iceberg_namespace IN ITEMS 5arrow 7parquet)
  set(_iceberg_qualifiers "")
  foreach(_iceberg_num_qualifiers RANGE 4)
    list(APPEND _iceberg_private_symbols "*N${_iceberg_qualifiers}${_iceberg_namespace}*")
    string(APPEND _iceberg_qualifiers "[rVKRO]")
  endforeach()
endforeach()

if(APPLE)
  # Links a static library with every symbol it defines hidden.
  set(CMAKE_LINK_LIBRARY_USING_axiom_load_hidden "LINKER:-load_hidden,<LIBRARY>")
  set(CMAKE_LINK_LIBRARY_USING_axiom_load_hidden_SUPPORTED TRUE)
  foreach(_iceberg_target IN LISTS _iceberg_arrow_targets)
    set_property(
      TARGET axiom_iceberg_cpp
      PROPERTY LINK_LIBRARY_OVERRIDE_${_iceberg_target} axiom_load_hidden
    )
  endforeach()
  list(JOIN _iceberg_private_symbols "\n" _iceberg_symbol_list)
  set(_iceberg_symbol_file "${CMAKE_CURRENT_BINARY_DIR}/axiom_iceberg_cpp.unexported")
  file(CONFIGURE OUTPUT "${_iceberg_symbol_file}" CONTENT "${_iceberg_symbol_list}\n")
  target_link_options(
    axiom_iceberg_cpp
    PRIVATE "LINKER:-unexported_symbols_list,${_iceberg_symbol_file}"
  )
else()
  set(_iceberg_hidden_archives "")
  foreach(_iceberg_target IN LISTS _iceberg_arrow_targets)
    list(APPEND _iceberg_hidden_archives "$<TARGET_FILE_NAME:${_iceberg_target}>")
  endforeach()
  list(JOIN _iceberg_hidden_archives ":" _iceberg_hidden_archives)
  list(JOIN _iceberg_private_symbols ";\n    " _iceberg_symbol_list)
  set(_iceberg_symbol_file "${CMAKE_CURRENT_BINARY_DIR}/axiom_iceberg_cpp.map")
  file(
    CONFIGURE
    OUTPUT "${_iceberg_symbol_file}"
    CONTENT "{\n  local:\n    ${_iceberg_symbol_list};\n};\n"
  )
  # -z defs makes every reference resolve in this link. An Arrow call left to
  # the loader would bind to whichever Arrow the process has.
  target_link_options(
    axiom_iceberg_cpp
    PRIVATE
      "LINKER:--exclude-libs,${_iceberg_hidden_archives}"
      "LINKER:--version-script=${_iceberg_symbol_file}"
      "LINKER:-z,defs"
  )
endif()
set_property(TARGET axiom_iceberg_cpp APPEND PROPERTY LINK_DEPENDS "${_iceberg_symbol_file}")

# A leak corrupts memory without failing any test reliably, so every link of
# the library checks its symbol table.
add_custom_command(
  TARGET axiom_iceberg_cpp
  POST_BUILD
  COMMAND
    "${CMAKE_COMMAND}" "-DNM=${CMAKE_NM}" "-DLIBRARY=$<TARGET_FILE:axiom_iceberg_cpp>" -P
    "${CMAKE_CURRENT_LIST_DIR}/ArrowIsolationCheck.cmake"
  VERBATIM
)

message(STATUS "Using iceberg-cpp from ${AXIOM_ICEBERG_CPP_PREFIX}")
