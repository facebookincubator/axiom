# Copyright (c) Facebook, Inc. and its affiliates.
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
include_guard(GLOBAL)
function(axiom_get_rpath_origin VAR)
  if(APPLE)
    set(_origin @loader_path)
  else()
    set(_origin "\$ORIGIN")
  endif()
  set(${VAR} ${_origin} PARENT_SCOPE)
endfunction()

# TODO use file sets
function(axiom_install_library_headers)
  # Find any headers and install them relative to the source tree in include.
  file(GLOB _hdrs "*.h")
  if(NOT "${_hdrs}" STREQUAL "")
    cmake_path(
      RELATIVE_PATH
      CMAKE_CURRENT_SOURCE_DIR
      BASE_DIRECTORY "${CMAKE_SOURCE_DIR}"
      OUTPUT_VARIABLE _hdr_dir
    )
    install(FILES ${_hdrs} DESTINATION include/${_hdr_dir})
  endif()
endfunction()

# Base add axiom library call to add a library and install it.
function(axiom_base_add_library TARGET)
  add_library(${TARGET} ${ARGN})
  install(TARGETS ${TARGET} DESTINATION lib/axiom)
  axiom_install_library_headers()
endfunction()

# This is extremely hackish but presents an easy path to installation.
function(axiom_add_library TARGET)
  set(options OBJECT STATIC SHARED INTERFACE)
  set(oneValueArgs)
  set(multiValueArgs)
  cmake_parse_arguments(AXIOM "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

  # Remove library type specifiers from ARGN
  set(library_type)
  if(AXIOM_OBJECT)
    set(library_type OBJECT)
  elseif(AXIOM_STATIC)
    set(library_type STATIC)
  elseif(AXIOM_SHARED)
    set(library_type SHARED)
  elseif(AXIOM_INTERFACE)
    set(library_type INTERFACE)
  endif()

  list(REMOVE_ITEM ARGN OBJECT)
  list(REMOVE_ITEM ARGN STATIC)
  list(REMOVE_ITEM ARGN SHARED)
  list(REMOVE_ITEM ARGN INTERFACE)
  # Propagate to the underlying add_library and then install the target.
  if(AXIOM_MONO_LIBRARY)
    if(TARGET axiom)
      # Target already exists, append sources to it.
      target_sources(axiom PRIVATE ${ARGN})
    else()
      set(_type STATIC)
      if(AXIOM_BUILD_SHARED)
        set(_type SHARED)
      endif()
      # Create the target if this is the first invocation.
      add_library(axiom ${_type} ${ARGN})
      set_target_properties(axiom PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
      set_target_properties(axiom PROPERTIES ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
      install(TARGETS axiom DESTINATION lib/axiom EXPORT axiom_targets)
    endif()
    # create alias for compatability
    if(NOT TARGET ${TARGET})
      add_library(${TARGET} ALIAS axiom)
    endif()
  else()
    # Create a library for each invocation.
    axiom_base_add_library(${TARGET} ${library_type} ${ARGN})
  endif()
  axiom_install_library_headers()
endfunction()

function(axiom_link_libraries TARGET)
  # TODO(assignUser): Handle scope keywords (they currently are empty calls ala
  # target_link_libraries(target PRIVATE))
  if(AXIOM_MONO_LIBRARY)
    # These targets follow the axiom_* name for consistency but are NOT actually
    # aliases to axiom when building the mono lib and need to be linked
    # explicitly (this is a hack)
    set(explicit_targets)

    foreach(_arg ${ARGN})
      list(FIND explicit_targets ${_arg} _explicit)
      if(_explicit EQUAL -1 AND "${_arg}" MATCHES "^axiom_*")
        message(DEBUG "\t\tDROP: ${_arg}")
      else()
        message(DEBUG "\t\tADDING: ${_arg}")
        target_link_libraries(axiom ${_arg})
      endif()
    endforeach()
  else()
    target_link_libraries(${TARGET} ${ARGN})
  endif()
endfunction()

function(axiom_include_directories TARGET)
  if(AXIOM_MONO_LIBRARY)
    target_include_directories(axiom ${ARGN})
  else()
    target_include_directories(${TARGET} ${ARGN})
  endif()
endfunction()

function(axiom_compile_definitions TARGET)
  if(AXIOM_MONO_LIBRARY)
    target_compile_definitions(axiom ${ARGN})
  else()
    target_compile_definitions(${TARGET} ${ARGN})
  endif()
endfunction()

function(axiom_sources TARGET)
  if(AXIOM_MONO_LIBRARY)
    target_sources(axiom ${ARGN})
  else()
    target_sources(${TARGET} ${ARGN})
  endif()
endfunction()
