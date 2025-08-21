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

# Require ANTLR4 code generation to be available
if(NOT ANTLR4_AVAILABLE)
  message(
    FATAL_ERROR
      "ANTLR4 code generation is required but not available. "
      "Please ensure Java Runtime Environment is installed. "
      "Java is required for ANTLR4 to generate C++ code from PrestoSql.g4")
endif()

# Include ANTLR module (path was set up by antlr4-runtime.cmake)
include(FindANTLR)

# Generate ANTLR4 target for PrestoSql grammar
antlr_target(
  PrestoSqlGenerated
  PrestoSql.g4
  VISITOR
  LISTENER
  OUTPUT_DIRECTORY
  ${CMAKE_CURRENT_BINARY_DIR}/antlr4_generated)

# Set include directories for generated files
include_directories(${CMAKE_CURRENT_BINARY_DIR}/antlr4_generated)

message(STATUS "ANTLR4 code generation enabled for PrestoSql grammar")

add_subdirectory(tests)

# Create the library with auto-generated files
add_library(velox_presto_grammar_lib ${ANTLR_PrestoSqlGenerated_CXX_OUTPUTS})

target_include_directories(
  velox_presto_grammar_lib PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/antlr4_generated
                                  ${CMAKE_CURRENT_SOURCE_DIR})

# Always add dependency on ANTLR4 runtime
add_dependencies(velox_presto_grammar_lib antlr4_shared)

target_link_libraries(
  velox_presto_grammar_lib antlr4_shared)
