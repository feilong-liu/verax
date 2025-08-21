<!--
Copyright (c) Facebook, Inc. and its affiliates.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Presto SQL Parser

The SQL parser here is almost an exact copy of the Presto Java SQL parser but with a few changes to make it work in C++ due to C++ keywords. The original Presto Java grammar is located in [Presto SQL grammar](https://github.com/prestodb/presto/blob/master/presto-parser/src/main/antlr4/com/facebook/presto/sql/parser/SqlBase.g4).

The generated visitor APIs are meant to be implemented in order to create the AST classes representing the user defined ANTLR4 grammar.

The ANTLR4 generated code is now automatically generated as part of the CMAKE build process (see https://github.com/antlr/antlr4/tree/dev/runtime/Cpp). When you update the `PrestoSql.g4` grammar file, the build process will automatically regenerate the ANTLR4 C++ code during compilation. You will then need to implement any new visitor APIs and create the respective AST classes for any grammar changes.

## Build Requirements

- **Java Runtime Environment (REQUIRED)**: ANTLR4 code generation requires Java to be installed
- ANTLR4 C++ runtime and JAR are automatically managed by the CMake dependency system
- **The build will fail if Java is not available** - this ensures generated code is always up-to-date

## ANTLR4 Integration

The ANTLR4 code generation is centrally managed by `/CMake/resolve_dependency_modules/antlr4-runtime.cmake`, which:

1. Downloads the ANTLR4 C++ runtime
2. Configures the ANTLR4 JAR for code generation
3. Sets up the FindANTLR module paths
4. Validates that Java is available for code generation

This centralized approach ensures consistent ANTLR4 setup across all projects in the repository. **The build enforces that ANTLR4 code generation is working properly** rather than falling back to potentially outdated manually committed files.

### Error Resolution

If you encounter an error like:
```
ANTLR4 code generation is required but not available. Please ensure Java Runtime Environment is installed.
```

Install Java using your system package manager:
- **macOS**: `brew install openjdk`
- **Ubuntu/Debian**: `sudo apt-get install default-jre`
- **CentOS/RHEL**: `sudo yum install java-11-openjdk`
