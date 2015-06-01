# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# where to put generated libraries.
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/service")

# where to put generated binaries.
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/service")

add_library(Service
  frontend.cc
  fe-support.cc
  fragment-exec-state.cc
  fragment-mgr.cc
  hs2-util.cc
  impala-server.cc
  impala-server-callbacks.cc
  impala-hs2-server.cc
  impala-beeswax-server.cc
  query-exec-state.cc
  query-options.cc
  child-query.cc
)
add_dependencies(Service thrift-cpp)

# this shared library provides Impala executor functionality to FE test.
add_library(fesupport SHARED
  fe-support.cc
)
add_dependencies(fesupport thrift-cpp)

set(FE_SUPPORT_LIBS ${IMPALA_LINK_LIBS_NO_TCMALLOC})

# When we use the toolchain, and link statically, we need to replace the static libhdfs.a
# with the dynamic version
if (IMPALA_TOOLCHAIN AND NOT BUILD_SHARED_LIBS)
  list(REMOVE_ITEM FE_SUPPORT_LIBS hdfsstatic)
  set(FE_SUPPORT_LIBS ${FE_SUPPORT_LIBS} ${HDFS_SO_LIB})
endif()
target_link_libraries(fesupport ${FE_SUPPORT_LIBS})

add_executable(impalad
  impalad-main.cc
)

# This permits libraries loaded by dlopen to link to the symbols in the program.
set_target_properties(impalad PROPERTIES LINK_FLAGS -rdynamic)

target_link_libraries(impalad
  ${JAVA_JSIG_LIBRARY}
  ${IMPALA_LINK_LIBS}
)

ADD_BE_TEST(session-expiry-test session-expiry-test.cc)
ADD_BE_TEST(hs2-util-test hs2-util-test.cc)
