# - Find re2 headers and lib.
# This module defines
#  RE2_INCLUDE_DIR, directory containing headers
#  RE2_STATIC_LIB, path to libsnappy.a
#  RE2_FOUND, whether gflags has been found

set(RE2_SEARCH_HEADER_PATHS
  $ENV{IMPALA_TOOLCHAIN}/re2-$ENV{IMPALA_RE2_VERSION}/include
  ${CMAKE_SOURCE_DIR}/thirdparty/re2
)

set(RE2_SEARCH_LIB_PATHS
  $ENV{IMPALA_TOOLCHAIN}/re2-$ENV{IMPALA_RE2_VERSION}/lib
  ${CMAKE_SOURCE_DIR}/thirdparty/re2/obj
)

find_path(RE2_INCLUDE_DIR re2/re2.h
  PATHS ${RE2_SEARCH_HEADER_PATHS}
        NO_DEFAULT_PATH
  DOC  "Google's re2 regex header path"
)

find_library(RE2_LIBS NAMES re2
  PATHS ${RE2_SEARCH_LIB_PATHS}
        NO_DEFAULT_PATH
  DOC   "Google's re2 regex library"
)

find_library(RE2_STATIC_LIB NAMES libre2.a
  PATHS ${RE2_SEARCH_LIB_PATHS}
        NO_DEFAULT_PATH
  DOC   "Google's re2 regex static library"
)

message(STATUS ${RE2_INCLUDE_DIR})

if (RE2_INCLUDE_DIR STREQUAL "RE2_INCLUDE_DIR-NOTFOUND" OR
    RE2_LIBS STREQUAL "RE2_LIBS-NOTFOUND" OR
    RE2_STATIC_LIB STREQUAL "RE2_STATIC_LIB-NOTFOUND")
  message(FATAL_ERROR "Re2 includes and libraries NOT found. "
    "Looked for headers in ${RE2_SEARCH_HEADER_PATH}, "
    "and for libs in ${RE2_SEARCH_LIB_PATH}")
endif ()

mark_as_advanced(
  RE2_INCLUDE_DIR
  RE2_LIBS
  RE2_STATIC_LIB
)
