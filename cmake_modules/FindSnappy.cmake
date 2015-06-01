# - Find SNAPPY (snappy.h, libsnappy.a, libsnappy.so, and libsnappy.so.1)
# This module defines
#  SNAPPY_INCLUDE_DIR, directory containing headers
#  SNAPPY_LIBS, directory containing gflag libraries
#  SNAPPY_STATIC_LIB, path to libsnappy.a
#  SNAPPY_FOUND, whether gflags has been found

set(SNAPPY_SEARCH_HEADER_PATHS
  $ENV{IMPALA_TOOLCHAIN}/snappy-$ENV{IMPALA_SNAPPY_VERSION}/include
  ${CMAKE_SOURCE_DIR}/thirdparty/snappy-1.0.5/build/include
)

set(SNAPPY_SEARCH_LIB_PATH
  $ENV{IMPALA_TOOLCHAIN}/snappy-$ENV{IMPALA_SNAPPY_VERSION}/lib
  ${CMAKE_SOURCE_DIR}/thirdparty/snappy-1.0.5/build/lib
)

find_path(SNAPPY_INCLUDE_DIR
  NAMES snappy.h
  PATHS ${SNAPPY_SEARCH_HEADER_PATHS}
  NO_DEFAULT_PATH)

find_library(SNAPPY_LIBS NAMES snappy
  PATHS ${SNAPPY_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
  DOC   "Google's snappy compression library"
)

find_library(SNAPPY_STATIC_LIB NAMES libsnappy.a
  PATHS ${SNAPPY_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
  DOC   "Google's snappy compression static library"
)

if (SNAPPY_LIBS STREQUAL "SNAPPY_LIBS-NOTFOUND" OR
    SNAPPY_STATIC_LIB STREQUAL "SNAPPY_STATIC_LIB-NOTFOUND")
  message(FATAL_ERROR "Snappy includes and libraries NOT found. "
    "Looked for headers in ${SNAPPY_SEARCH_HEADER_PATH}, "
    "and for libs in ${SNAPPY_SEARCH_LIB_PATH}")
endif ()

mark_as_advanced(
  SNAPPY_INCLUDE_DIR
  SNAPPY_LIBS
  SNAPPY_STATIC_LIB
)
