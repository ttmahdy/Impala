# - Find GFLAGS (gflags.h, libgflags.a, libgflags.so, and libgflags.so.0)
# This module defines
#  GFLAGS_INCLUDE_DIR, directory containing headers
#  GFLAGS_LIBS, directory containing gflag libraries
#  GFLAGS_STATIC_LIB, path to libgflags.a
#  GFLAGS_FOUND, whether gflags has been found

set(GFLAGS_SEARCH_HEADER_PATHS
  $ENV{IMPALA_TOOLCHAIN}/gflags-$ENV{IMPALA_GFLAGS_VERSION}/include
  ${CMAKE_SOURCE_DIR}/thirdparty/gflags-$ENV{IMPALA_GFLAGS_VERSION}/src
)

set(GFLAGS_SEARCH_LIB_PATH
  $ENV{IMPALA_TOOLCHAIN}/gflags-$ENV{IMPALA_GFLAGS_VERSION}/lib
  ${CMAKE_SOURCE_DIR}/thirdparty/gflags-$ENV{IMPALA_GFLAGS_VERSION}/.libs
)

find_path(GFLAGS_INCLUDE_DIR gflags/gflags.h PATHS
  ${GFLAGS_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(GFLAGS_LIBS NAMES gflags PATHS ${GFLAGS_SEARCH_LIB_PATH})
find_library(GFLAGS_STATIC_LIB NAMES libgflags.a PATHS ${GFLAGS_SEARCH_LIB_PATH})

if (GFLAGS_LIBS STREQUAL "GFLAGS_LIBS-NOTFOUND" OR
    GFLAGS_STATIC_LIB STREQUAL "GFLAGS_STATIC_LIB-NOTFOUND")
  message(FATAL_ERROR "GFlags includes and libraries NOT found. "
    "Looked for headers in ${GFLAGS_SEARCH_HEADER_PATHS}, "
    "and for libs in ${GFLAGS_SEARCH_LIB_PATH}")
endif ()

mark_as_advanced(
  GFLAGS_INCLUDE_DIR
  GFLAGS_LIBS
  GFLAGS_STATIC_LIB
)
