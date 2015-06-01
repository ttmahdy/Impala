# - Find GLOG (logging.h, libglog.a, libglog.so, and libglog.so.0)
# This module defines
#  GLOG_INCLUDE_DIR, directory containing headers
#  GLOG_LIBS, directory containing glog libraries
#  GLOG_STATIC_LIB, path to libglog.a (
#  GLOG_FOUND, whether glog has been found

set(THIRDPARTY ${CMAKE_SOURCE_DIR}/thirdparty)
set(GLOG_SEARCH_HEADER_PATHS
  $ENV{IMPALA_TOOLCHAIN}/glog-$ENV{IMPALA_GLOG_VERSION}/include
  ${THIRDPARTY}/glog-$ENV{IMPALA_GLOG_VERSION}/src
)
set(GLOG_SEARCH_LIB_PATH
  $ENV{IMPALA_TOOLCHAIN}/glog-$ENV{IMPALA_GLOG_VERSION}/lib
  ${THIRDPARTY}/glog-$ENV{IMPALA_GLOG_VERSION}/.libs
)

find_path(GLOG_INCLUDE_DIR glog/logging.h PATHS
  ${GLOG_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(GLOG_LIBS NAMES glog PATHS ${GLOG_SEARCH_LIB_PATH})
find_library(GLOG_STATIC_LIB NAMES libglog.a PATHS ${GLOG_SEARCH_LIB_PATH})

if (GLOG_LIBS STREQUAL "GLOG_LIBS-NOTFOUND" OR
    GLOG_STATIC_LIB STREQUAL "GLOG_STATIC_LIB-NOTFOUND")
  message(FATAL_ERROR "GLog includes and libraries NOT found. "
    "Looked for headers in ${GLOG_SEARCH_HEADER_PATH}, "
    "and for libs in ${GLOG_SEARCH_LIB_PATH}")
endif ()

mark_as_advanced(
  GLOG_INCLUDE_DIR
  GLOG_LIBS
  GLOG_STATIC_LIB
)
