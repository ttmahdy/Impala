# - Find Avro (headers and libavrocpp_s.a)
# This module defines
#  AVRO_INCLUDE_DIR, directory containing headers
#  AVRO_LIBS, directory containing Avro libraries
#  AVRO_STATIC_LIB, path to libavrocpp_s.a
#  AVRO_FOUND, whether Avro has been found
set(AVRO_SEARCH_HEADER_PATHS
  $ENV{IMPALA_TOOLCHAIN}/avro-$ENV{IMPALA_AVRO_VERSION}/include
  ${CMAKE_SOURCE_DIR}/thirdparty/avro-c-$ENV{IMPALA_AVRO_VERSION}/src)

set(AVRO_SEARCH_LIB_PATH
  $ENV{IMPALA_TOOLCHAIN}/avro-$ENV{IMPALA_AVRO_VERSION}/lib
  ${CMAKE_SOURCE_DIR}/thirdparty/avro-c-$ENV{IMPALA_AVRO_VERSION}/src)

find_path(AVRO_INCLUDE_DIR NAMES avro/schema.h schema.h PATHS
  ${AVRO_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH)

find_library(AVRO_STATIC_LIB NAMES libavro.a PATHS ${AVRO_SEARCH_LIB_PATH})

if(AVRO_STATIC_LIB STREQUAL "AVRO_STATIC_LIB-NOTFOUND")
  message(FATAL_ERROR "Avro includes and libraries NOT found. "
    "Looked for headers in ${AVRO_SEARCH_HEADER_PATHS}, "
    "and for libs in ${AVRO_SEARCH_LIB_PATH}")
endif ()

mark_as_advanced(
  AVRO_INCLUDE_DIR
  AVRO_STATIC_LIB
)
