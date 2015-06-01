# - Find SASL security library.
# This module defines
#  SASL_INCLUDE_DIR, where to find SASL headers
#  SASL_STATIC_LIBRARY, the library to use.
#  SASL_FOUND, If false, do not try to use.

set(THIRDPARTY_SASL thirdparty/cyrus-sasl-$ENV{IMPALA_CYRUS_SASL_VERSION})

set (THIRDPARTY ${CMAKE_SOURCE_DIR}/thirdparty)

set(SASL_SEARCH_LIB_PATH
  $ENV{IMPALA_TOOLCHAIN}/cyrus-sasl-$ENV{IMPALA_CYRUS_SASL_VERSION}/lib
  $ENV{IMPALA_CYRUS_SASL_INSTALL_DIR}/lib)
set(SASL_SEARCH_INCLUDE_DIR
  $ENV{IMPALA_TOOLCHAIN}/cyrus-sasl-$ENV{IMPALA_CYRUS_SASL_VERSION}/include
  $ENV{IMPALA_CYRUS_SASL_INSTALL_DIR}/include)


find_path(SASL_INCLUDE_DIR NAMES sasl/sasl.h
  PATHS ${SASL_SEARCH_INCLUDE_DIR}
  NO_DEFAULT_PATH)

find_library(SASL_STATIC_LIBRARY NAMES libsasl2.a
  PATHS ${SASL_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
        DOC   "Cyrus-sasl library"
)

if (SASL_STATIC_LIBRARY STREQUAL "SASL_STATIC_LIB-NOTFOUND" OR
    SASL_INCLUDE_DIR STREQUAL "SASL_INCLUDE_DIR-NOTFOUND")
  message(FATAL_ERROR "SASL includes and libraries NOT found.")
endif ()


mark_as_advanced(
  SASL_STATIC_LIBRARY
  SASL_INCLUDE_DIR
)
