# In order to statically link in the Boost, bz2, event and z libraries, they
# needs to be recompiled with -fPIC. Set $PIC_LIB_PATH to the location of
# these libraries in the environment, or dynamic linking will be used instead.

# If the toolchain is present, we have all the libraries compiled with -fpic, so we don't
# need this check
if (NOT IMPALA_TOOLCHAIN)
  IF (DEFINED ENV{PIC_LIB_PATH})
    set(CMAKE_SKIP_RPATH TRUE)
    set(Boost_USE_STATIC_LIBS ON)
    set(Boost_USE_STATIC_RUNTIME ON)
    set(LIBBZ2 $ENV{PIC_LIB_PATH}/lib/libbz2.a)
    set(LIBZ $ENV{PIC_LIB_PATH}/lib/libz.a)
  ELSE (DEFINED ENV{PIC_LIB_PATH})
    set(Boost_USE_STATIC_LIBS OFF)
    set(Boost_USE_STATIC_RUNTIME OFF)
    set(LIBBZ2 -lbz2)
    set(LIBZ -lz)
  ENDIF (DEFINED ENV{PIC_LIB_PATH})
else ()
  if(NOT BUILD_SHARED_LIBS)
    set(CMAKE_SKIP_RPATH TRUE)
    set(Boost_USE_STATIC_LIBS ON)
    set(Boost_USE_STATIC_RUNTIME ON)
    set(LIBBZ2 $ENV{IMPALA_TOOLCHAIN}/bzip2-$ENV{IMPALA_BZIP2_VERSION}/lib/libbz2.a)
    set(LIBZ $ENV{IMPALA_TOOLCHAIN}/zlib-$ENV{IMPALA_ZLIB_VERSION}/lib/libz.a)
    message(STATUS "zlib ${LIBZ}")
  else()
    set(LIBBZ2 -lbz2)
    set(LIBZ -lz)
  endif ()
endif ()
