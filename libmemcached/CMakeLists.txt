PROJECT(libmemcached)
CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

ADD_LIBRARY(hashkit SHARED
            libhashkit/algorithm.c
            libhashkit/behavior.c
            libhashkit/crc32.c
            libhashkit/fnv.c
            libhashkit/digest.c
            libhashkit/function.c
            libhashkit/hashkit.c
            libhashkit/jenkins.c
            libhashkit/ketama.c
            libhashkit/md5.c
            libhashkit/one_at_a_time.c
            libhashkit/strerror.c)
SET_TARGET_PROPERTIES(hashkit PROPERTIES SOVERSION 1.0.0)
SET_TARGET_PROPERTIES(hashkit PROPERTIES
                      COMPILE_FLAGS -DBUILDING_HASHKIT=1)


INCLUDE(CheckFunctionExists)
CHECK_SYMBOL_EXISTS(htonll arpa/inet.h HAVE_HTONLL)
CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/libmemcached_config.h.in
                ${CMAKE_CURRENT_BINARY_DIR}/libmemcached_config.h)


IF (WIN32)
  SET(LMCD_EXTRA libmemcached/win32.c)
ENDIF(WIN32)

ADD_LIBRARY(mcd SHARED
            libmemcached/allocators.c libmemcached/analyze.c
            libmemcached/auto.c libmemcached/behavior.c
            libmemcached/byteorder.c libmemcached/connect.c
            libmemcached/delete.c libmemcached/do.c
            libmemcached/dump.c libmemcached/fetch.c
            libmemcached/flush.c libmemcached/flush_buffers.c
            libmemcached/get.c libmemcached/hash.c
            libmemcached/hosts.c libmemcached/io.c libmemcached/key.c
            libmemcached/memcached.c libmemcached/parse.c
            libmemcached/purge.c libmemcached/quit.c
            libmemcached/response.c libmemcached/result.c
            libmemcached/server.c libmemcached/server_list.c
            libmemcached/stats.c libmemcached/string.c
            libmemcached/storage.c
            libmemcached/strerror.c libmemcached/verbosity.c
            libmemcached/version.c ${LMCD_EXTRA})

SET_TARGET_PROPERTIES(mcd PROPERTIES SOVERSION 6.1.0)

IF (WIN32)
    INCLUDE_DIRECTORIES(BEFORE ${PROJECT_SOURCE_DIR}
                               ${PROJECT_SOURCE_DIR}/win32)
ELSE (WIN32)
    INCLUDE_DIRECTORIES(BEFORE ${PROJECT_SOURCE_DIR})
    TARGET_LINK_LIBRARIES(hashkit m)
ENDIF (WIN32)

INCLUDE_DIRECTORIES(BEFORE ${PROJECT_BINARY_DIR})

TARGET_LINK_LIBRARIES(mcd hashkit ${COUCHBASE_NETWORK_LIBS})

IF (INSTALL_HEADER_FILES)
   INSTALL(FILES
           libhashkit/algorithm.h libhashkit/behavior.h
           libhashkit/configure.h libhashkit/digest.h
           libhashkit/function.h libhashkit/hashkit.h
           libhashkit/strerror.h libhashkit/types.h
           libhashkit/visibility.h
           DESTINATION include/libhashkit)

   INSTALL(FILES
           libmemcached/allocators.h libmemcached/analyze.h
           libmemcached/auto.h libmemcached/behavior.h
           libmemcached/callback.h libmemcached/configure.h
           libmemcached/constants.h libmemcached/delete.h
           libmemcached/dump.h libmemcached/exception.hpp
           libmemcached/fetch.h libmemcached/flush.h
           libmemcached/flush_buffers.h libmemcached/get.h
           libmemcached/hash.h libmemcached/memcached.h
           libmemcached/memcached.hpp
           libmemcached/memcached/protocol_binary.h
           libmemcached/parse.h libmemcached/platform.h
           libmemcached/quit.h libmemcached/result.h
           libmemcached/sasl.h libmemcached/server.h
           libmemcached/server_list.h libmemcached/stats.h
           libmemcached/storage.h libmemcached/strerror.h
           libmemcached/string.h libmemcached/types.h
           libmemcached/verbosity.h libmemcached/version.h
           libmemcached/visibility.h libmemcached/watchpoint.h
           DESTINATION include/libmemcached)
ENDIF (INSTALL_HEADER_FILES)

INSTALL(TARGETS hashkit mcd
  RUNTIME DESTINATION bin
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
)
