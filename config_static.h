/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#ifndef CONFIG_STATIC_H
#define CONFIG_STATIC_H 1

/* The intention of this file is to avoid cluttering the code with #ifdefs */

//#ifdef WIN32
/* HAVE_CONFIG_H is causing problems with pthreads.h on in32 */
//#undef HAVE_CONFIG_H

//#define _WIN32_WINNT    0x0501
//#include <winsock2.h>
//#include <ws2tcpip.h>

//struct iovec {
//    size_t iov_len;
//    void* iov_base;
//};

//#include "win32/win32.h"


//#else
//#define initialize_sockets()
//#endif

#if !defined(_EVENT_NUMERIC_VERSION) || _EVENT_NUMERIC_VERSION < 0x02000000
typedef int evutil_socket_t;
#endif

#ifndef DEFAULT_ERRORLOG
#define DEFAULT_ERRORLOG ERRORLOG_STDERR
#endif

#if defined(WORDS_BIGENDIAN) && WORDS_BIGENDIAN > 1
#define ENDIAN_BIG 1
#else
#define ENDIAN_LITTLE 1
#endif

#endif
