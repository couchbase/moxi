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

#undef HAVE_CONFIG_H

// The intention of this file is to avoid cluttering the code with #ifdefs

#ifdef WIN32
#define _WIN32_WINNT    0x0501
#include <winsock2.h>
#include <ws2tcpip.h>

struct iovec {
    size_t iov_len;
    void* iov_base;
};

#include "win32/win32.h"

#define EX_USAGE EXIT_FAILURE
#define EX_OSERR EXIT_FAILURE

#else

#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <pwd.h>
#include <sys/mman.h>
#include <netinet/tcp.h>
#include <sysexits.h>
#include <sys/uio.h>
#include <sys/resource.h>

#define initialize_sockets()
#endif

#endif
