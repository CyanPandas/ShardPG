/*
 * enospc_inject.c
 *
 * LD_PRELOAD library that intercepts write() for pg_parwal files on worker1.
 * When /tmp/enospc_inject_active exists, all writes to the worker1 pg_parwal
 * directory return ENOSPC.  Removing the file re-enables writes (simulates
 * space being freed).
 *
 * Build:
 *   gcc -shared -fPIC -o /tmp/libenospc_inject.so enospc_inject.c -ldl
 *
 * Usage:
 *   LD_PRELOAD=/tmp/libenospc_inject.so pg_ctl -D ... start
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#define INJECT_TRIGGER "/tmp/enospc_inject_active"
#define TARGET_PREFIX  "/work/pg-cluster-data/worker1/pg_parwal/"
#define TARGET_LEN     (sizeof(TARGET_PREFIX) - 1)

static ssize_t (*real_write)(int, const void *, size_t) = NULL;

__attribute__((constructor)) static void
init_hook(void)
{
    real_write = dlsym(RTLD_NEXT, "write");
}

static int
is_parwal_fd(int fd)
{
    char fdlink[64];
    char path[512];
    ssize_t n;

    snprintf(fdlink, sizeof(fdlink), "/proc/self/fd/%d", fd);
    n = readlink(fdlink, path, sizeof(path) - 1);
    if (n <= 0)
        return 0;
    path[n] = '\0';
    return strncmp(path, TARGET_PREFIX, TARGET_LEN) == 0;
}

ssize_t
write(int fd, const void *buf, size_t count)
{
    struct stat st;

    if (!real_write)
        init_hook();

    if (stat(INJECT_TRIGGER, &st) == 0 && is_parwal_fd(fd))
    {
        errno = ENOSPC;
        return -1;
    }

    return real_write(fd, buf, count);
}
