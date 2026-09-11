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
/*
 * ★★ T7.13（2026-09-11）：目标路径改为**编译期可传入**。
 *   原先写死 worker1 —— 与 test_enospc_recovery.sh 写死 worker1 是同一个
 *   3 节点假设。脚本改成按 pg_dist_node 动态选节点之后，若这里还钉在
 *   worker1，就会出现"脚本在 :5437 上注入、库却只拦 worker1 的写"——
 *   **注入不生效，而用例照样跑完**，是最难查的那种假绿。
 *   编译：gcc -shared -fPIC -DTARGET_PREFIX='"'"'"<dir>/pg_parwal/"'"'"' \
 *              -o /tmp/libenospc_inject.so enospc_inject.c -ldl
 */
#ifndef TARGET_PREFIX
#define TARGET_PREFIX  "/work/pg-cluster-data/worker1/pg_parwal/"
#endif
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
