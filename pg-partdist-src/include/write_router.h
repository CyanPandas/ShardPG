#ifndef WRITE_ROUTER_H
#define WRITE_ROUTER_H

#include "pg_partdist.h"
#include "executor/executor.h"

/* Hook wrappers registered in _PG_init */
extern void pg_partdist_executor_start(QueryDesc *queryDesc, int eflags);

/* Convert RouteStatus to a human-readable string (static storage) */
extern const char *RouteStatusString(RouteStatus status);

#endif /* WRITE_ROUTER_H */
