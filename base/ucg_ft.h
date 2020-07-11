/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_FT_H_
#define UCG_FT_H_

#include "../api/ucg_plan_component.h"

#include <ucs/time/timerq.h>
#include <ucs/stats/stats.h>
#include <ucs/datastruct/khash.h>
#include <ucs/datastruct/mpool.h>

enum ucg_ft_mode {
    UCG_FT_MODE_NONE = 0,
    UCG_FT_MODE_ON_DEMAND, /* Only active connections are monitored. */
    UCG_FT_MODE_NEIGHBORS, /* Each process monitors two neighbors and broadcasts faults. */
};

typedef struct ucg_ft_config {
    enum ucg_ft_mode         ft_mode;
    double                   timeout_after_keepalive;
    double                   timeout_between_keepalives;
} ucg_ft_config_t;

__KHASH_TYPE(ucg_ft_ep, ucg_group_member_index_t, uct_ep_h)

typedef ucs_status_t (*ucg_ft_fault_cb_t)(void *arg, ucg_group_member_index_t index);

typedef struct ucg_ft_ctx {
    khash_t(ucg_ft_ep)       ep_ht;     /* group-index->endpoint hash-table */
    ucs_mpool_t              handle_mp; /* pool of handles for ucg_ft_start/end */
    ucg_group_h              main_grp;  /* To connect, typically MPI_COMM_WORLD */
    ucs_timer_queue_t        reminder;  /* fault-tolerance interaction timers */
    ucg_group_member_index_t my_index;  /* my index, for a "return address" */
    uint8_t                  ft_am_id;  /* Active-Message ID for FT messages */
    int                      timer_id;  /* ID for async timer cleanup */

    ucg_ft_fault_cb_t        fault_cb;  /* callaback for notification of faults */
    void                    *fault_arg; /* callback argument for fault_cb */

    ucg_ft_config_t          config;    /* configured settings */

    UCS_STATS_NODE_DECLARE(stats);
} ucg_ft_ctx_t;

ucs_status_t ucg_ft_init(ucs_async_context_t *async, ucg_group_h main_group,
                         uint8_t ft_am_id, ucg_ft_fault_cb_t notify_cb,
                         void *notify_arg, ucg_ft_ctx_t *ctx);

void ucg_ft_cleanup(ucg_ft_ctx_t *ctx);

#endif /* UCG_FT_H_ */
