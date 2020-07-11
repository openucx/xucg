/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_GROUP_H_
#define UCG_GROUP_H_

#include <ucs/stats/stats.h>
#include <ucs/type/spinlock.h>
#include <ucs/datastruct/khash.h>

/* Note: <ucs/api/...> not used because this header is not installed */
#include "../api/ucg_plan_component.h"

#define UCG_GROUP_MAX_IFACES (8)

#define UCG_GROUP_CACHE_MODIFIER_MASK UCS_MASK(7)

__KHASH_TYPE(ucg_group_ep, ucg_group_member_index_t, ucp_ep_h)

typedef struct ucg_group {
    /*
     * Whether a current barrier is waited upon. If so, new collectives cannot
     * start until this barrier is cleared, so it is put in the pending queue.
     */
    volatile uint32_t          is_barrier_outstanding;
    uint32_t                   is_cache_cleanup_due;

#if ENABLE_MT
    ucs_recursive_spinlock_t lock;
#endif

    ucg_context_h         context;      /**< Back-reference to UCP context */
    ucp_worker_h          worker;       /**< for conn. est. and progress calls */
    ucg_coll_id_t         next_coll_id; /**< for the next collective operation */
    ucs_queue_head_t      pending;      /**< requests currently pending execution */
    ucg_group_params_t    params;       /**< parameters, for future connections */
    ucs_list_link_t       list;         /**< worker's group list */
    ucg_plan_resources_t *resources;    /**< resources available to this group */
    khash_t(ucg_group_ep) eps;          /**< endpoints created for this group */

    UCS_STATS_NODE_DECLARE(stats);

    unsigned              iface_cnt;    /**< number of interfaces (for progress) */
    uct_iface_h           ifaces[UCG_MAX_IFACES];

    /*
     * per-group cache of previous plans/operations, arranged as follows:
     * for each collective type (e.g. Allreduce) there is a plan with a list of
     * operations. To re-use a past operation it must be available and match the
     * requested collective parameters. The cache size is a total across all
     * collective types.
     */
    unsigned              cache_size;
    ucg_plan_t           *barrier_cache;
    ucg_plan_t           *cache[UCG_GROUP_CACHE_MODIFIER_MASK];

    /* Below this point - the private per-planner data is allocated/stored */
} ucg_group_t;

const ucg_group_params_t* ucg_group_get_params(ucg_group_h group); /* for tests */

#endif /* UCG_GROUP_H_ */
