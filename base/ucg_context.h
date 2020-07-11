/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_CONTEXT_H_
#define UCG_CONTEXT_H_

#include <ucs/config/types.h>
#include <ucp/core/ucp_context.h>

/* Note: <ucs/api/...> not used because this header is not installed */
#include "../api/ucg_plan_component.h"

typedef struct ucg_config {
    ucp_config_t super;

    /** Array of planner names to use */
    ucs_config_names_array_t planners;
} ucg_context_config_t;

/*
 * To enable the "Groups" feature in UCX - it's registered as part of the UCX
 * context - and allocated a context slot in each UCP Worker at a certain offset.
 */
typedef struct ucg_context {
    ucs_list_link_t       groups_head;
    ucg_group_id_t        next_group_id;
    ucg_context_config_t  config;
    unsigned              iface_cnt;
    uct_iface_h           ifaces[UCG_MAX_IFACES];

    size_t                total_planner_sizes;
    unsigned              num_planners;
    ucg_plan_desc_t      *planners;

    struct {
        int (*lookup_f)(void *cb_group_context,
                        ucg_group_member_index_t index,
                        ucp_address_t **addr,
                        size_t *addr_len);
        void (*release_f)(ucp_address_t *addr);
    } address;

#if ENABLE_FAULT_TOLERANCE
    ucg_ft_ctx_t          ft_ctx;
#endif

    ucp_context_t         ucp_ctx; /* must be last, for ABI compatibility */
} ucg_context_t;

#endif /* UCG_CONTEXT_H_ */
