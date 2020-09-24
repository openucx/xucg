/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <string.h>
#include <ucs/arch/atomic.h>
#include <ucs/profile/profile.h>
#include <ucp/core/ucp_request.inl>
#include <ucg/api/ucg_plan_component.h>

#include "ops/builtin_ops.h"
#include "ops/builtin_comp_step.inl"
#include "plan/builtin_plan.h"

/* Backport to UCX v1.6.0 */
#ifndef UCS_MEMUNITS_INF
#define UCS_MEMUNITS_INF UCS_CONFIG_MEMUNITS_INF
#define CONDITIONAL_NULL
#else
#define CONDITIONAL_NULL ,NULL
#endif

#define UCG_BUILTIN_SUPPORT_MASK (UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_SOURCE      |\
                                  UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_DESTINATION |\
                                  UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE          |\
                                  UCG_GROUP_COLLECTIVE_MODIFIER_CONCATENATE        |\
                                  UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST          |\
                                  UCG_GROUP_COLLECTIVE_MODIFIER_IN_PLACE           |\
                                  UCG_GROUP_COLLECTIVE_MODIFIER_BARRIER            |\
                                  UCG_GROUP_COLLECTIVE_MODIFIER_MOCK_EPS)


#define UCG_BUILTIN_PARAM_MASK   (UCG_GROUP_PARAM_FIELD_MEMBER_COUNT |\
                                  UCG_GROUP_PARAM_FIELD_MEMBER_INDEX |\
                                  UCG_GROUP_PARAM_FIELD_DISTANCES)

static ucs_config_field_t ucg_builtin_config_table[] = {
    {"PLAN_", "", NULL, ucs_offsetof(ucg_builtin_config_t, super),
     UCS_CONFIG_TYPE_TABLE(ucg_plan_config_table)},

    {"TREE_", "", NULL, ucs_offsetof(ucg_builtin_config_t, tree),
     UCS_CONFIG_TYPE_TABLE(ucg_builtin_tree_config_table)},

    {"RECURSIVE_", "", NULL, ucs_offsetof(ucg_builtin_config_t, recursive),
     UCS_CONFIG_TYPE_TABLE(ucg_builtin_recursive_config_table)},

    {"NEIGHBOR_", "", NULL, ucs_offsetof(ucg_builtin_config_t, neighbor),
     UCS_CONFIG_TYPE_TABLE(ucg_builtin_neighbor_config_table)},

    {"CACHE_SIZE", "1000", "Number of cached collective operations",
     ucs_offsetof(ucg_builtin_config_t, cache_size), UCS_CONFIG_TYPE_UINT},

    {"SHORT_MAX_TX_SIZE", "256", "Largest send operation to use short messages",
     ucs_offsetof(ucg_builtin_config_t, short_max_tx), UCS_CONFIG_TYPE_MEMUNITS},

    {"BCOPY_MAX_TX_SIZE", "32768", "Largest send operation to use buffer copy",
     ucs_offsetof(ucg_builtin_config_t, bcopy_max_tx), UCS_CONFIG_TYPE_MEMUNITS},

    {"MEM_REG_OPT_CNT", "10", "Operation counter before registering the memory",
     ucs_offsetof(ucg_builtin_config_t, mem_reg_opt_cnt), UCS_CONFIG_TYPE_ULUNITS},

    {NULL}
};

struct ucg_builtin_group_ctx {
    /*
     * The following is the key structure of a group - an array of outstanding
     * collective operations, one slot per operation. Messages for future ops
     * may be stored in a slot before the operation actually starts.
     *
     * TODO: support more than this amount of concurrent operations...
     */
    ucg_builtin_comp_slot_t   slots[UCG_BUILTIN_MAX_CONCURRENT_OPS];

    /*
     * Resend slots is a bit-field indicating which slots require re-sending,
     * typically due to insufficient buffers on the receiver side (indicated by
     * UCS_ERR_NO_RESOURCES during the UCT call). On progress calls, all these
     * steps will be resumed by calling @ref ucg_builtin_step_execute on each.
     */
    uint64_t                  resend_slots;

    /* Mostly control-path, from here on */
    ucg_builtin_ctx_t        *bctx;          /**< global context */
    ucg_group_h               group;         /**< group handle */
    ucp_worker_h              worker;        /**< worker handle, for descriptors */
    const ucg_group_params_t *group_params;  /**< the original group parameters */
    ucg_group_member_index_t  host_proc_cnt; /**< Number of intra-node processes */
    ucg_group_id_t            group_id;      /**< Group identifier */
    ucg_builtin_config_t     *config;        /**< configured/default settings */
    ucs_list_link_t           plan_head;     /**< list of plans (for cleanup) */
    ucs_ptr_array_t           faults;        /**< flexible array of faulty members */
};

extern ucg_plan_component_t ucg_builtin_component;

static ucs_status_t
ucg_builtin_choose_topology(enum ucg_collective_modifiers flags,
                            ucg_group_member_index_t group_size,
                            ucg_builtin_plan_topology_t *topology)
{
    if (flags & UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_SOURCE) {
        /* MPI_Bcast / MPI_Scatter */
        topology->type = UCG_PLAN_TREE_FANOUT;
        return UCS_OK;
    }

    if (flags & UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_DESTINATION) {
        /* MPI_Reduce / MPI_Gather */
        // TODO: Alex - test operand/operator support
        topology->type = UCG_PLAN_TREE_FANIN;
        return UCS_OK;
    }

    if (flags & UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE) {
        /* MPI_Allreduce */
        if (ucs_popcount(group_size) > 1) {
            /* Not a power of two */
            topology->type = UCG_PLAN_TREE_FANIN_FANOUT;
        } else {
            topology->type = UCG_PLAN_RECURSIVE;
        }
        return UCS_OK;
    }

    /* MPI_Alltoall */
    ucs_assert(flags == 0);
    if (ucs_popcount(group_size) == 1) {
        topology->type = UCG_PLAN_ALLTOALL_BRUCK;
    } else {
        topology->type = UCG_PLAN_PAIRWISE;
    }
    return UCS_OK;
}

UCS_PROFILE_FUNC(ucs_status_t, ucg_builtin_am_handler,
                 (ctx, data, length, am_flags),
                 void *ctx, void *data, size_t length, unsigned am_flags)
{
    ucg_builtin_ctx_t *bctx      = ctx;
    ucg_builtin_header_t* header = data;
    ucs_assert(length >= sizeof(header));
    ucs_assert(header != 0); /* since group_id >= UCG_GROUP_FIRST_GROUP_ID */

    /* Find the Group context, based on the ID received in the header */
    ucg_group_id_t group_id = header->group_id;
    ucs_assert(group_id < bctx->group_by_id.size);
    ucg_builtin_group_ctx_t *gctx;
    if (ucs_unlikely(!ucs_ptr_array_lookup(&bctx->group_by_id, group_id, gctx))) {
        return UCS_ERR_INVALID_PARAM;
    }
    ucs_assert(gctx != NULL);

    /* Find the slot to be used, based on the ID received in the header */
    ucg_coll_id_t coll_id = header->msg.coll_id;
    ucg_builtin_comp_slot_t *slot = &gctx->slots[coll_id % UCG_BUILTIN_MAX_CONCURRENT_OPS];
    ucs_assert((slot->req.latest.coll_id != coll_id) ||
               (slot->req.latest.step_idx <= header->msg.step_idx));

    /* Consume the message if it fits the current collective and step index */
    if (ucs_likely(header->msg.local_id == slot->req.latest.local_id)) {
        /* Make sure the packet indeed belongs to the collective currently on */
        data    = header + 1;
        length -= sizeof(ucg_builtin_header_t);

        ucg_builtin_step_recv_cb(&slot->req, header->remote_offset, data, length);

        ucs_trace_req("ucg_builtin_am_handler CB: coll_id %u step_idx %u pending %u",
                      header->msg.coll_id, header->msg.step_idx, slot->req.pending);
        return UCS_OK;
    }

    ucs_trace_req("ucg_builtin_am_handler STORE: group_id %u "
                  "coll_id %u(%u) step_idx %u slot_step_idx %u",
                  header->group_id, header->msg.coll_id, slot->req.latest.coll_id,
                  header->msg.step_idx, slot->req.latest.step_idx);

    /* In case of a stride - the stored length is actually longer */
    if (am_flags & UCT_CB_PARAM_FLAG_STRIDE) {
        length = sizeof(ucg_builtin_header_t) +
                (length - sizeof(ucg_builtin_header_t)) *
                (gctx->group_params->member_count - 1);
    }

    /* Store the message (if the relevant step has not been reached) */
    ucp_recv_desc_t *rdesc;
    ucs_status_t ret = ucp_recv_desc_init(gctx->worker, data, length,
                                          0, am_flags, 0, 0, 0, &rdesc);
    if (ucs_likely(ret != UCS_ERR_NO_MEMORY)) {
        (void) ucs_ptr_array_insert(&slot->messages, rdesc);
    }
    return ret;
}

static void ucg_builtin_msg_dump(void *arg, uct_am_trace_type_t type,
                                 uint8_t id, const void *data, size_t length,
                                 char *buffer, size_t max)
{
    const ucg_builtin_header_t *header = (const ucg_builtin_header_t*)data;
    snprintf(buffer, max, "COLLECTIVE [coll_id %u step_idx %u offset %lu length %lu]",
             (unsigned)header->msg.coll_id, (unsigned)header->msg.step_idx,
             (uint64_t)header->remote_offset, length - sizeof(*header));
}

static ucs_status_t ucg_builtin_query(ucg_plan_desc_t **desc_p, unsigned *desc_cnt)
{
    /* Return a simple description of the "Builtin" module */
    ucs_status_t status              = ucg_plan_single(&ucg_builtin_component,
                                                       desc_p, desc_cnt);
    (*desc_p)[0].modifiers_supported = UCG_BUILTIN_SUPPORT_MASK;
    (*desc_p)[0].flags = 0;
    return status;
}

static ucg_group_member_index_t
ucg_builtin_calc_host_proc_cnt(const ucg_group_params_t *group_params)
{
    ucg_group_member_index_t index, count = 0;

    for (index = 0; index < group_params->member_count; index++) {
        if (group_params->distance[index] < UCG_GROUP_MEMBER_DISTANCE_NET) {
            count++;
        }
    }

    return count;
}

static ucs_status_t ucg_builtin_init(ucg_plan_ctx_h pctx,
                                     uint16_t am_id,
                                     uct_am_callback_t *am_cb,
                                     uct_am_tracer_t *am_tracer)
{
    ucg_builtin_ctx_t *bctx = pctx;
    bctx->am_id             = am_id;

    ucs_ptr_array_init(&bctx->group_by_id, "builtin_group_table");

    *am_cb     = ucg_builtin_am_handler;
    *am_tracer = ucg_builtin_msg_dump;

    return UCS_OK;
}

static void ucg_builtin_finalize(ucg_plan_ctx_h pctx)
{
    ucg_builtin_ctx_t *bctx = pctx;
    ucs_ptr_array_cleanup(&bctx->group_by_id);
}

static ucs_status_t ucg_builtin_create(ucg_plan_ctx_h pctx,
                                       ucg_group_ctx_h ctx,
                                       const ucg_group_params_t *params)
{
    if ((params->field_mask & UCG_BUILTIN_PARAM_MASK) !=
                                    UCG_BUILTIN_PARAM_MASK) {
        ucs_error("UCG Planner \"Builtin\" is missing some group parameters");
        return UCS_ERR_INVALID_PARAM;
    }

    /* Fill in the information in the per-group context */
    ucg_builtin_group_ctx_t *gctx = ctx;
    gctx->group_id                = params->id;
    gctx->group_params            = params;
    gctx->host_proc_cnt           = ucg_builtin_calc_host_proc_cnt(params);
    gctx->config                  = ucg_builtin_component.plan_config;
    gctx->bctx                    = pctx;
    gctx->resend_slots            = 0; /* No pending resends */
    ucs_assert(params->field_mask & UCG_GROUP_PARAM_FIELD_ID);
    ucs_list_head_init(&gctx->plan_head);

    unsigned i;
    for (i = 0; i < UCG_BUILTIN_MAX_CONCURRENT_OPS; i++) {
        ucg_builtin_comp_slot_t *slot = &gctx->slots[i];
        ucs_ptr_array_init(&slot->messages, "builtin messages");
        slot->req.latest.local_id = 0;
    }

    /* Add the group to the lookup table */
    ucs_ptr_array_set(&((ucg_builtin_ctx_t*)pctx)->group_by_id, gctx->group_id, gctx);
    return UCS_OK;
}

static void ucg_builtin_destroy(ucg_group_ctx_h ctx)
{
    /* Cleanup left-over messages and outstanding operations */
    unsigned i, j;
    ucg_builtin_group_ctx_t *gctx = ctx;
    for (i = 0; i < UCG_BUILTIN_MAX_CONCURRENT_OPS; i++) {
        ucg_builtin_comp_slot_t *slot = &gctx->slots[i];
        if (slot->req.latest.local_id != 0) {
            ucs_warn("Collective operation #%u has been left incomplete (Group #%u)",
                    gctx->slots[i].req.latest.coll_id, gctx->group_id);
        }

        ucp_recv_desc_t *rdesc;
        ucs_ptr_array_for_each(rdesc, j, &slot->messages) {
            ucs_warn("Collective operation #%u still has a pending message for"
                     "step #%u (Group #%u)",
                     ((ucg_builtin_header_t*)(rdesc + 1))->msg.coll_id,
                     ((ucg_builtin_header_t*)(rdesc + 1))->msg.step_idx,
                     ((ucg_builtin_header_t*)(rdesc + 1))->group_id);
#ifdef HAVE_UCP_EXTENSIONS
            /* No UCT interface information, we can't release if it's shared */
            if (!(rdesc->flags & UCP_RECV_DESC_FLAG_UCT_DESC_SHARED))
#endif
            ucp_recv_desc_release(rdesc CONDITIONAL_NULL);
            ucs_ptr_array_remove(&slot->messages, j);
        }
        ucs_ptr_array_cleanup(&slot->messages);
    }


    /* Cleanup plans created for this group */
    while (!ucs_list_is_empty(&gctx->plan_head)) {
        ucg_builtin_plan_t *plan = ucs_list_extract_head(&gctx->plan_head,
                ucg_builtin_plan_t, list);

        while (!ucs_list_is_empty(&plan->super.op_head)) {
            ucg_op_t *op = ucs_list_extract_head(&plan->super.op_head, ucg_op_t, list);
            ucg_builtin_op_discard(op);
        }

        ucs_mpool_cleanup(&plan->op_mp, 1);
        ucs_free(plan);
    }

    /* Remove the group from the global storage array */
    ucg_builtin_ctx_t *bctx = gctx->bctx;
    ucs_ptr_array_remove(&bctx->group_by_id, gctx->group_id);
    if (ucs_unlikely(gctx->group_id == 0)) {
        // TODO: replce with ucs_ptr_array_is_empty(&bctx->group_by_id, group_id, gctx);
        ucs_ptr_array_cleanup(&bctx->group_by_id);
        ucs_free(bctx);
    }
    /* gctx is freed as part of the group object itself */
}

static unsigned ucg_builtin_progress(ucg_group_ctx_h ctx)
{
    /* Reset the list of active slots, then re-test (some may return to it) */
    ucg_builtin_group_ctx_t *gctx = ctx;
    uint64_t resend_slots         = ucs_atomic_swap64(&gctx->resend_slots, 0);
    if (ucs_likely(resend_slots == 0)) {
        return 0;
    }

    unsigned index, ret = 0;
    ucs_for_each_bit(index, resend_slots) {
        ucg_builtin_request_t *req = &gctx->slots[index].req;
        ucs_status_t status = ucg_builtin_step_execute(req, NULL);
        if (status != UCS_INPROGRESS) {
            if (ucs_unlikely(UCS_STATUS_IS_ERR(status))) {
                ucs_error("error during UCG progress: %s", ucs_status_string(status));
            }
            ret++;
        }
    }

#if ENABLE_FAULT_TOLERANCE
        else if ((status == UCS_INPROGRESS) &&
                !(req->step->flags & UCG_BUILTIN_OP_STEP_FLAG_FT_ONGOING)) {
            /* Step in progress - start F/T for missing inbound messages */
            ucg_builtin_plan_phase_t *phase = req->step->phase;
            if (phase->ep_cnt == 1) {
                ucg_ft_start(group, phase->indexes[0], phase->single_ep, &phase->handles[0]);
            } else {
                unsigned peer_idx = 0;
                while (peer_idx < phase->ep_cnt) {
                    ucg_ft_start(group, phase->indexes[peer_idx],
                            phase->multi_eps[peer_idx], &phase->handles[peer_idx]);
                    peer_idx++;
                }
            }

            /* Mark fault-tolerance in progress */
            req->step->flags |= UCG_BUILTIN_OP_STEP_FLAG_FT_ONGOING;
        }
#endif

    return ret;






}

ucs_mpool_ops_t ucg_builtin_plan_mpool_ops = {
    .chunk_alloc   = ucs_mpool_hugetlb_malloc,
    .chunk_release = ucs_mpool_hugetlb_free,
    .obj_init      = ucs_empty_function,
    .obj_cleanup   = ucs_empty_function
};

static ucs_status_t ucg_builtin_plan(ucg_group_ctx_h ctx,
                                     const ucg_collective_type_t *coll_type,
                                     ucg_plan_t **plan_p)
{
    /* Check what kind of resources are available to the group (e.g. SM) */
    ucg_builtin_group_ctx_t *gctx        = ctx;
    ucg_builtin_plan_topology_t topology = {0};
    ucs_status_t status = ucg_plan_query_resources(gctx->group,
                                                   &topology.resources);
    if (status != UCS_OK) {
        return status;
    }

    /* Choose the best topology for this collective operation type */
    const ucg_group_params_t *params = gctx->group_params;
    status = ucg_builtin_choose_topology(coll_type->modifiers,
                                         params->member_count,
                                         &topology);
    if (status != UCS_OK) {
        return status;
    }

    /* Build the topology according to the requested */
    ucg_builtin_plan_t *plan;
    const ucg_builtin_config_t *config = ucg_builtin_component.plan_config;
    switch(topology.type) {
    case UCG_PLAN_RECURSIVE:
        status = ucg_builtin_recursive_create(gctx, &topology, config,
                                              params, coll_type, &plan);
        break;

    case UCG_PLAN_ALLTOALL_BRUCK:
        status = ucg_builtin_bruck_create(gctx, &topology, config,
                                          params, coll_type, &plan);
        break;

    case UCG_PLAN_PAIRWISE:
        status = ucg_builtin_pairwise_create(gctx, &topology, config,
                                             params, coll_type, &plan);
        break;

    case UCG_PLAN_TREE_FANIN:
    case UCG_PLAN_TREE_FANOUT:
    case UCG_PLAN_TREE_FANIN_FANOUT:
        status = ucg_builtin_tree_create(gctx, &topology, config,
                                         params, coll_type, &plan);
        break;
    }

    if (status != UCS_OK) {
        return status;
    }

    /* Create a memory-pool for operations for this plan */
    size_t op_size = sizeof(ucg_builtin_op_t) + plan->phs_cnt * sizeof(ucg_builtin_op_step_t);
    status = ucs_mpool_init(&plan->op_mp, 0, op_size, 0, UCS_SYS_CACHE_LINE_SIZE,
                            1, UINT_MAX, &ucg_builtin_plan_mpool_ops,
                            "ucg_builtin_plan_mp");
    if (status != UCS_OK) {
        return status;
    }

    ucs_list_add_head(&gctx->plan_head, &plan->list);
    plan->slots     = &gctx->slots[0];
    plan->am_id     = gctx->bctx->am_id;
    *plan_p         = (ucg_plan_t*)plan;
    return UCS_OK;
}

void ucg_builtin_print_flags(ucg_builtin_op_step_t *step)
{
    int flag;
    size_t buffer_length = step->buffer_length;

    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND) != 0);
    if (flag) {
        if (step->flags & (UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT |
                           UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY |
                           UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY)) {
            printf("\n\tReceive METHOD:\t\tafter sending");
        } else {
            printf("\n\tReceive METHOD:\t\treceive only");
        }
    }
    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_RECV_BEFORE_SEND1) != 0);
    if (flag) printf("\n\tReceive method:\t\tbefore sending once");
    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_RECV1_BEFORE_SEND) != 0);
    if (flag) printf("\n\tReceive method:\t\tonce, before sending");

    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT) != 0);
    if (flag) printf("\n\tSend method:\t\tshort");
    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY) != 0);
    if (flag) printf("\n\tSend method:\t\tbcopy");
    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY) != 0);
    if (flag) printf("\n\tSend method:\t\tzcopy");
    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED) != 0);
    printf("\n\tFRAGMENTED:\t\t%i", flag);
    if (flag) {
        printf("\n\t - Fragment Length: %lu", step->fragment_length);
        printf("\n\t - Fragment Count: %u", step->fragments);
    }

    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP) != 0);
    printf("\n\tLAST_STEP:\t\t%i", flag);
    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_SINGLE_ENDPOINT) != 0);
    printf("\n\tSINGLE_ENDPOINT:\t%i", flag);
    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_STRIDED) != 0);
    printf("\n\tSTRIDED:\t\t%i", flag);
    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_PIPELINED) != 0);
    printf("\n\tPIPELINED:\t\t%i", flag);
    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED) != 0);
    printf("\n\tTEMP_BUFFER_USED:\t%i", flag);

    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_PACKED_DTYPE_MODE) != 0);
    printf("\n\tPACKED_DTYPE_MODE:\t%i", flag);
    if (flag) {
        uct_coll_dtype_mode_t mode;
        if (step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT) {
            mode = UCT_COLL_DTYPE_MODE_UNPACK_MODE(buffer_length);
            buffer_length = UCT_COLL_DTYPE_MODE_UNPACK_VALUE(buffer_length);
        } else {
            mode = UCT_COLL_DTYPE_MODE_UNPACK_MODE(step->uct_flags);
            flag = UCT_SEND_FLAG_PACK_LOCK &
                    UCT_COLL_DTYPE_MODE_UNPACK_VALUE(step->uct_flags);
            printf("\n\tUCT packing mutex requested:\t\t%i", flag);
        }

        printf("\n\tDatatype mode:\t\t");
        switch (mode) {
        case UCT_COLL_DTYPE_MODE_PADDED:
            printf("Data (may be padded)");
            break;

        case UCT_COLL_DTYPE_MODE_PACKED:
            printf("Packed data");
            break;

        case UCT_COLL_DTYPE_MODE_VAR_COUNT:
            printf("Variable length");
            break;

        case UCT_COLL_DTYPE_MODE_VAR_DTYPE:
            printf("Variable datatypes");
            break;

        case UCT_COLL_DTYPE_MODE_LAST:
            break;
        }
    }

    printf("\n\tData aggregation:\t");
    switch (step->comp_aggregation) {
    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_NOP:
        printf("none");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_WRITE:
        printf("write");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_GATHER_TERMINAL:
        printf("gather (terminal)");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_GATHER_WAYPOINT:
        printf("gather (way-point)");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REDUCE:
        printf("reduce");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REDUCE_BATCHED:
        printf("reduce (batched)");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_RNDV_READ_FROM_REMOTE_AND_REDUCE:
        printf("read (from remote) and reduce");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_RNDV_READ_FROM_REMOTE:
        printf("read (from remote)");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_RNDV_READ_FROM_LOCAL:
        printf("read (from local)");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_RNDV_WRITE_TO_REMOTE:
        printf("write (to remote)");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_RNDV_WRITE_TO_LOCAL:
        printf("write (to local)");
        break;
    }

    printf("\n\tCompletion criteria:\t");
    switch (step->comp_criteria) {
    case UCG_BUILTIN_OP_STEP_COMP_CRITERIA_SEND:
        printf("successful send calls");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_CRITERIA_SINGLE_MESSAGE:
        printf("single message");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES:
        printf("multiple messages");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES_ZCOPY:
        printf("multiple message (zero-copy)");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_CRITERIA_BY_FRAGMENT_OFFSET:
        printf("multiple fragments");
        break;
    }

    printf("\n\tCompletion action:\t");
    switch (step->comp_action) {
    case UCG_BUILTIN_OP_STEP_COMP_OP:
        printf("finish the operation");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_STEP:
        printf("move to the next step");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_SEND:
        printf("proceed to sending");
        break;
    }

    printf("\n\tStep buffer length:\t%lu", buffer_length);

    printf("\n\n");
}

static void ucg_builtin_print(ucg_plan_t *plan, const ucg_collective_params_t *coll_params)
{
    ucs_status_t status;
    ucg_builtin_plan_t *builtin_plan = (ucg_builtin_plan_t*)plan;
    printf("Planner:       %s\n", builtin_plan->super.planner->name);
    printf("Phases:        %i\n", builtin_plan->phs_cnt);
    printf("P2P Endpoints: %i\n", builtin_plan->ep_cnt);

    printf("Object memory size:\n");
    printf("\tPlan: %lu bytes\n", sizeof(ucg_builtin_plan_t) +
            builtin_plan->phs_cnt * sizeof(ucg_builtin_plan_phase_t) +
            builtin_plan->ep_cnt * sizeof(uct_ep_h));
    printf("\tOperation: %lu bytes (%lu per step)\n", sizeof(ucg_builtin_op_t) +
            builtin_plan->phs_cnt * sizeof(ucg_builtin_op_step_t),
            sizeof(ucg_builtin_op_step_t));
    printf("\tRequest: %lu bytes\n", sizeof(ucg_builtin_request_t));
    printf("\tSlot: %lu bytes\n", sizeof(ucg_builtin_comp_slot_t));
    printf("\tPer-group context: %lu bytes (including %u slots)\n",
           sizeof(ucg_builtin_group_ctx_t), UCG_BUILTIN_MAX_CONCURRENT_OPS);

    unsigned phase_idx;
    for (phase_idx = 0; phase_idx < builtin_plan->phs_cnt; phase_idx++) {
        printf("Phase #%i: ", phase_idx);
        printf("the method is ");
        switch (builtin_plan->phss[phase_idx].method) {
        case UCG_PLAN_METHOD_SEND_TERMINAL:
            printf("Send (T), ");
            break;
        case UCG_PLAN_METHOD_SEND_TO_SM_ROOT:
            printf("Send (SM), ");
            break;
        case UCG_PLAN_METHOD_RECV_TERMINAL:
            printf("Recv (T), ");
            break;
        case UCG_PLAN_METHOD_BCAST_WAYPOINT:
            printf("Bcast (W), ");
            break;
        case UCG_PLAN_METHOD_SCATTER_TERMINAL:
            printf("Scatter (T), ");
            break;
        case UCG_PLAN_METHOD_SCATTER_WAYPOINT:
            printf("Scatter (W), ");
            break;
        case UCG_PLAN_METHOD_GATHER_TERMINAL:
            printf("Gather (T), ");
            break;
        case UCG_PLAN_METHOD_GATHER_WAYPOINT:
            printf("Gather (W), ");
            break;
        case UCG_PLAN_METHOD_REDUCE_TERMINAL:
            printf("Reduce (T), ");
            break;
        case UCG_PLAN_METHOD_REDUCE_WAYPOINT:
            printf("Reduce (W), ");
            break;
        case UCG_PLAN_METHOD_REDUCE_RECURSIVE:
            printf("Reduce (R), ");
            break;
        case UCG_PLAN_METHOD_ALLGATHER_BRUCK:
            printf("Allgather (G), ");
            break;
        case UCG_PLAN_METHOD_ALLTOALL_BRUCK:
            printf("Alltoall (B), ");
            break;
        case UCG_PLAN_METHOD_PAIRWISE:
            printf("Alltoall (P), ");
            break;
        case UCG_PLAN_METHOD_NEIGHBOR:
            printf("Neighbors, ");
            break;
        }

#if ENABLE_DEBUG_DATA || ENABLE_FAULT_TOLERANCE
        ucg_builtin_plan_phase_t *phase = &builtin_plan->phss[phase_idx];
#ifdef HAVE_UCT_COLLECTIVES
        if ((phase->ep_cnt == 1) &&
            (phase->indexes[0] == UCG_GROUP_MEMBER_INDEX_UNSPECIFIED)) {
            printf("with all same-level peers (collective-aware transport)\n");
        } else
#endif
        {
            uct_ep_h *ep = (phase->ep_cnt == 1) ? &phase->single_ep :
                                                   phase->multi_eps;
            printf("with the following peers: ");

            unsigned peer_idx;
            for (peer_idx = 0;
                 peer_idx < phase->ep_cnt;
                 peer_idx++, ep++) {
                printf("%lu,", phase->indexes[peer_idx]);
            }
            printf("\n");
        }
#else
        printf("no peer info (configured without \"--enable-debug-data\")");
#endif

        if (coll_params) {
            enum ucg_builtin_op_step_flags flags = 0;
            if (phase_idx == (builtin_plan->phs_cnt - 1)) {
                flags |= UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP;
            }

            int8_t *temp_buffer = NULL;
            ucg_builtin_op_step_t step;
            ucg_builtin_op_init_cb_t init_cb = NULL;
            ucg_builtin_op_fini_cb_t fini_cb = NULL;
            printf("Step #%i (actual index used: %u):", phase_idx,
                    builtin_plan->phss[phase_idx].step_index);
            status = ucg_builtin_step_create(builtin_plan,
                    &builtin_plan->phss[phase_idx], &flags, coll_params,
                    &temp_buffer, &init_cb, &fini_cb, &step);
            if (status != UCS_OK) {
                printf("failed to create, %s", ucs_status_string(status));
            }

            if (phase_idx == 0) {
                printf("\n\tOP initialization:\t");
                ucg_builtin_print_init_cb_name(init_cb);

                printf("\n\tOP finalization:\t");
                ucg_builtin_print_fini_cb_name(fini_cb);
            }

            ucg_builtin_step_select_packers(coll_params, &step);
            printf("\n\tPacker (if used):\t");
            ucg_builtin_print_pack_cb_name(step.bcopy.pack_single_cb);

            ucg_builtin_print_flags(&step);
        }
    }

    if (coll_params->send.count != 1) {
        ucs_warn("currently, short active messages are assumed for any buffer size");
    }
}

#define UCG_BUILTIN_CONNECT_SINGLE_EP ((unsigned)-1)
static uct_iface_attr_t mock_ep_attr;

ucs_status_t ucg_builtin_connect(ucg_builtin_group_ctx_t *ctx,
        ucg_group_member_index_t idx, ucg_builtin_plan_phase_t *phase,
        unsigned phase_ep_index, enum ucg_plan_connect_flags flags, int is_mock)
{
#if ENABLE_FAULT_TOLERANCE || ENABLE_DEBUG_DATA
    if (phase->indexes == NULL) {
        phase->indexes = UCS_ALLOC_CHECK(sizeof(ucg_group_member_index_t) *
                                         phase->ep_cnt, "ucg_phase_indexes");
#if ENABLE_FAULT_TOLERANCE
        phase->handles = UCS_ALLOC_CHECK(sizeof(ucg_ft_h) * phase->ep_cnt,
                                         "ucg_phase_handles");
#endif
    }
    phase->indexes[(phase_ep_index != UCG_BUILTIN_CONNECT_SINGLE_EP) ?
            phase_ep_index : 0] = flags ? UCG_GROUP_MEMBER_INDEX_UNSPECIFIED : idx;
#endif
    if (is_mock) {
        // TODO: allocate mock attributes according to flags (and later free it)
        unsigned dtype_support = UCS_MASK(UCT_COLL_DTYPE_MODE_LAST);
        memset(&mock_ep_attr, 0, sizeof(mock_ep_attr));

        mock_ep_attr.cap.am.max_short          = SIZE_MAX;
        mock_ep_attr.cap.flags                 = UCT_IFACE_FLAG_AM_SHORT |
                                                 UCT_IFACE_FLAG_INCAST   |
                                                 UCT_IFACE_FLAG_BCAST;
        mock_ep_attr.cap.am.coll_mode_flags    = dtype_support;
        mock_ep_attr.cap.coll_mode.short_flags = dtype_support;
        phase->iface_attr                      = &mock_ep_attr;
        phase->md                              = NULL;

        return UCS_OK;
    }

    uct_ep_h ep;
    ucs_status_t status = ucg_plan_connect(ctx->group, idx, flags,
            &ep, &phase->iface_attr, &phase->md, &phase->md_attr);
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }

#if ENABLE_FAULT_TOLERANCE
    /* Send information about any faults that may have happened */
    status = ucg_ft_propagate(ctx->group, ctx->group_params, ep);
    if (status != UCS_OK) {
        return status;
    }
#endif

    /* Store the endpoint as part of the phase */
    phase->resends = &ctx->resend_slots;
    phase->host_proc_cnt = ctx->host_proc_cnt;
    if (phase_ep_index == UCG_BUILTIN_CONNECT_SINGLE_EP) {
        phase->single_ep = ep;
    } else {
        ucs_assert(phase_ep_index < phase->ep_cnt);
        phase->multi_eps[phase_ep_index] = ep;
    }

    return status;
}

ucs_status_t ucg_builtin_single_connection_phase(ucg_builtin_group_ctx_t *ctx,
        ucg_group_member_index_t idx, ucg_step_idx_t step_index,
        enum ucg_builtin_plan_method_type method,
        enum ucg_plan_connect_flags flags,
        ucg_builtin_plan_phase_t *phase,
        int is_mock)
{
    phase->ep_cnt     = 1;
    phase->step_index = step_index;
    phase->method     = method;

#if ENABLE_DEBUG_DATA || ENABLE_FAULT_TOLERANCE
    phase->indexes = UCS_ALLOC_CHECK(sizeof(idx), "phase indexes");
#endif

    return ucg_builtin_connect(ctx, idx, phase, UCG_BUILTIN_CONNECT_SINGLE_EP,
                               flags, is_mock);
}

static ucs_status_t ucg_builtin_handle_fault(ucg_group_ctx_h gctx,
                                             ucg_group_member_index_t index)
{
    return UCS_ERR_NOT_IMPLEMENTED;
}


UCG_PLAN_COMPONENT_DEFINE(ucg_builtin_component, "builtin",
                          sizeof(ucg_builtin_ctx_t),
                          sizeof(ucg_builtin_group_ctx_t),
                          ucg_builtin_query, ucg_builtin_init,
                          ucg_builtin_finalize, ucg_builtin_create,
                          ucg_builtin_destroy, ucg_builtin_progress,
                          ucg_builtin_plan, ucg_builtin_op_create,
                          ucg_builtin_op_trigger, ucg_builtin_op_discard,
                          ucg_builtin_print, ucg_builtin_handle_fault, "BUILTIN_",
                          ucg_builtin_config_table, ucg_builtin_config_t);

