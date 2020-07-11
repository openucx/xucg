/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "ucg_plan.h"
#include "ucg_group.h"
#include "ucg_context.h"

#include <ucs/datastruct/queue.h>
#include <ucs/datastruct/list.h>
#include <ucs/profile/profile.h>
#include <ucs/debug/memtrack.h>
#include <ucp/core/ucp_ep.inl>
#include <ucp/core/ucp_worker.h>
#include <ucp/core/ucp_ep.inl>
#include <ucp/core/ucp_proxy_ep.h> /* for @ref ucp_proxy_ep_test */


#define UCG_FLAG_MASK(params) ((params)->type.modifiers & UCG_GROUP_CACHE_MODIFIER_MASK)

#define UCG_GROUP_PARAM_REQUIRED_MASK (UCG_GROUP_PARAM_FIELD_MEMBER_COUNT |\
                                       UCG_GROUP_PARAM_FIELD_MEMBER_INDEX |\
                                       UCG_GROUP_PARAM_FIELD_CB_CONTEXT)

#if ENABLE_STATS
/**
 * UCG group statistics counters
 */
enum {
    UCG_GROUP_STAT_PLANS_CREATED,
    UCG_GROUP_STAT_PLANS_USED,

    UCG_GROUP_STAT_OPS_CREATED,
    UCG_GROUP_STAT_OPS_USED,
    UCG_GROUP_STAT_OPS_IMMEDIATE,

    UCG_GROUP_STAT_LAST
};

static ucs_stats_class_t ucg_group_stats_class = {
    .name           = "ucg_group",
    .num_counters   = UCG_GROUP_STAT_LAST,
    .counter_names  = {
        [UCG_GROUP_STAT_PLANS_CREATED] = "plans_created",
        [UCG_GROUP_STAT_PLANS_USED]    = "plans_reused",
        [UCG_GROUP_STAT_OPS_CREATED]   = "ops_created",
        [UCG_GROUP_STAT_OPS_USED]      = "ops_started",
        [UCG_GROUP_STAT_OPS_IMMEDIATE] = "ops_immediate"
    }
};
#endif /* ENABLE_STATS */

#define UCG_GROUP_PROGRESS_ADD(iface, ctx) {          \
    unsigned _idx = 0;                                \
    if (ucs_unlikely(_idx == UCG_MAX_IFACES)) {       \
        return UCS_ERR_EXCEEDS_LIMIT;                 \
    }                                                 \
                                                      \
    while (_idx < (ctx)->iface_cnt) {                 \
        if ((ctx)->ifaces[_idx] == (iface)) {         \
            break;                                    \
        }                                             \
        _idx++;                                       \
    }                                                 \
                                                      \
    if (_idx == (ctx)->iface_cnt) {                   \
        (ctx)->ifaces[(ctx)->iface_cnt++] = (iface);  \
    }                                                 \
}

__KHASH_IMPL(ucg_group_ep, static UCS_F_MAYBE_UNUSED inline,
             ucg_group_member_index_t, ucp_ep_h, 1, kh_int64_hash_func,
             kh_int64_hash_equal);

static UCS_F_ALWAYS_INLINE
ucg_context_h ucg_worker_get_context(ucp_worker_h worker)
{
    return ucs_container_of(worker->context, ucg_context_t, ucp_ctx);
}

unsigned ucg_worker_progress(ucp_worker_h worker)
{
    unsigned idx, ret = 0;
    ucg_context_t *ctx = ucg_worker_get_context(worker);

    /* First, try the interfaces used for collectives */
    for (idx = 0; idx < ctx->iface_cnt; idx++) {
        ret += uct_iface_progress(ctx->ifaces[idx]);
    }

    /* As a fallback (and for correctness) - try all other transports */
    return ret + ucp_worker_progress(worker);
}

unsigned ucg_group_progress(ucg_group_h group)
{
    unsigned idx, ret = 0;
    ucg_context_t *ctx = group->context;

    /* First, try the per-planner progress functions */
    ucg_plan_component_t *planc;
    ucg_group_ctx_h gctx = group + 1;
    for (idx = 0; idx < ctx->num_planners; idx++) {
        planc = ctx->planners[idx].plan_component;
        ret += planc->progress(gctx);
        gctx = UCS_PTR_BYTE_OFFSET(gctx, planc->per_group_ctx_size);
    }
    if (ret) {
        return ret;
    }

    return ucg_worker_progress(group->worker);
}

ucs_status_t ucg_group_create(ucp_worker_h worker,
                              const ucg_group_params_t *params,
                              ucg_group_h *group_p)
{
    ucs_status_t status;
    ucg_context_t *ctx = ucg_worker_get_context(worker);

    if ((params->field_mask & UCG_GROUP_PARAM_REQUIRED_MASK) !=
                              UCG_GROUP_PARAM_REQUIRED_MASK) {
        ucs_error("UCG is missing some critical group parameters");
        return UCS_ERR_INVALID_PARAM;
    }

    UCP_WORKER_THREAD_CS_ENTER_CONDITIONAL(worker); // TODO: check where needed

    /* allocate a new group */
    size_t distance_size              = sizeof(*params->distance) * params->member_count;
    struct ucg_group *new_group       = ucs_malloc(sizeof(struct ucg_group) +
            ctx->total_planner_sizes + distance_size, "communicator group");
    if (new_group == NULL) {
        status = UCS_ERR_NO_MEMORY;
        goto cleanup_none;
    }

    /* fill in the group fields */
    new_group->is_barrier_outstanding = 0;
    new_group->worker                 = worker;
    new_group->next_coll_id           = 0;
    new_group->iface_cnt              = 0;

    ucs_queue_head_init(&new_group->pending);
    kh_init_inplace(ucg_group_ep, &new_group->eps);
    memcpy((ucg_group_params_t*)&new_group->params, params, sizeof(*params));
    new_group->params.distance = (typeof(params->distance))((char*)(new_group
            + 1) + ctx->total_planner_sizes);

    if (params->field_mask & UCG_GROUP_PARAM_FIELD_DISTANCES) {
        memcpy(new_group->params.distance, params->distance, distance_size);
    } else {
        /* If the user didn't specify the distances - treat as uniform */
        memset(new_group->params.distance, UCG_GROUP_MEMBER_DISTANCE_LAST,
               distance_size);
    }

    if (params->field_mask & UCG_GROUP_PARAM_FIELD_ID) {
        ctx->next_group_id = ucs_max(ctx->next_group_id, new_group->params.id);
    } else {
        new_group->params.id = ++ctx->next_group_id;
    }

    /*
     * Note: the contexts of each planner are initialized to zero, otherwise
     *       there's no indication of the "first" usage of a group (to suggest
     *       initialization, and prevent it from repeating).
     */
    memset(new_group + 1, 0, ctx->total_planner_sizes);

    unsigned idx;
    for (idx = 0; idx < UCG_GROUP_CACHE_MODIFIER_MASK; idx++) {
        new_group->cache[idx] = NULL;
    }

    /* Create a loopback connection, since resolve_cb may fail loopback */
    ucp_ep_params_t ep_params = { .field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS };
    status = ucp_worker_get_address(worker, (ucp_address_t**)&ep_params.address,
            &distance_size);
    if (status != UCS_OK) {
        return status;
    }
    ucp_ep_h loopback_ep;
    status = ucp_ep_create(worker, &ep_params, &loopback_ep);
    ucp_worker_release_address(worker, (ucp_address_t*)ep_params.address);
    if (status != UCS_OK) {
        return status;
    }

    /* Store this loopback endpoint, for future reference */
    ucg_group_member_index_t my_index = new_group->params.member_index;
    ucs_assert(kh_get(ucg_group_ep, &new_group->eps, my_index) == kh_end(&new_group->eps));
    khiter_t iter = kh_put(ucg_group_ep, &new_group->eps, my_index, (int*)&idx);
    kh_value(&new_group->eps, iter) = loopback_ep;

    /* Initialize the planners (modules) */
    ucg_plan_ctx_h pctx       = ctx + 1;
    ucg_group_ctx_h gctx = new_group + 1;
    for (idx = 0; idx < ctx->num_planners; idx++) {
        /* Create the per-planner per-group context */
        ucg_plan_component_t *planner = ctx->planners[idx].plan_component;

        status = planner->create(pctx, gctx, &new_group->params);
        if (status != UCS_OK) {
            goto cleanup_planners;
        }

        pctx = UCS_PTR_BYTE_OFFSET(pctx, planner->global_ctx_size);
        gctx = UCS_PTR_BYTE_OFFSET(gctx, planner->per_group_ctx_size);
    }

    status = UCS_STATS_NODE_ALLOC(&new_group->stats,
                                  &ucg_group_stats_class,
                                  worker->stats, "-%p", new_group);
    if (status != UCS_OK) {
        goto cleanup_planners;
    }

#if ENABLE_FAULT_TOLERANCE
    if (ucs_list_is_empty(&ctx->groups_head)) {
        /* Initialize the fault-tolerance context for the entire UCG layer */
        status = ucg_ft_init(&worker->async, new_group, ucg_base_am_id + idx,
                ucg_group_fault_cb, ctx, &ctx->ft_ctx);
        if (status != UCS_OK) {
            UCS_STATS_NODE_FREE(&new_group->stats);
            goto cleanup_planners;
        }
    }
#endif

    ucs_list_add_head(&ctx->groups_head, &new_group->list);
    UCP_WORKER_THREAD_CS_EXIT_CONDITIONAL(worker);
    *group_p = new_group;
    return UCS_OK;

cleanup_planners:
    while (idx) {
        ucg_plan_component_t *planner = ctx->planners[idx--].plan_component;
        planner->destroy((void*)gctx);
        gctx = UCS_PTR_BYTE_OFFSET(gctx, -planner->per_group_ctx_size);
    }
    ucs_assert(gctx == (new_group + 1));
    ucs_free(new_group);

cleanup_none:
    UCP_WORKER_THREAD_CS_EXIT_CONDITIONAL(worker);
    return status;
}

const ucg_group_params_t* ucg_group_get_params(ucg_group_h group)
{
    return &group->params;
}

void ucg_group_destroy(ucg_group_h group)
{
    /* First - make sure all the collectives are completed */
    while (!ucs_queue_is_empty(&group->pending)) {
        ucg_group_progress(group);
    }

#if ENABLE_MT
    ucp_worker_h worker = group->worker;
#endif
    UCP_WORKER_THREAD_CS_ENTER_CONDITIONAL(worker);

    /* TODO: fix:
    unsigned idx;
    ucg_context_t *gctx = UCG_WORKER_TO_GROUPS_CTX(group->worker);
    for (idx = 0; idx < gctx->num_planners; idx++) {
        ucg_plan_component_t *planc = gctx->planners[idx].plan_component;
         fix planc->destroy(group);
    }*/

    kh_destroy_inplace(ucg_group_ep, &group->eps);
    UCS_STATS_NODE_FREE(group->stats);
    ucs_list_del(&group->list);
    ucs_free(group);

#if ENABLE_FAULT_TOLERANCE
    if (ucs_list_is_empty(&group->list)) {
        ucg_ft_cleanup(&gctx->ft_ctx);
    }
#endif

    UCP_WORKER_THREAD_CS_EXIT_CONDITIONAL(worker);
}

void ucg_request_cancel(ucg_group_h group, ucg_request_t *req)
{
    // TODO: implement
}

UCS_PROFILE_FUNC(ucs_status_t, ucg_collective_create,
        (group, params, coll), ucg_group_h group,
        const ucg_collective_params_t *params, ucg_coll_h *coll)
{
    ucg_op_t *op;
    ucs_status_t status;
    ucg_plan_component_t *planc;

    UCP_WORKER_THREAD_CS_ENTER_CONDITIONAL(group->worker);

    /* check the recycling/cache for this collective */
    unsigned coll_mask = UCG_FLAG_MASK(params);
    ucg_plan_t *plan   = group->cache[coll_mask];
    if (ucs_likely(plan != NULL)) {

        ucs_list_for_each(op, &plan->op_head, list) {
            if (!memcmp(&op->params, params, sizeof(*params))) {
                status = UCS_OK;
                goto op_found;
            }
        }

        UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_PLANS_USED, 1);
        planc = plan->planner;
        goto plan_found;
    }

    /* select which plan to use for this collective operation */
    size_t gctx_offset;
    status = ucg_plan_select(group, params, &planc, &gctx_offset);
    if (status != UCS_OK) {
        goto out;
    }

    /* create the actual plan for the collective operation */
    UCS_PROFILE_CODE("ucg_plan") {
        ucg_group_ctx_h gctx = UCS_PTR_BYTE_OFFSET(group, gctx_offset);
        ucs_trace_req("ucg_collective_create PLAN: planc=%s type=%x root=%"PRIx64,
                      &planc->name[0], params->type.modifiers,
                      (uint64_t)params->type.root);

        status = planc->plan(gctx, &params->type, &plan);
    }
    if (status != UCS_OK) {
        goto out;
    }

    plan->planner = planc;
    plan->group   = group;
    plan->type    = params->type;

    if (group->params.field_mask & UCG_GROUP_PARAM_FIELD_ID) {
        plan->group_id = group->group_id;
    }
    if (group->params.field_mask & UCG_GROUP_PARAM_FIELD_MEMBER_INDEX) {
        plan->my_index = group->params.member_index;
    }
    if (group->params.field_mask & UCG_GROUP_PARAM_FIELD_MEMBER_COUNT) {
        plan->group_size = group->params.member_count;
    }

#ifdef HAVE_UCT_COLLECTIVES
    plan->group_host_size = group->worker->context->config.num_local_peers;
#endif

    group->cache[coll_mask] = plan;
    ucs_list_head_init(&plan->op_head);
    UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_PLANS_CREATED, 1);

plan_found:
    UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_OPS_CREATED, 1);
    UCS_PROFILE_CODE("ucg_prepare") {
        status = planc->prepare(plan, params, &op);
    }
    if (status != UCS_OK) {
        goto out;
    }

    ucs_trace_req("ucg_collective_create OP: planc=%s "
                  "params={type=%u, root=%lu, send=[%p,%i,%lu,%p,%p], "
                  "recv=[%p,%i,%lu,%p,%p], cb=%p, op=%p}", &planc->name[0],
                  (unsigned)params->type.modifiers, (uint64_t)params->type.root,
                  params->send.buf, params->send.count, params->send.dt_len,
                  params->send.dt_ext, params->send.displs,
                  params->recv.buf, params->recv.count, params->recv.dt_len,
                  params->recv.dt_ext, params->recv.displs,
                  params->comp_cb, params->recv.op_ext);

    ucs_list_add_head(&plan->op_head, &op->list);
    memcpy(&op->params, params, sizeof(*params));

    op->trigger_f = planc->trigger;
    op->plan      = plan;

op_found:
    *coll = op;

out:
    UCP_WORKER_THREAD_CS_EXIT_CONDITIONAL(group->worker);

    return status;
}

ucs_status_t static UCS_F_ALWAYS_INLINE
ucg_collective_trigger(ucg_group_h group, ucg_op_t *op, ucg_request_t *req)
{
    /* Start the first step of the collective operation */
    ucs_status_t ret;
    UCS_PROFILE_CODE("ucg_trigger") {
        ret = op->trigger_f(op, ++group->next_coll_id, req);
    }

    if (ret != UCS_INPROGRESS) {
        UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_OPS_IMMEDIATE, 1);
    }

    return ret;
}

ucs_status_t ucg_collective_acquire_barrier(ucg_group_h group)
{
    ucs_assert(group->is_barrier_outstanding == 0);
    group->is_barrier_outstanding = 1;
    return UCS_OK;
}

ucs_status_t ucg_collective_release_barrier(ucg_group_h group)
{
    ucs_assert(group->is_barrier_outstanding);
    group->is_barrier_outstanding = 0;
    if (ucs_queue_is_empty(&group->pending)) {
        return UCS_OK;
    }

    ucs_status_t ret;
    do {
        /* Move the operation from the pending queue back to the original one */
        ucg_op_t *op = (ucg_op_t*)ucs_queue_pull_non_empty(&group->pending);
        ucg_request_t *req = op->pending_req;
        ucs_list_add_head(&op->plan->op_head, &op->list);

        /* Start this next pending operation */
        ret = ucg_collective_trigger(group, op, req);
    } while ((!ucs_queue_is_empty(&group->pending)) &&
             (!group->is_barrier_outstanding) &&
             (ret == UCS_OK));

    return ret;
}

UCS_PROFILE_FUNC(ucs_status_t, ucg_collective_start, (coll, req),
                 ucg_coll_h coll, ucg_request_t *req)
{
    ucs_status_t ret;
    ucg_op_t *op = (ucg_op_t*)coll;
    ucg_group_h group = op->plan->group;

    /* Since group was created - don't need UCP_CONTEXT_CHECK_FEATURE_FLAGS */
    UCP_WORKER_THREAD_CS_ENTER_CONDITIONAL(group->worker);

    ucs_trace_req("ucg_collective_start: op=%p req=%p", coll, req);

    if (ucs_unlikely(group->is_barrier_outstanding)) {
        ucs_list_del(&op->list);
        ucs_queue_push(&group->pending, &op->queue);
        op->pending_req = req;
        ret = UCS_INPROGRESS;
    } else {
        ret = ucg_collective_trigger(group, op, req);
    }

    UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_OPS_USED, 1);
    UCP_WORKER_THREAD_CS_EXIT_CONDITIONAL(group->worker);
    return ret;
}

void ucg_collective_destroy(ucg_coll_h coll)
{
    ucg_op_t *op = (ucg_op_t*)coll;
    op->plan->planner->discard(op);
}

/*
 * Below are some seemingly-unrelated functions - it's here because of the
 * group pointer dereference (requires access to the internal structure).
 */

ucs_status_t ucg_plan_select(ucg_group_h group,
                             const ucg_collective_params_t *params,
                             ucg_plan_component_t **planc_p,
                             size_t *planc_offset_p)
{
    ucg_context_t *ctx = group->context;
    return ucg_plan_select_component(ctx->planners, ctx->num_planners,
                                     &ctx->config.planners, &group->params,
                                     params, planc_p, planc_offset_p);
}

ucs_status_t ucg_plan_connect(ucg_group_h group,
                              ucg_group_member_index_t idx,
                              enum ucg_plan_connect_flags flags,
                              uct_ep_h *ep_p, const uct_iface_attr_t **ep_attr_p,
                              uct_md_h *md_p, const uct_md_attr_t    **md_attr_p)
{
    int ret;
    ucs_status_t status;
    size_t remote_addr_len;
    ucp_address_t *remote_addr = NULL;

    /* Look-up the UCP endpoint based on the index */
    ucp_ep_h ucp_ep;
    khiter_t iter = kh_get(ucg_group_ep, &group->eps, idx);
    if (iter != kh_end(&group->eps)) {
        /* Use the cached connection */
        ucp_ep = kh_value(&group->eps, iter);
    } else {
        /* fill-in UCP connection parameters */
        status = ucg_params.address.lookup_f(group->params.cb_context,
                                             idx, &remote_addr, &remote_addr_len);
        if (status != UCS_OK) {
            ucs_error("failed to obtain a UCP endpoint from the external callback");
            return status;
        }

        /* special case: connecting to a zero-length address means it's "debugging" */
        if (ucs_unlikely(remote_addr_len == 0)) {
            *ep_p = NULL;
            return UCS_OK;
        }

        /* create an endpoint for communication with the remote member */
        ucp_ep_params_t ep_params = {
                .field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS,
                .address = remote_addr
        };
        status = ucp_ep_create(group->worker, &ep_params, &ucp_ep);
        ucg_params.address.release_f(remote_addr);
        if (status != UCS_OK) {
            return status;
        }

        /* Store this endpoint, for future reference */
        iter = kh_put(ucg_group_ep, &group->eps, idx, &ret);
        kh_value(&group->eps, iter) = ucp_ep;
    }

    /* Connect for point-to-point communication */
    ucp_lane_index_t lane;
am_retry:
#ifdef HAVE_UCT_COLLECTIVES
    if (flags & UCG_PLAN_CONNECT_FLAG_WANT_INCAST) {
        lane = ucp_ep_get_incast_lane(ucp_ep);
        if (ucs_unlikely(lane == UCP_NULL_LANE)) {
            ucs_warn("No transports with native incast support were found,"
                     " falling back to P2P transports (slower)");
            return UCS_ERR_UNREACHABLE;
        }
        *ep_p = ucp_ep_get_incast_uct_ep(ucp_ep);
    } else if (flags & UCG_PLAN_CONNECT_FLAG_WANT_BCAST) {
        lane = ucp_ep_get_bcast_lane(ucp_ep);
        if (ucs_unlikely(lane == UCP_NULL_LANE)) {
            ucs_warn("No transports with native broadcast support were found,"
                     " falling back to P2P transports (slower)");
            return UCS_ERR_UNREACHABLE;
        }
        *ep_p = ucp_ep_get_bcast_uct_ep(ucp_ep);
    } else
#endif /* HAVE_UCT_COLLECTIVES */
    {
        lane  = ucp_ep_get_am_lane(ucp_ep);
        *ep_p = ucp_ep_get_am_uct_ep(ucp_ep);
    }

    if (*ep_p == NULL) {
        status = ucp_wireup_connect_remote(ucp_ep, lane);
        if (status != UCS_OK) {
            return status;
        }
        goto am_retry; /* Just to obtain the right lane */
    }

    if (ucp_proxy_ep_test(*ep_p)) {
        ucp_proxy_ep_t *proxy_ep = ucs_derived_of(*ep_p, ucp_proxy_ep_t);
        *ep_p = proxy_ep->uct_ep;
        ucs_assert(*ep_p != NULL);
    }

    ucs_assert((*ep_p)->iface != NULL);
    if ((*ep_p)->iface->ops.ep_am_short ==
            (typeof((*ep_p)->iface->ops.ep_am_short))
            ucs_empty_function_return_no_resource) {
        ucp_worker_progress(group->worker);
        goto am_retry;
    }

    /* Register interfaces to be progressed in future calls */
    ucg_context_t *ctx = group->context;
    UCG_GROUP_PROGRESS_ADD((*ep_p)->iface, ctx);
    UCG_GROUP_PROGRESS_ADD((*ep_p)->iface, group);

    *md_p      = ucp_ep_md(ucp_ep, lane);
    *md_attr_p = ucp_ep_md_attr(ucp_ep, lane);
    *ep_attr_p = ucp_ep_get_iface_attr(ucp_ep, lane);

    return UCS_OK;
}

ucs_status_t ucg_plan_query_resources(ucg_group_h group,
                                      ucg_plan_resources_t **resources)
{
    return UCS_OK;
}
