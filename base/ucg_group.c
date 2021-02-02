/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "ucg_plan.h"
#include "ucg_group.h"
#include "ucg_context.h"

#include <ucp/core/ucp_worker.h>
#include <ucs/datastruct/queue.h>
#include <ucs/datastruct/list.h>
#include <ucs/profile/profile.h>
#include <ucs/debug/memtrack.h>


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

#if ENABLE_MT
#define UCG_GROUP_THREAD_CS_ENTER(_obj) ucs_recursive_spin_lock(&(_obj)->lock);
#define UCG_GROUP_THREAD_CS_EXIT(_obj)  ucs_recursive_spin_unlock(&(_obj)->lock);
#else
#define UCG_GROUP_THREAD_CS_ENTER(_obj)
#define UCG_GROUP_THREAD_CS_EXIT(_obj)
#endif

static inline ucs_status_t ucg_group_plan(ucg_group_h group,
                                          const ucg_collective_params_t *params,
                                          ucg_plan_t **plan_p)
{
    ucg_plan_t *plan;
    ucs_status_t status;
    ucg_group_ctx_h gctx;
    ucg_plan_desc_t *planner;

    UCS_PROFILE_CODE("ucg_choose") {
        status = ucg_plan_choose(params, group, &planner, &gctx);
    }
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }

    UCS_PROFILE_CODE("ucg_plan") {
        status = planner->component->plan(gctx, &UCG_PARAM_TYPE(params), &plan);
    }
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }

#if ENABLE_MT
    status = ucs_recursive_spinlock_init(&plan->lock, 0);
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }
#endif

    if (group->params.field_mask & UCG_GROUP_PARAM_FIELD_MEMBER_INDEX) {
        plan->my_index = group->params.member_index;
    }
    if (group->params.field_mask & UCG_GROUP_PARAM_FIELD_MEMBER_COUNT) {
        plan->group_size = group->params.member_count;
    }

    ucs_list_head_init(&plan->op_head);

    plan->group_id = group->params.id;
    plan->planner  = planner;
    plan->group    = group;
    *plan_p        = plan;

    return UCS_OK;
}

ucs_status_t ucg_group_create(ucp_worker_h worker,
                              const ucg_group_params_t *params,
                              ucg_group_h *group_p)
{
    ucs_status_t status;
    struct ucg_group *group;
    ucg_context_t *ctx = ucs_container_of(worker->context, ucg_context_t, ucp_ctx);

    if (!ucs_test_all_flags(params->field_mask, UCG_GROUP_PARAM_REQUIRED_MASK)) {
        ucs_error("UCG is missing some critical group parameters");
        return UCS_ERR_INVALID_PARAM;
    }

    /* Allocate a new group */
    size_t dist_size  = sizeof(*params->distance) * params->member_count;
    size_t total_size = ctx->per_group_planners_ctx + dist_size;
    group             = UCS_ALLOC_CHECK(total_size, "communicator group");

    /* Fill in the group fields */
    group->is_barrier_outstanding = 0;
    group->is_cache_cleanup_due   = 0;
    group->context                = ctx;
    group->worker                 = worker;
    group->next_coll_id           = 1;

#if ENABLE_MT
    ucs_recursive_spinlock_init(&group->lock, 0);
#endif
    ucs_queue_head_init(&group->pending);
    memcpy((ucg_group_params_t*)&group->params, params, sizeof(*params));
    // TODO: replace memcpy with per-field copy to improve ABI compatibility
    group->params.distance = UCS_PTR_BYTE_OFFSET(group, total_size - dist_size);

    if (params->field_mask & UCG_GROUP_PARAM_FIELD_DISTANCES) {
        memcpy(group->params.distance, params->distance, dist_size);
    } else {
        /* If the user didn't specify the distances - treat as uniform */
        memset(group->params.distance, UCG_GROUP_MEMBER_DISTANCE_LAST, dist_size);
    }

    if ((params->field_mask & UCG_GROUP_PARAM_FIELD_ID) != 0) {
        ctx->next_group_id = ucs_max(ctx->next_group_id, group->params.id);
    } else {
        group->params.id = ++ctx->next_group_id;
        group->params.field_mask |= UCG_GROUP_PARAM_FIELD_ID;
    }

    /* Initialize the planners (loadable modules) */
    status = ucg_plan_group_create(group);
    if (status != UCS_OK) {
        goto cleanup_group;
    }

    // TODO: status = ucg_group_wireup_coll_ifaces(group); // based on my index

    status = UCS_STATS_NODE_ALLOC(&group->stats,
                                  &ucg_group_stats_class,
                                  worker->stats, "-%p", group);
    if (status != UCS_OK) {
        goto cleanup_group;
    }

    /* Clear the cache */
    group->cache_size = 0;
    memset(group->cache, 0, sizeof(group->cache));

    ucs_list_add_head(&ctx->groups_head, &group->list);

    *group_p = group;
    return UCS_OK;

cleanup_group:
    ucs_free(group);
    return status;
}

const ucg_group_params_t* ucg_group_get_params(ucg_group_h group)
{
    return &group->params;
}

static ucs_status_t ucg_group_cache_cleanup(ucg_group_h group)
{
    // TODO: implement... otherwise cache might grow too big!
    // TODO: use "op->discard_f(op);"
    return UCS_OK;
}

void ucg_group_destroy(ucg_group_h group)
{
    /* First - make sure all the collectives are completed */
    while (!ucs_queue_is_empty(&group->pending)) {
        ucp_worker_progress(group->worker);
    }

    UCG_GROUP_THREAD_CS_ENTER(group)

    ucg_plan_group_destroy(group);
    UCS_STATS_NODE_FREE(group->stats);
    ucs_list_del(&group->list);

    UCG_GROUP_THREAD_CS_EXIT(group)

#if ENABLE_MT
    ucs_recursive_spinlock_destroy(&group->lock);
#endif

    ucs_free(group);
}

ucg_collective_progress_t ucg_request_get_progress(ucg_coll_h coll)
{
    return ((ucg_op_t*)coll)->plan->planner->component->progress;
}

void ucg_request_cancel(ucg_coll_h coll)
{
    // TODO: implement
}

UCS_PROFILE_FUNC(ucs_status_t, ucg_collective_create,
        (group, params, coll), ucg_group_h group,
        const ucg_collective_params_t *params, ucg_coll_h *coll)
{
    int is_match;
    ucg_op_t *op;
    ucs_status_t status;

    uint16_t modifiers = UCG_PARAM_TYPE(params).modifiers;
    unsigned coll_mask = modifiers & UCG_GROUP_CACHE_MODIFIER_MASK;
    ucg_plan_t *plan   = group->cache[coll_mask];

    /* check the recycling/cache for this collective */
    if (ucs_likely(plan != NULL)) {
        UCG_GROUP_THREAD_CS_ENTER(plan)

        ucs_list_for_each(op, &plan->op_head, list) {
            /* we only need to compare the first 64 bytes of each set of cached
             * parameters against the given one (checked during compile time) */
            UCS_STATIC_ASSERT(sizeof(ucg_collective_params_t) ==
                              UCS_SYS_CACHE_LINE_SIZE);

#ifdef HAVE_UCP_EXTENSIONS
#ifdef __AVX512F__
            /* Only apply AVX512 to broadcast - otherwise risk CPU down-clocking! */
            if (plan->my_index == 0) {
#else
            if (1) {
#endif
                is_match = ucs_cpu_cache_line_is_equal(params, &op->params);
            } else
#endif
            is_match = (memcmp(params, &op->params, UCS_SYS_CACHE_LINE_SIZE) == 0);
            if (is_match) {
                ucs_list_del(&op->list);
                UCG_GROUP_THREAD_CS_EXIT(plan);
                status = UCS_OK;
                goto op_found;
            }
        }

        UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_PLANS_USED, 1);
    } else {
        ucs_trace_req("ucg_collective_create PLAN: type=%x root=%"PRIx64,
                      (unsigned)UCG_PARAM_TYPE(params).modifiers,
                      (uint64_t)UCG_PARAM_TYPE(params).root);

        /* create the actual plan for the collective operation */
        status = ucg_group_plan(group, params, &plan);
        if (status != UCS_OK) {
            goto out;
        }

        UCG_GROUP_THREAD_CS_ENTER(plan);

        group->cache[coll_mask] = plan;

        UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_PLANS_CREATED, 1);
    }

    UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_OPS_CREATED, 1);

    UCS_PROFILE_CODE("ucg_prepare") {
        status = plan->planner->component->prepare(plan, params, &op);
    }

    UCG_GROUP_THREAD_CS_EXIT(plan);

    if (status != UCS_OK) {
        goto out;
    }

    ucs_trace_req("ucg_collective_create OP: planner=%s(%s) "
                  "params={type=%u, root=%lu, send=[%p,%lu,%p], "
                  "recv=[%p,%lu,%p], op/displs=%p}",
                  plan->planner->name, plan->planner->component->name,
                  (uint16_t)UCG_PARAM_TYPE(params).modifiers,
                  (uint64_t)UCG_PARAM_TYPE(params).root,
                  params->send.buffer, params->send.count, params->send.dtype,
                  params->recv.buffer, params->recv.count, params->recv.dtype,
                  UCG_PARAM_OP(params));

    if (ucs_unlikely(++group->cache_size >
                     group->context->config.group_cache_size_thresh)) {
        group->is_cache_cleanup_due = 1;
    }

op_found:
    *coll = op;

out:
    return status;
}

ucs_status_t static UCS_F_ALWAYS_INLINE
ucg_collective_trigger(ucg_group_h group, ucg_op_t *op, void *req)
{
    ucs_status_t ret;

    /* Start the first step of the collective operation */
    UCS_PROFILE_CODE("ucg_trigger") {
        ret = op->trigger_f(op, group->next_coll_id++, req);
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
    ucs_status_t status;

    ucs_assert(group->is_barrier_outstanding == 1);
    group->is_barrier_outstanding = 0;

    UCG_GROUP_THREAD_CS_ENTER(group)

    if (!ucs_queue_is_empty(&group->pending)) {
        do {
            /* Start the next pending operation */
            ucg_op_t *op = (ucg_op_t*)ucs_queue_pull_non_empty(&group->pending);
            status = ucg_collective_trigger(group, op, op->pending_req);
        } while ((!ucs_queue_is_empty(&group->pending)) &&
                 (!group->is_barrier_outstanding) &&
                 (status == UCS_OK));
    } else {
        status = UCS_OK;
    }

    UCG_GROUP_THREAD_CS_EXIT(group)

    return status;
}

UCS_PROFILE_FUNC(ucs_status_t, ucg_collective_start, (coll, req),
                 ucg_coll_h coll, void *req)
{
    ucs_status_t ret;
    ucg_op_t *op = (ucg_op_t*)coll;
    ucg_group_h group = op->plan->group;

    ucs_trace_req("ucg_collective_start: op=%p req=%p", coll, req);

    UCG_GROUP_THREAD_CS_ENTER(group)

    if (ucs_unlikely(group->is_barrier_outstanding)) {
        ucs_queue_push(&group->pending, &op->queue);
        op->pending_req = req;
        ret = UCS_INPROGRESS;
    } else {
        ret = ucg_collective_trigger(group, op, req);
    }

    if (ucs_unlikely(group->is_cache_cleanup_due) && !UCS_STATUS_IS_ERR(ret)) {
        ucg_group_cache_cleanup(group);
    }

    UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_OPS_USED, 1);

    UCG_GROUP_THREAD_CS_EXIT(group)

    return ret;
}

void ucg_collective_destroy(ucg_coll_h coll)
{
    ucg_op_t *op     = (ucg_op_t*)coll;
    ucg_plan_t *plan = op->plan;

    UCG_GROUP_THREAD_CS_ENTER(plan);

    ucs_list_add_head(&plan->op_head, &op->list);

    UCG_GROUP_THREAD_CS_EXIT(plan);
}
