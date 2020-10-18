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

enum ucg_group_cmp_mode {
    UCG_GROUP_CMP_MODE_FULL      = 0,
    UCG_GROUP_CMP_MODE_RECV_ONLY = UCG_GROUP_COLLECTIVE_MODIFIER_IGNORE_SEND >> 14,
    UCG_GROUP_CMP_MODE_SEND_ONLY = UCG_GROUP_COLLECTIVE_MODIFIER_IGNORE_RECV >> 14,
    UCG_GROUP_CMP_MODE_BARRIER   = (UCG_GROUP_COLLECTIVE_MODIFIER_IGNORE_SEND |
                                    UCG_GROUP_COLLECTIVE_MODIFIER_IGNORE_RECV) >> 14
};

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
    unsigned ret = ucg_plan_group_progress(group);
    if (ret) {
        return ret;
    }

    return ucg_worker_progress(group->worker);
}

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
        status = planner->component->plan(gctx, &params->type, &plan);
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

static ucs_status_t ucg_group_init_cache(ucg_group_h group)
{
    ucs_status_t status;
    ucg_collective_params_t coll_params = {
        .type = {
            .modifiers = UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE   |
                         UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST   |
                         UCG_GROUP_COLLECTIVE_MODIFIER_BARRIER     |
                         UCG_GROUP_COLLECTIVE_MODIFIER_IGNORE_SEND |
                         UCG_GROUP_COLLECTIVE_MODIFIER_IGNORE_RECV,
            .root      = 0
        },
        .comp_cb = NULL
    };

    /* Clear the cache */
    group->cache_size = 0;
    memset(group->cache, 0, sizeof(group->cache));

    /* Create a plan for barrier, so that we can assume it exists in run-time */
    status = ucg_group_plan(group, &coll_params, &group->barrier_cache);
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }

    /* Also plan the non-barrier counter-part - so that fast-path runs right */
    coll_params.type.modifiers &= UCG_GROUP_CACHE_MODIFIER_MASK;
    return ucg_group_plan(group, &coll_params,
                          &group->cache[coll_params.type.modifiers]);
}

ucs_status_t ucg_group_create(ucp_worker_h worker,
                              const ucg_group_params_t *params,
                              ucg_group_h *group_p)
{
    ucs_status_t status;
    ucg_context_t *ctx = ucg_worker_get_context(worker);

    if (!ucs_test_all_flags(params->field_mask, UCG_GROUP_PARAM_REQUIRED_MASK)) {
        ucs_error("UCG is missing some critical group parameters");
        return UCS_ERR_INVALID_PARAM;
    }

    /* Allocate a new group */
    size_t distance_size    = sizeof(*params->distance) * params->member_count;
    size_t total_group_size = sizeof(struct ucg_group) +
                              ctx->per_group_planners_ctx + distance_size;
    struct ucg_group *group = UCS_ALLOC_CHECK(total_group_size,
                                              "communicator group");

    /* Fill in the group fields */
    group->is_barrier_outstanding = 0;
    group->is_cache_cleanup_due   = 0;
    group->context                = ctx;
    group->worker                 = worker;
    group->next_coll_id           = 0;
    group->iface_cnt              = 0;

#if ENABLE_MT
    ucs_recursive_spinlock_init(&group->lock, 0);
#endif
    ucs_queue_head_init(&group->pending);
    memcpy((ucg_group_params_t*)&group->params, params, sizeof(*params));
    // TODO: replace memcpy with per-field copy to improve ABI compatibility
    group->params.distance = (typeof(params->distance))((char*)(group
            + 1) + ctx->per_group_planners_ctx);

    if (params->field_mask & UCG_GROUP_PARAM_FIELD_DISTANCES) {
        memcpy(group->params.distance, params->distance, distance_size);
    } else {
        /* If the user didn't specify the distances - treat as uniform */
        memset(group->params.distance, UCG_GROUP_MEMBER_DISTANCE_LAST,
               distance_size);
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

    status = ucg_group_init_cache(group);
    if (status != UCS_OK) {
        goto cleanup_group;
    }

    status = UCS_STATS_NODE_ALLOC(&group->stats,
                                  &ucg_group_stats_class,
                                  worker->stats, "-%p", group);
    if (status != UCS_OK) {
        goto cleanup_group;
    }

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
        ucg_group_progress(group);
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

void ucg_request_cancel(ucg_group_h group, ucg_request_t *req)
{
    // TODO: implement
}

static UCS_F_ALWAYS_INLINE int
ucg_group_is_matching_op(enum ucg_group_cmp_mode mode,
                         const ucg_collective_params_t *a,
                         const ucg_collective_params_t *b)
{
    size_t cmp_size;
    size_t cmp_offset;

    switch (mode) {
    case UCG_GROUP_CMP_MODE_FULL:
        cmp_size   = sizeof(ucg_collective_params_t);
        cmp_offset = 0;
        break;

    case UCG_GROUP_CMP_MODE_SEND_ONLY:
        cmp_size   = offsetof(ucg_collective_params_t, recv);
        cmp_offset = 0;
        break;

    case UCG_GROUP_CMP_MODE_RECV_ONLY:
        cmp_size   = sizeof(ucg_collective_params_t) -
                     offsetof(ucg_collective_params_t, recv);
        cmp_offset = offsetof(ucg_collective_params_t, recv);
        break;

    case UCG_GROUP_CMP_MODE_BARRIER:
        /* Barrier only - has a different solution... will never get here */
        ucs_assert_always(0);
        break;
    }

    ucs_assert((mode == UCG_GROUP_CMP_MODE_RECV_ONLY) ||
               ((a->recv_only.type.modifiers == 0) &&
                (b->recv_only.type.modifiers == 0) &&
                (a->recv_only.type.root == 0)      &&
                (b->recv_only.type.root == 0)      &&
                (a->recv_only.comp_cb == 0)        &&
                (b->recv_only.comp_cb == 0)));

    ucs_assert(((uintptr_t)a % UCS_SYS_CACHE_LINE_SIZE) == 0);
    ucs_assert(((uintptr_t)b % UCS_SYS_CACHE_LINE_SIZE) == 0);
    ucs_assert((cmp_size     % UCS_SYS_CACHE_LINE_SIZE) == 0);
    ucs_assert((cmp_offset   % UCS_SYS_CACHE_LINE_SIZE) == 0);

    return (0 == memcmp((uint8_t*)a + cmp_offset,
                        (uint8_t*)b + cmp_offset,
                        cmp_size));
}

UCS_PROFILE_FUNC(ucs_status_t, ucg_collective_create,
        (group, params, coll), ucg_group_h group,
        const ucg_collective_params_t *params, ucg_coll_h *coll)
{
    ucg_op_t *op;
    ucs_status_t status;

    uint16_t modifiers               = params->type.modifiers;
    unsigned coll_mask               = modifiers & UCG_GROUP_CACHE_MODIFIER_MASK;
    ucg_plan_t *plan                 = group->cache[coll_mask];
    unsigned cmp_bit_offset          = ucs_count_trailing_zero_bits(
                                       UCG_GROUP_COLLECTIVE_MODIFIER_IGNORE_SEND);
    enum ucg_group_cmp_mode cmp_mode = (enum ucg_group_cmp_mode)(modifiers >>
                                                                 cmp_bit_offset);


    /* check the recycling/cache for this collective */
    if (ucs_likely(plan != NULL)) {

        UCG_GROUP_THREAD_CS_ENTER(plan)

        switch (cmp_mode) {
        case UCG_GROUP_CMP_MODE_FULL:
            ucs_list_for_each(op, &plan->op_head, list) {
                if (ucg_group_is_matching_op(UCG_GROUP_CMP_MODE_FULL,
                                             &op->params, params)) {
                    ucs_list_del(&op->list);
                    UCG_GROUP_THREAD_CS_EXIT(plan);
                    status = UCS_OK;
                    goto op_found;
                }
            }
            break;

        case UCG_GROUP_CMP_MODE_SEND_ONLY:
            ucs_list_for_each(op, &plan->op_head, list) {
                if (ucg_group_is_matching_op(UCG_GROUP_CMP_MODE_SEND_ONLY,
                                             &op->params, params)) {
                    ucs_list_del(&op->list);
                    UCG_GROUP_THREAD_CS_EXIT(plan);
                    status = UCS_OK;
                    goto op_found;
                }
            }
            break;

        case UCG_GROUP_CMP_MODE_RECV_ONLY:
            ucs_list_for_each(op, &plan->op_head, list) {
                if (ucg_group_is_matching_op(UCG_GROUP_CMP_MODE_RECV_ONLY,
                                             &op->params, params)) {
                    ucs_list_del(&op->list);
                    UCG_GROUP_THREAD_CS_EXIT(plan);
                    status = UCS_OK;
                    goto op_found;
                }
            }
            break;

        case UCG_GROUP_CMP_MODE_BARRIER:
            /* No need to look for a match - barrier is pre-calculated */
            plan = group->barrier_cache;
            if (ucs_likely(!ucs_list_is_empty(&plan->op_head))) {
                op = ucs_list_extract_head(&plan->op_head, ucg_op_t, list);
                UCG_GROUP_THREAD_CS_EXIT(plan);
                status = UCS_OK;
                goto op_found;
            }
            break;
        }

        UCG_GROUP_THREAD_CS_EXIT(plan);

        UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_PLANS_USED, 1);
    } else {
        ucs_assert((modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_BARRIER) == 0);
        ucs_trace_req("ucg_collective_create PLAN: type=%x root=%"PRIx64,
                      params->type.modifiers, (uint64_t)params->type.root);

        /* create the actual plan for the collective operation */
        status = ucg_group_plan(group, params, &plan);
        if (status != UCS_OK) {
            goto out;
        }

        group->cache[coll_mask] = plan;

        UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_PLANS_CREATED, 1);
    }

    UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_OPS_CREATED, 1);

    UCS_PROFILE_CODE("ucg_prepare") {
        status = plan->planner->component->prepare(plan, params, &op);
    }
    if (status != UCS_OK) {
        goto out;
    }

    ucs_trace_req("ucg_collective_create OP: planner=%s(%s) "
                  "params={type=%u, root=%lu, send=[%p,%lu,%lu,%p,%p], "
                  "recv=[%p,%lu,%lu,%p,%p], cb=%p(%p), op=%p}",
                  plan->planner->name, plan->planner->component->name,
                  (uint16_t)params->type.modifiers,
                  (uint64_t)params->type.root, params->send.buffer,
                  params->send.count, params->send.dt_len,
                  params->send.dt_ext, params->send.displs,
                  params->recv.buffer, params->recv.count, params->recv.dt_len,
                  params->recv.dt_ext, params->recv.displs,
                  params->comp_cb, params->recv_only.comp_cb,
                  params->recv.op_ext);

    if (ucs_unlikely(++group->cache_size >
                     group->context->config.group_cache_size_thresh)) {
        group->is_cache_cleanup_due = 1;
    }

op_found:
    *coll = op;

out:
    UCG_GROUP_THREAD_CS_EXIT(group)

    return status;
}

ucs_status_t static UCS_F_ALWAYS_INLINE
ucg_collective_trigger(ucg_group_h group, ucg_op_t *op, ucg_request_t *req)
{
    ucs_status_t ret;

    /* Start the first step of the collective operation */
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
    ucs_assert(group->is_barrier_outstanding == 1);
    group->is_barrier_outstanding = 0;

    UCG_GROUP_THREAD_CS_ENTER(group)

    if (ucs_queue_is_empty(&group->pending)) {
        return UCS_OK;
    }

    ucs_status_t ret;
    do {
        /* Start the next pending operation */
        ucg_op_t *op = (ucg_op_t*)ucs_queue_pull_non_empty(&group->pending);
        ret = ucg_collective_trigger(group, op, op->pending_req);
    } while ((!ucs_queue_is_empty(&group->pending)) &&
             (!group->is_barrier_outstanding) &&
             (ret == UCS_OK));

    UCG_GROUP_THREAD_CS_EXIT(group)

    return ret;
}

UCS_PROFILE_FUNC(ucs_status_t, ucg_collective_start, (coll, req),
                 ucg_coll_h coll, ucg_request_t *req)
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

    ucs_recursive_spin_lock(&plan->lock);

    ucs_list_add_head(&plan->op_head, &op->list);

    ucs_recursive_spin_unlock(&plan->lock);
}
