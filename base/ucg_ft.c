/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "ucg_ft.h"
#include "ucg_group.h"

#include <ucp/core/ucp_context.h>
#include <ucs/datastruct/mpool.inl>

__KHASH_IMPL(ucg_ft_ep, static UCS_F_MAYBE_UNUSED inline, ucg_group_member_index_t,
        uct_ep_h, 1, kh_int64_hash_func, kh_int64_hash_equal)

#if ENABLE_FAULT_TOLERANCE
#if ENABLE_STATS
/**
 * UCG fault-tolerance statistics counters
 */
enum {
    UCG_FT_STAT_START,

    UCG_FT_STAT_SENT_KEEPALIVE,
    UCG_FT_STAT_SENT_ACK,
    UCG_FT_STAT_SENT_FAULT,

    UCG_FT_STAT_GOT_KEEPALIVE,
    UCG_FT_STAT_GOT_ACK,
    UCG_FT_STAT_GOT_FAULT,

    UCG_FT_STAT_LAST
};

static ucs_stats_class_t ucg_ft_stats_class = {
    .name           = "ucg_ft",
    .num_counters   = UCG_FT_STAT_LAST,
    .counter_names  = {
        [UCG_FT_STAT_START]          = "ft_interactions",
        [UCG_FT_STAT_SENT_KEEPALIVE] = "keepalives_sent",
        [UCG_FT_STAT_SENT_ACK]       = "acks_sent",
        [UCG_FT_STAT_SENT_FAULT]     = "faults_sent",
        [UCG_FT_STAT_GOT_KEEPALIVE]  = "keepalives_gotten",
        [UCG_FT_STAT_GOT_ACK]        = "acks_gotten",
        [UCG_FT_STAT_GOT_FAULT]      = "faults_gotten",
    }
};
#endif

struct ucg_ft_handle {
    ucs_timer_queue_t *remainder;
    ucs_timer_t       *location_hint;
};

enum ucg_ft_msg_type {
    UCG_FT_MSG_KEEPALIVE,
    UCG_FT_MSG_ACK,
    UCG_FT_MSG_FAULT
};

typedef union ucg_ft_msg {
    struct {
        ucg_group_member_index_t index :62;
        enum ucg_ft_msg_type     type  :2;
    };
    uint64_t                     am_header;
} ucg_ft_msg_t;

static UCS_F_ALWAYS_INLINE ucs_status_t ucg_ft_send(ucg_ft_ctx_t *ctx,
        enum ucg_ft_msg_type type, uct_ep_t *ep, ucg_group_member_index_t index)
{
    ucs_status_t status;
    ucg_ft_msg_t msg = {
            .type  = type,
            .index = (type == UCG_FT_MSG_FAULT) ? index : ctx->my_index
    };

    UCS_STATS_UPDATE_COUNTER(ctx->stats, UCG_FT_STAT_SENT_KEEPALIVE, 1);

    do {
        status = ep->iface->ops.ep_am_short(ep, ctx->ft_am_id, msg.am_header, NULL, 0);
    } while (status != UCS_INPROGRESS);
    return status;
}

static UCS_F_ALWAYS_INLINE ucs_status_t ucg_ft_get_ctx(ucg_group_h group,
        ucg_group_member_index_t *index_p, ucg_ft_ctx_t **ctx_p)
{
    *ctx_p = &UCG_WORKER_TO_GROUPS_CTX(group->worker)->ft_ctx;
    // TODO: convert index from sub-communicator to communicator!
    return UCS_OK;
}

static UCS_F_ALWAYS_INLINE ucs_status_t ucg_ft_get_ep(ucg_ft_ctx_t *ctx,
        ucg_group_member_index_t index, uct_ep_h *ep_p)
{
    khiter_t iter = kh_get(ucg_ft_ep, &ctx->ep_ht, index);
    if (iter != kh_end(&ctx->ep_ht)) {
        *ep_p = kh_value(&ctx->ep_ht, iter);
        return UCS_OK;
    }

    /* Index not found in the hash-table - obtain a new endpoint */
    uct_md_h dummy_md;
    const uct_md_attr_t* dummy_md_attr;
    const uct_iface_attr_t *dummy_ep_attr;
    ucs_status_t status = ucg_plan_connect(ctx->main_grp, index, 0, ep_p,
            &dummy_ep_attr, &dummy_md, &dummy_md_attr);
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }

    /* Store the new endpoint in the hash-table, for next time */
    int ret;
    iter = kh_put(ucg_ft_ep, &ctx->ep_ht, index, &ret);
    if (ucs_unlikely(ret != 0)) {
        return UCS_ERR_NO_RESOURCE;
    }
    kh_value(&ctx->ep_ht, iter) = *ep_p;
    return UCS_OK;
}

ucs_status_t ucg_ft_start(ucg_group_h group, ucg_group_member_index_t index,
        uct_ep_h optional_ep, ucg_ft_h *handle_p)
{
    ucg_ft_ctx_t *ctx;
    ucs_status_t status = ucg_ft_get_ctx(group, &index, &ctx);
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }

    if (ucs_unlikely(optional_ep == NULL)) {
        status = ucg_ft_get_ep(ctx, index, &optional_ep);
        if (ucs_unlikely(status != UCS_OK)) {
            return status;
        }
    } else if (ucs_unlikely(ctx->config.ft_mode != UCG_FT_MODE_ON_DEMAND)) {
        if (handle_p) {
            *handle_p = NULL;
        }
        return UCS_OK;
    }

    status = ucg_ft_send(ctx, UCG_FT_MSG_KEEPALIVE, optional_ep, 0);
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }

    ucs_timer_t **removal_hint;
    if (ucs_likely(handle_p != NULL)) {
        *handle_p              = ucs_mpool_get_inline(&ctx->handle_mp);
        (*handle_p)->remainder = &ctx->reminder;
        removal_hint           = &(*handle_p)->location_hint;
    } else {
        removal_hint           = NULL;
    }

    UCS_STATS_UPDATE_COUNTER(ctx->stats, UCG_FT_STAT_START, 1);
    return ucs_timerq_add(&ctx->reminder, index,
            ctx->config.timeout_after_keepalive, removal_hint);
}

ucs_status_t ucg_ft_end(ucg_ft_h handle, ucg_group_member_index_t index)
{
    ucs_status_t status = ucs_timerq_remove(handle->remainder, index, handle->location_hint);
    ucs_mpool_put_inline(handle);
    return status;
}

ucs_status_t ucg_ft_propagate(ucg_group_h group,
        const ucg_group_params_t *params, uct_ep_h new_ep)
{
    ucg_ft_ctx_t *ctx;
    ucs_status_t status = UCS_OK;
    ucg_group_member_index_t member_idx = 0;
    while ((status == UCS_OK) && (member_idx < params->member_count)) {
        if (params->distance[member_idx] == UCG_GROUP_MEMBER_DISTANCE_FAULT) {
            ucg_group_member_index_t global_idx = member_idx;
            status = ucg_ft_get_ctx(group, &global_idx, &ctx);
            if (ucs_likely(status == UCS_OK)) {
                status = ucg_ft_send(ctx, UCG_FT_MSG_FAULT, new_ep, member_idx);
            }
        }
        member_idx++;
    }
    return status;
}

ucs_status_t ucg_ft_handler(void *arg, void *data, size_t length, unsigned flags)
{
    uct_ep_h ep;
    ucs_status_t status;
    ucg_ft_ctx_t *ctx = (ucg_ft_ctx_t*)arg;
    ucg_ft_msg_t *msg = (ucg_ft_msg_t*)data;

    switch (msg->type) {
    case UCG_FT_MSG_KEEPALIVE:
        /* Find the endpoint pointing to the KEEPALIVE sender */
        status = ucg_ft_get_ep(ctx, msg->index, &ep);
        if (ucs_unlikely(status != UCS_OK)) {
            return status;
        }

        /* Send back an acknowledgement */
        UCS_STATS_UPDATE_COUNTER(group->stats, UCG_FT_STAT_GOT_KEEPALIVE, 1);
        return ucg_ft_send(ctx, UCG_FT_MSG_ACK, ep, msg->index);

    case UCG_FT_MSG_ACK:
        /* Delay the next time this index gets the timerq's attention */
        UCS_STATS_UPDATE_COUNTER(group->stats, UCG_FT_STAT_GOT_ACK, 1);
        return ucs_timerq_modify(&ctx->reminder, msg->index,
                ctx->config.timeout_between_keepalives);

    case UCG_FT_MSG_FAULT:
        UCS_STATS_UPDATE_COUNTER(group->stats, UCG_FT_STAT_GOT_FAULT, 1);
        ucs_warn("UCG was notified of a fault on group member #%lu", (uint64_t)msg->index);

        /* Disregard this index - which may not even be monitored at the moment */
        (void) ucs_timerq_remove(&ctx->reminder, (uint64_t)msg->index, NULL);

        /* Notify every component about the fault */
        return ctx->fault_cb(ctx->fault_arg, msg->index);

    default:
        return UCS_ERR_NOT_IMPLEMENTED; /* Future-proofing */
    }
    return UCS_OK;
}

void ucg_ft_msg_dump(ucp_worker_h worker, uct_am_trace_type_t type,
        uint8_t id, const void *data, size_t length, char *buffer, size_t max)
{
    // TODO: implement
}

static void ucg_ft_slow_progress(int timer_id, void *arg)
{
    ucs_timer_t *timer;
    ucg_ft_ctx_t *ctx = arg;
    ucg_group_member_index_t index;
    double timeout = ctx->config.timeout_after_keepalive;
    ucs_trace_async("UCG fault-tolerance slow_timer_sweep");

    ucs_timerq_for_each_expired(timer, &ctx->reminder, ucs_get_time(), {
        index = timer->id;
        /* Determine the type of timeout based on the timer interval */
        if (timer->interval == timeout) {
            goto fault_detected;
        }

        /* Send a KEEPALIVE message to that destination */
        uct_ep_h ep;
        (void) ucg_ft_get_ep(ctx, index, &ep);
        /* Intentionally ignoring errors - ep guaraneed to be found */
        (void) ucg_ft_send(ctx, UCG_FT_MSG_KEEPALIVE, ep, index);
        /* Intentionally ignoring errors - still counts as an attempt */

        /* Adjust the timers following the sent KEEPALIVE message */
        timer->interval   = timeout;
        timer->expiration = ucs_get_time() + timeout;
    })
    return;

fault_detected:
    ucs_warn("UCG detected a fault on group member #%lu", index);
    (void) ctx->fault_cb(ctx->fault_arg, index);
    ucs_timerq_remove(&ctx->reminder, index, NULL);
}

ucs_mpool_ops_t ucg_ft_mpool_ops = {
    .chunk_alloc   = ucs_mpool_hugetlb_malloc,
    .chunk_release = ucs_mpool_hugetlb_free,
    .obj_init      = ucs_empty_function,
    .obj_cleanup   = ucs_empty_function
};

ucs_status_t ucg_ft_init(ucs_async_context_t *async, ucg_group_h main_group,
        uint8_t ft_am_id, ucg_ft_fault_cb_t notify_cb, void *notify_arg,
        ucg_ft_ctx_t *ctx)
{
    /* Find my index in the distance array */
    ucg_group_member_index_t index = 0;
    while ((main_group->params.distance[index] != UCG_GROUP_MEMBER_DISTANCE_SELF) &&
           (index < main_group->params.member_count)) {
        index++;
    }

    if (ucs_unlikely(index == main_group->params.member_count)) {
        ucs_error("UCG Group's distance array does not contain its own rank");
        return UCS_ERR_INVALID_PARAM;
    }

    /* Initialize the contents of the fault-tolerance context */
    ctx->main_grp  = main_group;
    ctx->my_index  = index;
    ctx->ft_am_id  = ft_am_id;
    ctx->fault_cb  = notify_cb;
    ctx->fault_arg = notify_arg;
    kh_init_inplace(ucg_ft_ep, &ctx->ep_ht);
    ucs_status_t status = ucs_mpool_init(&ctx->handle_mp, 0,
            sizeof(struct ucg_ft_handle), 0, UCS_SYS_CACHE_LINE_SIZE,
            1, UINT_MAX, &ucg_ft_mpool_ops, "ucg_ft_handles_mp");

    // TODO: FIXME: config

    /* Start the fault-tolerance monitoring according to the configured mode */
    switch (ctx->config.ft_mode) {
    case UCG_FT_MODE_NONE:
    case UCG_FT_MODE_ON_DEMAND:
        break;

    case UCG_FT_MODE_NEIGHBORS:
        /* Monitor the next neighbor */
        index = (index + 1) % main_group->params.member_count;
        status = ucg_ft_start(main_group, index, NULL, NULL);
        if (status != UCS_OK) {
            return status;
        }

        /* Monitor the previous neighbor */
        index = (index + main_group->params.member_count - 2) %
                main_group->params.member_count;
        status = ucg_ft_start(main_group, index, NULL, NULL);
        if (status != UCS_OK) {
            return status;
        }
        break;

    default:
        ucs_error("Invalid fault-tolerance mode selected");
        return UCS_ERR_INVALID_PARAM;
    }

    /* Register a periodic callback for fault-tolerance timeouts */
    status = ucs_async_add_timer(UCS_ASYNC_MODE_THREAD_SPINLOCK,
            ctx->config.timeout_after_keepalive, ucg_ft_slow_progress, ctx,
            async, &ctx->timer_id);
    if (status != UCS_OK) {
        kh_destroy_inplace(ucg_ft_ep, &ctx->ep_ht);
        return status;
    }

    /* Initialize the handler for FT messages */
    ucp_am_handler_t* am_handler = ucp_am_handlers + ft_am_id;
    am_handler->features         = UCP_FEATURE_GROUPS;
    am_handler->cb               = ucg_ft_handler;
    am_handler->tracer           = ucg_ft_msg_dump;
    am_handler->flags            = 0;
    return UCS_OK;
}

void ucg_ft_cleanup(ucg_ft_ctx_t *ctx)
{
    kh_destroy_inplace(ucg_ft_ep, &ctx->ep_ht);
    ucs_async_remove_handler(ctx->timer_id, 0);
    ucs_mpool_cleanup(&ctx->handle_mp, 1);
}

#endif /* ENABLE_FAULT_TOLERANCE */
