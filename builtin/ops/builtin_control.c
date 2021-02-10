/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <stddef.h>
#include <ucs/sys/compiler_def.h>
#include <ucp/dt/dt_contig.h>

#include "builtin_ops.h"
#include "builtin_comp_step.inl"

/*
 * Below is a list of possible callback functions for operation initialization.
 */

static void ucg_builtin_init_barrier(ucg_builtin_op_t *op, ucg_coll_id_t coll_id)
{
    ucg_collective_acquire_barrier(op->super.plan->group);
}

static void ucg_builtin_finalize_barrier(ucg_builtin_op_t *op)
{
    ucg_collective_release_barrier(op->super.plan->group);
}

static void ucg_builtin_init_gather_waypoint(ucg_builtin_op_t *op, ucg_coll_id_t coll_id)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    memcpy(step->recv_buffer, step->send_buffer, step->buffer_length);
    ucs_assert(step->flags & UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED);
}

static void ucg_builtin_init_gather_terminal(ucg_builtin_op_t *op, ucg_coll_id_t coll_id)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    size_t len = step->buffer_length;
    memcpy(step->recv_buffer + (UCG_PARAM_TYPE(&op->super.params).root * len),
           step->send_buffer, len);
    ucs_assert((step->flags & UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED) == 0);
}

static void ucg_builtin_init_reduce(ucg_builtin_op_t *op, ucg_coll_id_t coll_id)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    memcpy(step->recv_buffer, op->super.params.send.buffer, step->buffer_length);
}

static UCS_F_ALWAYS_INLINE void
ucg_builtin_init_state(ucg_builtin_op_t *op, int is_pack)
{
    ucp_dt_generic_t *dt_gen;

    const ucg_collective_params_t *params = &op->super.params;

    if (is_pack) {
        dt_gen = ucp_dt_to_generic(op->send_dt);
        op->rstate.dt.generic.state = dt_gen->ops.start_pack(dt_gen->context,
                                                             params->send.buffer,
                                                             params->send.count);
    } else {
        dt_gen = ucp_dt_to_generic(op->recv_dt);
        op->wstate.dt.generic.state = dt_gen->ops.start_unpack(dt_gen->context,
                                                               params->recv.buffer,
                                                               params->recv.count);
    }
    // TODO: re-use ucp_request_send_state_init()+ucp_request_send_state_reset()?
}

static UCS_F_ALWAYS_INLINE void
ucg_builtin_finalize_state(ucg_builtin_op_t *op, int is_pack)
{
    ucp_dt_generic_t *dt_gen;

    if (is_pack) {
        dt_gen = ucp_dt_to_generic(op->send_dt);
        dt_gen->ops.finish(op->rstate.dt.generic.state);
    } else {
        dt_gen = ucp_dt_to_generic(op->recv_dt);
        dt_gen->ops.finish(op->wstate.dt.generic.state);
    }
}

static void ucg_builtin_init_pack(ucg_builtin_op_t *op, ucg_coll_id_t coll_id)
{
    ucg_builtin_init_state(op, 1);
}

static void ucg_builtin_init_unpack(ucg_builtin_op_t *op, ucg_coll_id_t coll_id)
{
    ucg_builtin_init_state(op, 0);
}

static void ucg_builtin_init_pack_and_unpack(ucg_builtin_op_t *op,
                                             ucg_coll_id_t coll_id)
{
    ucg_builtin_init_state(op, 1);
    ucg_builtin_init_state(op, 0);
}

static void ucg_builtin_finalize_pack(ucg_builtin_op_t *op)
{
    ucg_builtin_finalize_state(op, 1);
}

static void ucg_builtin_finalize_unpack(ucg_builtin_op_t *op)
{
    ucg_builtin_finalize_state(op, 0);
}

static void ucg_builtin_finalize_pack_and_unpack(ucg_builtin_op_t *op)
{
    ucg_builtin_finalize_state(op, 1);
    ucg_builtin_finalize_state(op, 0);
}

/* Alltoall Bruck phase 1/3: shuffle the data */
static void ucg_builtin_init_alltoall(ucg_builtin_op_t *op, ucg_coll_id_t coll_id)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    int bsize                   = step->buffer_length;
    int my_idx                  = op->super.plan->my_index;
    int nProcs                  = op->super.plan->group_size;
    int ii;

    /* Shuffle data: rank i displaces all data blocks "i blocks" upwards */
    for(ii=0; ii < nProcs; ii++){
        memcpy(step->send_buffer + bsize * ii,
               step->recv_buffer + bsize * ((ii + my_idx) % nProcs),
               bsize);
    }
}

/* Alltoall Bruck phase 2/3: send data
static void ucg_builtin_calc_alltoall(ucg_builtin_request_t *req, uint8_t *send_count,
                                      size_t *base_offset, size_t *item_interval)
{
    int kk, nProcs = req->op->super.plan->group_size;

    // k = ceil( log(nProcs) / log(2) ) communication steps
    //      - For each step k, rank (i+2^k) sends all the data blocks whose k^{th} bits are 1
    for(kk = 0; kk < ceil( log(nProcs) / log(2) ); kk++){
        unsigned bit_k    = UCS_BIT(kk);
        send_count   [kk] = bit_k;
        base_offset  [kk] = bit_k;
        item_interval[kk] = bit_k;
    }
} // TODO: re-apply the calculation in builtin_data.c
*/

/* Alltoall Bruck phase 3/3: shuffle the data */
static void ucg_builtin_finalize_alltoall(ucg_builtin_op_t *op)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    int bsize                   = step->buffer_length;
    int nProcs                  = op->super.plan->group_size;
    int ii;

    /* Shuffle data: rank i displaces all data blocks up by i+1 blocks and inverts vector */
    for(ii = 0; ii < nProcs; ii++){
        memcpy(step->send_buffer + bsize * ii,
               step->recv_buffer + bsize * (nProcs - 1 - ii),
               bsize);
    }
}

void ucg_builtin_init_scatter(ucg_builtin_op_t *op, ucg_coll_id_t coll_id)
{
    ucg_builtin_plan_t *plan    = ucs_derived_of(op->super.plan, ucg_builtin_plan_t);
    void *dst                   = op->steps[plan->phs_cnt - 1].recv_buffer;
    ucg_builtin_op_step_t *step = &op->steps[0];
    void *src                   = step->send_buffer;
    size_t length               = step->buffer_length;
    size_t offset               = length * plan->super.my_index;

    if (dst != src) {
        memcpy(dst, src + offset, length);
    }
}

void ucg_builtin_print_init_cb_name(ucg_builtin_op_init_cb_t init_cb)
{
    if (init_cb == NULL) {
        printf("none");
    } else if (init_cb == ucg_builtin_init_barrier) {
        printf("barrier");
    } else if (init_cb == ucg_builtin_init_reduce) {
        printf("reduce");
    } else if (init_cb == ucg_builtin_init_gather_terminal) {
        printf("gather (terminal)");
    } else if (init_cb == ucg_builtin_init_gather_waypoint) {
        printf("gather (waypoint)");
    } else if (init_cb == ucg_builtin_init_alltoall) {
        printf("alltoall");
    } else if (init_cb == ucg_builtin_init_scatter) {
        printf("scatter");
    } else if (init_cb == ucg_builtin_init_pack) {
        printf("pack");
    } else if (init_cb == ucg_builtin_init_unpack) {
        printf("unpack");
    } else if (init_cb == ucg_builtin_init_pack_and_unpack) {
        printf("pack + unpack");
    } else {
        printf("\n");
        ucs_error("unrecognized operation initialization function");
    }
}

void ucg_builtin_print_fini_cb_name(ucg_builtin_op_fini_cb_t fini_cb)
{
    if (fini_cb == NULL) {
        printf("none");
    } else if (fini_cb == ucg_builtin_finalize_barrier) {
        printf("barrier");
    } else if (fini_cb == ucg_builtin_finalize_alltoall) {
        printf("alltoall");
    } else if (fini_cb == ucg_builtin_finalize_pack) {
        printf("pack");
    } else if (fini_cb == ucg_builtin_finalize_unpack) {
        printf("unpack");
    } else if (fini_cb == ucg_builtin_finalize_pack_and_unpack) {
        printf("pack + unpack");
    } else {
        printf("\n");
        ucs_error("unrecognized operation finalization function");
    }
}

void ucg_builtin_print_pack_cb_name(uct_pack_callback_t pack_single_cb)
{
    if (pack_single_cb == NULL) {
        printf("NONE");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_single_, 8)) {
        printf("atomic (8 bytes, single integer)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 8)) {
        printf("atomic (8 bytes, multiple integers)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_single_, 16)) {
        printf("atomic (16 bytes, single integer)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 16)) {
        printf("atomic (16 bytes, multiple integers)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_single_, 32)) {
        printf("atomic (32 bytes, single integer)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 32)) {
        printf("atomic (32 bytes, multiple integers)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_single_, 64)) {
        printf("atomic (64 bytes, single integer)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 64)) {
        printf("atomic (64 bytes, multiple integers)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_reducing_, single)) {
        printf("reduction callback");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_, single)) {
        printf("memory copy");
    }
}

/*
 * Note: Change-ID 8da6a5be2e changed the UCT API w.r.t. uct_pending_callback_t:
 *
 * After:  typedef void (*uct_completion_callback_t)(uct_completion_t *self);
 * Before: typedef void (*uct_completion_callback_t)(uct_completion_t *self,
 *                                                   ucs_status_t status);
 */
static void ucg_builtin_step_am_zcopy_comp_step_check_cb(uct_completion_t *self
#ifdef HAVE_UCT_COMP_CB_STATUS_ARG
                                                       , ucs_status_t status)
#else
                                                        )
#endif
{
    ucg_builtin_zcomp_t *zcomp  = ucs_container_of(self, ucg_builtin_zcomp_t, comp);
    ucg_builtin_request_t *req  = zcomp->req;
    ucg_builtin_op_step_t *step = req->step;
    zcomp->comp.count           = step->fragments_total;

    ucg_builtin_comp_step_cb(req);
}

ucs_status_t ucg_builtin_step_zcopy_prep(ucg_builtin_op_step_t *step)
{
    step->zcopy.zcomp.comp.count = step->fragments_total;
    step->zcopy.zcomp.comp.func  = ucg_builtin_step_am_zcopy_comp_step_check_cb;

    /* Register the buffer, creating a memory handle used in zero-copy sends */
    return uct_md_mem_reg(step->uct_md, step->send_buffer, step->buffer_length,
                          UCT_MD_MEM_ACCESS_ALL, &step->zcopy.memh);
}

static ucs_status_t ucg_builtin_optimize_am_bcopy_to_zcopy(ucg_builtin_op_t *op)
{
    /* This function was called because we want to "upgrade" a bcopy-send to
     * zcopy, by way of memory registration (costly, but hopefully worth it) */
    ucs_status_t status;
    ucg_builtin_op_step_t *step;
    ucg_step_idx_t step_idx = 0;
    do {
        step = &op->steps[step_idx++];
        if ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY) &&
            (step->phase->md_attr->cap.max_reg > step->buffer_length)) {
            status = ucg_builtin_step_zcopy_prep(step);
            if (status != UCS_OK) {
                goto bcopy_to_zcopy_cleanup;
            }

            step->flags   &= ~UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY;
            step->flags   |=  UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY;
            step->uct_send = step->uct_iface->ops.ep_am_zcopy;

            if (step->comp_criteria ==
                    UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES) {
                step->comp_criteria =
                    UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES_ZCOPY;
            }
        }
    } while (!(step->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP));

    return UCS_OK;

bcopy_to_zcopy_cleanup:
    while (step_idx--) {
        if (step->zcopy.memh) {
            uct_md_mem_dereg(step->uct_md, step->zcopy.memh);
        }
    }
    return status;
}

static ucs_status_t ucg_builtin_optimize_am_to_rma(ucg_builtin_op_t *op)
{
    // TODO: implement, especially important for "symmetric" collectives, where
    //       all ranks run a persistent collective and thus can re-use rkeys...
    return UCS_OK;
}

static ucs_status_t ucg_builtin_no_optimization(ucg_builtin_op_t *op)
{
    return UCS_OK;
}

/*
 * While some buffers are large enough to be registered (as in memory
 * registration) upon first send, others are "buffer-copied" (BCOPY) - unless
 * it is used repeatedly. If an operation is used this many times - its buffers
 * will also be registered, turning it into a zero-copy (ZCOPY) send henceforth.
 */
ucs_status_t ucg_builtin_op_consider_optimization(ucg_builtin_op_t *op,
                                                  ucg_builtin_config_t *config)
{
    ucg_builtin_op_step_t *step;
    ucg_step_idx_t step_idx = 0;
    do {
        step = &op->steps[step_idx++];
        if ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY) &&
            (step->phase->iface_attr->cap.flags & UCT_IFACE_FLAG_AM_ZCOPY) &&
            (step->phase->md_attr->cap.max_reg > step->buffer_length)) {
            op->optm_cb = ucg_builtin_optimize_am_bcopy_to_zcopy;
            op->opt_cnt = config->mem_reg_opt_cnt;
            return UCS_OK;
        }

        if ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY) &&
            (step->flags & UCG_GROUP_COLLECTIVE_MODIFIER_SYMMETRIC) &&
            (step->phase->md_attr->cap.max_reg > step->buffer_length)) {
            op->optm_cb = ucg_builtin_optimize_am_to_rma;
            op->opt_cnt = config->mem_rma_opt_cnt;
            return UCS_OK;
        }
    } while (!(step->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP));

    /* Note: This function will be called... after opt_cnt wrap-around */
    op->optm_cb = ucg_builtin_no_optimization;
    op->opt_cnt = 0;
    return UCS_OK;
}

static enum ucg_builtin_op_step_flags ucg_builtin_step_method_flags[] = {
    [UCG_PLAN_METHOD_SEND_TERMINAL]    = 0,
    [UCG_PLAN_METHOD_SEND_TO_SM_ROOT]  = 0,
    [UCG_PLAN_METHOD_RECV_TERMINAL]    = UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND,
    [UCG_PLAN_METHOD_BCAST_WAYPOINT]   = UCG_BUILTIN_OP_STEP_FLAG_RECV1_BEFORE_SEND,
    [UCG_PLAN_METHOD_GATHER_TERMINAL]  = UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND,
    [UCG_PLAN_METHOD_GATHER_WAYPOINT]  = UCG_BUILTIN_OP_STEP_FLAG_RECV_BEFORE_SEND1 |
                                         UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED,
    [UCG_PLAN_METHOD_SCATTER_TERMINAL] = UCG_BUILTIN_OP_STEP_FLAG_SEND_STRIDED,
    [UCG_PLAN_METHOD_SCATTER_WAYPOINT] = UCG_BUILTIN_OP_STEP_FLAG_RECV1_BEFORE_SEND |
                                         UCG_BUILTIN_OP_STEP_FLAG_SEND_STRIDED |
                                         UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED,
    [UCG_PLAN_METHOD_REDUCE_TERMINAL]  = UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND,
    [UCG_PLAN_METHOD_REDUCE_WAYPOINT]  = UCG_BUILTIN_OP_STEP_FLAG_RECV_BEFORE_SEND1 |
                                         UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED,
    [UCG_PLAN_METHOD_REDUCE_RECURSIVE] = UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND,
    [UCG_PLAN_METHOD_ALLTOALL_BRUCK]   = UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND,
    [UCG_PLAN_METHOD_ALLGATHER_BRUCK]  = UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND,
    [UCG_PLAN_METHOD_PAIRWISE]         = UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND |
                                         UCG_BUILTIN_OP_STEP_FLAG_SEND_STRIDED,
    [UCG_PLAN_METHOD_NEIGHBOR]         = UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND
};

static inline ucs_status_t
ucg_builtin_step_send_flags(ucg_builtin_op_step_t *step,
                            ucg_builtin_plan_phase_t *phase,
                            const ucg_collective_params_t *params,
#ifdef HAVE_UCT_COLLECTIVES
                            uct_coll_dtype_mode_t mode,
#endif
                            size_t dt_len, int is_dt_contig,
                            uint64_t *send_flag)
{
    size_t length      = step->buffer_length;
#ifndef HAVE_UCT_COLLECTIVES
    int supports_short = (phase->iface_attr->cap.flags & UCT_IFACE_FLAG_AM_SHORT);
    int supports_bcopy = (phase->iface_attr->cap.flags & UCT_IFACE_FLAG_AM_BCOPY);
    int supports_zcopy = (phase->iface_attr->cap.flags & UCT_IFACE_FLAG_AM_ZCOPY);
#else
    int supports_short = is_dt_contig &&
                         (phase->iface_attr->cap.flags & UCT_IFACE_FLAG_AM_SHORT) &&
                        ((phase->iface_attr->cap.coll_mode.short_flags & UCS_BIT(mode)) ||
                         (mode == UCT_COLL_DTYPE_MODE_PADDED));
    int supports_bcopy = (phase->iface_attr->cap.flags & UCT_IFACE_FLAG_AM_BCOPY) &&
                        ((phase->iface_attr->cap.coll_mode.bcopy_flags & UCS_BIT(mode)) ||
                         (mode == UCT_COLL_DTYPE_MODE_PADDED));
    int supports_zcopy = is_dt_contig &&
                         (phase->iface_attr->cap.flags & UCT_IFACE_FLAG_AM_ZCOPY) &&
                        ((phase->iface_attr->cap.coll_mode.zcopy_flags & UCS_BIT(mode)) ||
                         (mode == UCT_COLL_DTYPE_MODE_PADDED));

    ucs_assert((mode == UCT_COLL_DTYPE_MODE_PADDED) ||
               (phase->iface_attr->cap.am.coll_mode_flags & mode));
#endif

    /*
     * Short messages
     */
    if (ucs_likely(supports_short)) {
        size_t max_short = phase->iface_attr->cap.am.max_short - sizeof(ucg_builtin_header_t);
        ucs_assert(phase->iface_attr->cap.am.max_short > sizeof(ucg_builtin_header_t));
        if (ucs_likely(length <= max_short)) {
            /* Short send - single message */
            *send_flag            = UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT;
            step->uct_send        = step->uct_iface->ops.ep_am_short;
            step->fragments_total = phase->ep_cnt;
            return UCS_OK;
        }

#ifdef HAVE_UCT_COLLECTIVES
        size_t max_bcopy       = phase->iface_attr->cap.am.max_bcopy;
        size_t short_msg_count = length / max_short + ((length % max_short) != 0);
        size_t bcopy_msg_count = supports_bcopy ?
                (length / max_bcopy + ((length % max_bcopy) != 0)) : SIZE_MAX;
        int is_short_best = (short_msg_count * phase->iface_attr->overhead_short) <
                            (bcopy_msg_count * phase->iface_attr->overhead_bcopy);
#else
        int is_short_best = 1;
#endif

        if (is_short_best || (!supports_bcopy && !supports_zcopy)) {
            /* Short send - multiple messages */
            *send_flag            = (enum ucg_builtin_op_step_flags)
                                    (UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT |
                                     UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED);

            step->uct_send        = step->uct_iface->ops.ep_am_short;
            step->fragment_length = max_short - (max_short % dt_len);
            step->fragments_total = phase->ep_cnt *
                                    (length / step->fragment_length +
                                     ((length % step->fragment_length) > 0));
            return UCS_OK;
        }
    }

    /*
     * Large messages (zero-copy sends)
     */
    if (supports_zcopy) {
        size_t zcopy_threshold = 100000; // TODO: need to calculate the threshold!
        if (length > zcopy_threshold) {
            size_t max_zcopy = phase->iface_attr->cap.am.max_zcopy - sizeof(ucg_builtin_header_t);
            ucs_assert(phase->iface_attr->cap.am.max_zcopy > sizeof(ucg_builtin_header_t));
            if (ucs_likely(length <= max_zcopy)) {
                /* ZCopy send - single message */
                *send_flag            = UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY;
                step->uct_send        = step->uct_iface->ops.ep_am_zcopy;
                step->fragments_total = phase->ep_cnt;
            } else {
                /* ZCopy send - single message */
                *send_flag            = (enum ucg_builtin_op_step_flags)
                                        (UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY |
                                         UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED);
                step->uct_send        = step->uct_iface->ops.ep_am_zcopy;
                step->fragment_length = max_zcopy - (max_zcopy % dt_len);
                step->fragments_total = phase->ep_cnt *
                                        (length / step->fragment_length +
                                         ((length % step->fragment_length) > 0));
            }
            return UCS_OK;
        }
    }

    if (ucs_unlikely(!supports_bcopy)) {
        ucs_error("collective not supported by any transport type");
        return UCS_ERR_UNSUPPORTED;
    }

    /*
     * Medium messages (buffer-copy)
     */
    size_t max_bcopy = phase->iface_attr->cap.am.max_bcopy - sizeof(ucg_builtin_header_t);
    ucs_assert(phase->iface_attr->cap.am.max_bcopy > sizeof(ucg_builtin_header_t));
    if (ucs_likely(length <= max_bcopy)) {
        /* BCopy send - single message */
        *send_flag            = UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY;
        step->uct_send        = step->uct_iface->ops.ep_am_bcopy;
        step->fragment_length = step->buffer_length;
        step->fragments_total = phase->ep_cnt;
    } else {
        /* BCopy send - multiple messages */
        *send_flag            = (enum ucg_builtin_op_step_flags)
                                (UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY |
                                 UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED);
        step->uct_send        = step->uct_iface->ops.ep_am_bcopy;
        step->fragment_length = max_bcopy - (max_bcopy % dt_len);
        step->fragments_total = phase->ep_cnt *
                                (length / step->fragment_length +
                                 ((length % step->fragment_length) > 0));
    }

    return UCS_OK;
}

ucs_status_t
ucg_builtin_step_select_packers(const ucg_collective_params_t *params,
                                size_t send_dt_len, int is_send_dt_contig,
                                ucg_builtin_op_step_t *step)
{
    int is_signed;
    uint16_t modifiers = UCG_PARAM_TYPE(params).modifiers;
    int is_sm_reduce   = ((step->phase->method == UCG_PLAN_METHOD_SEND_TO_SM_ROOT) &&
                          (modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE));

    if ((is_sm_reduce) &&
        (ucg_global_params.field_mask & UCG_PARAM_FIELD_DATATYPE_CB) &&
        (ucg_global_params.datatype.is_integer_f(params->send.dtype, &is_signed)) &&
        (!is_signed) &&
        (ucg_global_params.field_mask & UCG_PARAM_FIELD_REDUCE_OP_CB) &&
        (ucg_global_params.reduce_op.is_sum_f != NULL) &&
        (ucg_global_params.reduce_op.is_sum_f(params->send.op))) {
        int is_single = (params->send.count == 1);
        // TODO: (un)set UCG_BUILTIN_OP_STEP_FLAG_BCOPY_PACK_LOCK...
        switch (send_dt_len) {
        case 1:
            step->bcopy.pack_single_cb = is_single ?
                    UCG_BUILTIN_PACKER_NAME(_atomic_single_, 8) :
                    UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 8);
            return UCS_OK;

        case 2:
            step->bcopy.pack_single_cb = is_single ?
                    UCG_BUILTIN_PACKER_NAME(_atomic_single_, 16) :
                    UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 16);
            return UCS_OK;

        case 4:
            step->bcopy.pack_single_cb = is_single ?
                    UCG_BUILTIN_PACKER_NAME(_atomic_single_, 32) :
                    UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 32);
            return UCS_OK;

        case 8:
            step->bcopy.pack_single_cb = is_single ?
                    UCG_BUILTIN_PACKER_NAME(_atomic_single_, 64) :
                    UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 64);
            return UCS_OK;

        default:
            ucs_error("unsupported unsigned integer datatype length: %lu",
                      send_dt_len);
            break; /* fall-back to the MPI reduction callback */
        }
    }

    int is_variadic   = (UCG_PARAM_TYPE(params).modifiers &
                         UCG_GROUP_COLLECTIVE_MODIFIER_VARIADIC);

    if (!is_send_dt_contig) {
        step->bcopy.pack_full_cb   = UCG_BUILTIN_PACKER_NAME(_datatype_, full);
        step->bcopy.pack_part_cb   = UCG_BUILTIN_PACKER_NAME(_datatype_, part);
        step->bcopy.pack_single_cb = UCG_BUILTIN_PACKER_NAME(_datatype_, single);
    } else if (is_variadic) {
        step->bcopy.pack_full_cb   = UCG_BUILTIN_PACKER_NAME(_variadic_, full);
        step->bcopy.pack_part_cb   = UCG_BUILTIN_PACKER_NAME(_variadic_, part);
        step->bcopy.pack_single_cb = UCG_BUILTIN_PACKER_NAME(_variadic_, single);
    } else if (is_sm_reduce) {
        step->bcopy.pack_full_cb   = UCG_BUILTIN_PACKER_NAME(_reducing_, full);
        step->bcopy.pack_part_cb   = UCG_BUILTIN_PACKER_NAME(_reducing_, part);
        step->bcopy.pack_single_cb = UCG_BUILTIN_PACKER_NAME(_reducing_, single);
    } else {
        step->bcopy.pack_full_cb   = UCG_BUILTIN_PACKER_NAME(_, full);
        step->bcopy.pack_part_cb   = UCG_BUILTIN_PACKER_NAME(_, part);
        step->bcopy.pack_single_cb = UCG_BUILTIN_PACKER_NAME(_, single);
    }

    return UCS_OK;
}

#define UCG_BUILTIN_STEP_RECV_FLAGS (UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND |\
                                     UCG_BUILTIN_OP_STEP_FLAG_RECV_BEFORE_SEND1|\
                                     UCG_BUILTIN_OP_STEP_FLAG_RECV1_BEFORE_SEND)

static inline int ucg_builtin_convert_datatype(void *param_datatype,
                                               ucp_datatype_t *ucp_datatype)
{
    if (ucs_unlikely(param_datatype == NULL)) {
        return ucp_dt_make_contig(1);
    }

    if (ucs_likely(ucg_global_params.field_mask & UCG_PARAM_FIELD_DATATYPE_CB)) {
        int ret = ucg_global_params.datatype.convert(param_datatype, ucp_datatype);
        if (ucs_unlikely(ret != 0)) {
            ucs_error("Datatype conversion callback failed");
            return UCS_ERR_INVALID_PARAM;
        }
    } else {
        *ucp_datatype = (ucp_datatype_t)param_datatype;
    }

    return UCS_OK;
}

ucs_status_t ucg_builtin_step_create(ucg_builtin_plan_t *plan,
                                     ucg_builtin_plan_phase_t *phase,
                                     enum ucg_builtin_op_step_flags *flags,
                                     const ucg_collective_params_t *params,
                                     int8_t **current_data_buffer,
                                     int is_send_dt_contig,
                                     int is_recv_dt_contig,
                                     size_t send_dt_len,
                                     ucg_builtin_op_init_cb_t *init_cb,
                                     ucg_builtin_op_fini_cb_t *fini_cb,
                                     ucg_builtin_op_step_t *step,
                                     int *zcopy_step_skip)
{
    ucs_status_t status;
    enum ucg_collective_modifiers modifiers = UCG_PARAM_TYPE(params).modifiers;

    /* Make sure local_id is always nonzero ( @ref ucg_builtin_header_step_t )*/
    ucs_assert_always(phase->step_index    != 0);
    ucs_assert_always(plan->super.group_id != 0);
    ucs_assert_always(phase->host_proc_cnt < (typeof(step->batch_cnt))-1);

    /* See note after ucg_builtin_step_send_flags() call */
    *zcopy_step_skip = 0;
zcopy_redo:

    /* Set the parameters determining the send-flags later on */
    step->phase                   = phase;
    step->ep_cnt                  = phase->ep_cnt;
    step->batch_cnt               = phase->host_proc_cnt - 1;
    step->am_header.group_id      = plan->super.group_id;
    step->am_header.msg.step_idx  = phase->step_index;
    step->am_header.remote_offset = 0;
    step->iter_ep                 = 0;
    step->iter_offset             = 0;
    step->fragment_pending        = NULL;
    step->buffer_length           = send_dt_len * params->send.count;
    step->recv_buffer             = (int8_t*)params->recv.buffer;
    step->uct_md                  = phase->md;
    step->flags                   = ucg_builtin_step_method_flags[phase->method];
    step->uct_iface               = (phase->ep_cnt == 1) ?
                                    phase->single_ep->iface :
                                    phase->multi_eps[0]->iface;
    step->uct_progress            = step->uct_iface->ops.iface_progress;
    /* Note: we assume all the UCT endpoints have the same interface */

    /* If the previous step involved receiving - plan accordingly  */
    if (*flags & UCG_BUILTIN_STEP_RECV_FLAGS) {
        step->send_buffer = *current_data_buffer ?
                            *current_data_buffer :
                            (int8_t*)params->send.buffer;
    } else {
        ucs_assert(*current_data_buffer == NULL);
        step->send_buffer = (params->send.buffer == ucg_global_params.mpi_in_place) ?
                            (int8_t*)params->recv.buffer :
                            (int8_t*)params->send.buffer;
    }

    uint64_t send_flags;
    int is_concat = modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_CONCATENATE;
#ifdef HAVE_UCT_COLLECTIVES
    uct_coll_dtype_mode_t mode;
    if (is_concat) {
        if (modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_VARIADIC) {
            mode = UCT_COLL_DTYPE_MODE_VAR_DTYPE;
        } else if (modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_VARIADIC) {
            mode = UCT_COLL_DTYPE_MODE_VAR_COUNT;
        } else {
            mode = UCT_COLL_DTYPE_MODE_PACKED;
        }
    } else {
        mode = UCT_COLL_DTYPE_MODE_PADDED;
    }

    /* Decide how the messages are sent (regardless of my role) */
    status = ucg_builtin_step_send_flags(step, phase, params, mode,
#else
    status = ucg_builtin_step_send_flags(step, phase, params,
#endif
                                         send_dt_len, is_send_dt_contig, &send_flags);
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }

    /*
     * Note: specifically for steps containing zero-copy communication - an
     *       additional step should precede to facilitate the zero-copy by
     *       exchanging the remote keys (both for shared memory and network).
     *       In order to avoid making "op->steps" a pointer and add dereference
     *       during trigger(), this hack here jumps one step forward (assuming
     *       op has allocated enough of them) and restarts step creation at
     *       this new step+1. The function will also indicate "upwards" to
     *       follow-up, by filling the missing step with remote key exchange.
     */
    if ((send_flags & UCT_IFACE_FLAG_AM_ZCOPY) && !(*zcopy_step_skip)) {
        *zcopy_step_skip = 1;
        step++;
        goto zcopy_redo;
    }

    step->comp_flags = 0;
#ifdef HAVE_UCT_COLLECTIVES
    if (phase->iface_attr->cap.flags & UCT_IFACE_FLAG_INCAST) {
        step->comp_flags |= UCG_BUILTIN_OP_STEP_COMP_FLAG_BATCHED_DATA;
    }
#endif

    int is_fragmented = (send_flags & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED);
    if (is_fragmented) {
        step->flags           |= UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED;
        step->comp_flags      |= UCG_BUILTIN_OP_STEP_COMP_FLAG_FRAGMENTED_DATA;
        step->fragment_pending = (uint8_t*)UCS_ALLOC_CHECK(sizeof(phase->ep_cnt),
                                                           "ucg_builtin_step_pipelining");
    }

    if ((step->buffer_length * plan->super.group_size) > (ucg_offset_t)-1) {
        step->comp_flags |= UCG_BUILTIN_OP_STEP_COMP_FLAG_LONG_BUFFERS;
    }

    /* Do any special assignment w.r.t. the src/dst buffers in this step */
    int is_send            = 0;
    int is_send_after_recv = 0;
    int is_pipelined       = 0;
    int is_reduction       = 0;
    int is_barrier         = modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_BARRIER;
    int is_broadcast       = modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST;
    int is_one_dest        = modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_DESTINATION;
    switch (phase->method) {
    case UCG_PLAN_METHOD_SEND_TERMINAL:
    case UCG_PLAN_METHOD_SEND_TO_SM_ROOT:
    case UCG_PLAN_METHOD_SCATTER_TERMINAL:
        is_send = 1;
        if (init_cb != NULL) {
            if (is_barrier) {
                ucs_assert(fini_cb != NULL);
                *init_cb = ucg_builtin_init_barrier;
                *fini_cb = ucg_builtin_finalize_barrier;
            } else if ((!is_broadcast) && (!is_one_dest)) {
                *init_cb = ucg_builtin_init_scatter;
            }
        }
        break;

    case UCG_PLAN_METHOD_REDUCE_TERMINAL:
        is_reduction = 1;
        if (init_cb != NULL) {
            if (is_barrier) {
                ucs_assert(fini_cb != NULL);
                *init_cb = ucg_builtin_init_barrier;
                *fini_cb = ucg_builtin_finalize_barrier;
            } else {
                *init_cb = ucg_builtin_init_reduce;
            }
        }
        /* no break */
    case UCG_PLAN_METHOD_GATHER_TERMINAL:
        if ((init_cb != NULL) && (is_concat)) {
            ucs_assert(*init_cb == NULL);
            *init_cb = ucg_builtin_init_gather_terminal;
        }
        /* no break */
    case UCG_PLAN_METHOD_RECV_TERMINAL:
        *current_data_buffer = (int8_t*)params->recv.buffer;
        break;

    case UCG_PLAN_METHOD_REDUCE_WAYPOINT:
        is_reduction = 1;
        if (init_cb != NULL) {
            if (is_barrier) {
                ucs_assert(fini_cb != NULL);
                *init_cb = ucg_builtin_init_barrier;
                *fini_cb = ucg_builtin_finalize_barrier;
            } else {
                *init_cb = ucg_builtin_init_reduce;
            }
        }
        /* no break */
    case UCG_PLAN_METHOD_GATHER_WAYPOINT:
        if ((init_cb != NULL) && (is_concat)) {
            ucs_assert(*init_cb == NULL);
            *init_cb = ucg_builtin_init_gather_waypoint;
        }
        /* no break */
    case UCG_PLAN_METHOD_SCATTER_WAYPOINT:
        if ((init_cb != NULL) && (*init_cb == NULL)) {
            *init_cb = ucg_builtin_init_scatter;
        }
        step->flags         |= UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED;
        step->send_buffer    =
        step->recv_buffer    =
        *current_data_buffer =
                (int8_t*)UCS_ALLOC_CHECK(step->buffer_length,
                                         "ucg_fanin_waypoint_buffer");
        // TODO: memory registration, and de-registration at some point...
        /* no break */
    case UCG_PLAN_METHOD_BCAST_WAYPOINT:
        /* for all *WAYPOINT types */
        is_send_after_recv = 1;
        is_send            = 1;

        if ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED) == 0) {
            step->send_buffer = step->recv_buffer;
        }

        if (is_fragmented) {
            step->flags |= UCG_BUILTIN_OP_STEP_FLAG_PIPELINED;
            is_pipelined = 1;
        }
        break;

    case UCG_PLAN_METHOD_REDUCE_RECURSIVE:
        /* First step is the exception to this rule */
        is_send = 1;
        is_reduction = 1;
        if (init_cb != NULL) {
            if (is_barrier) {
                ucs_assert(fini_cb != NULL);
                *init_cb = ucg_builtin_init_barrier;
                *fini_cb = ucg_builtin_finalize_barrier;
            } else {
                *init_cb = ucg_builtin_init_reduce;
            }
        }
        if (phase->step_index == 1) {
            break;
        }
        /* no break */
    case UCG_PLAN_METHOD_NEIGHBOR:
        is_send = 1;
        step->send_buffer = step->recv_buffer;
        break;

    case UCG_PLAN_METHOD_ALLTOALL_BRUCK:
        is_send = 1;
        if (init_cb != NULL) {
            ucs_assert(fini_cb != NULL);
            *init_cb = ucg_builtin_init_alltoall;
            *fini_cb = ucg_builtin_finalize_alltoall;
        }
        break;

    default:
        break;
    }

    if (is_reduction && !is_barrier) {
        if (!(ucg_global_params.field_mask & UCG_PARAM_FIELD_REDUCE_OP_CB)) {
            ucs_error("Cannot perform reductions: Missing ucg_init() parameters");
            return UCS_ERR_INVALID_PARAM;
        }

        if (!ucg_global_params.reduce_op.is_commutative_f(UCG_PARAM_OP(params))) {
            ucs_error("Cannot perform reduction: non-commutative operations unsupported");
            return UCS_ERR_UNSUPPORTED;
            // TODO: set UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE_STABLE instead
        }

        if (ucg_global_params.reduce_op.is_loc_expected_f(UCG_PARAM_OP(params))) {
            ucs_error("Cannot perform reductions: MPI's MINLOC/MAXLOC unsupported");
            return UCS_ERR_UNSUPPORTED;
        }
    }

    if (is_send) {
        /* packer callback selection */
        if (send_flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY) {
            status = ucg_builtin_step_select_packers(params, send_dt_len,
                                                     is_send_dt_contig, step);
            if (ucs_unlikely(status != UCS_OK)) {
                return status;
            }

            if (!is_send_dt_contig) {
                step->comp_flags |= UCG_BUILTIN_OP_STEP_COMP_FLAG_PACKED_DATATYPE;
            }
        }

#ifdef HAVE_UCT_COLLECTIVES
        if ((is_send) &&
            (phase->iface_attr->cap.flags & (UCT_IFACE_FLAG_INCAST |
                                             UCT_IFACE_FLAG_BCAST))) {
            send_flags |= UCG_BUILTIN_OP_STEP_FLAG_PACKED_DTYPE_MODE;
            if (send_flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT) {
                if (is_fragmented) {
                    step->fragment_length =
                            UCT_COLL_DTYPE_MODE_PACK(mode, step->fragment_length);
                } else {
                    step->buffer_length =
                            UCT_COLL_DTYPE_MODE_PACK(mode, step->buffer_length);
                }
                step->comp_flags |= UCG_BUILTIN_OP_STEP_COMP_FLAG_PACKED_LENGTH;
            } else if (modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_VARIADIC) {
                //step->zcopy.iov[1].buffer = (void*)params->send.displs;
            }
        }
#endif
    } else {
        if (!is_recv_dt_contig) {
            step->comp_flags |= UCG_BUILTIN_OP_STEP_COMP_FLAG_PACKED_DATATYPE;
        } else {
            step->buffer_length = params->recv.count * send_dt_len;
            // TODO: fix for cases where send and receive datatypes differ
        }
    }

    if (is_concat) {
#if ENABLE_DEBUG_DATA || ENABLE_FAULT_TOLERANCE
        /* Assume only one-level gathers, so the parent is #0 */
        ucs_assert(phase->indexes[phase->ep_cnt - 1] == 0);
        /* TODO: remove this restriction */
#endif
        /* Assume my peers have a higher rank/index for offset calculation */
        step->am_header.remote_offset = plan->super.my_index * step->buffer_length;
    }

    /* memory registration (using the memory registration cache) */
    int is_zcopy = (send_flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY);
    if (is_zcopy) {
        status = ucg_builtin_step_zcopy_prep(step);
        if (ucs_unlikely(status != UCS_OK)) {
            return status;
        }
    }

    int is_last = *flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP;
    if (is_last) {
        step->flags |= UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP;
    }

    if (is_barrier || !(step->flags & UCG_BUILTIN_STEP_RECV_FLAGS)) {
        step->comp_aggregation = UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_NOP;
    } else if ((send_flags & UCT_IFACE_FLAG_AM_ZCOPY) && (zcopy_step_skip)) {
        step->comp_aggregation = UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REMOTE_KEY;
    } else if ((phase->method == UCG_PLAN_METHOD_GATHER_TERMINAL) ||
               (phase->method == UCG_PLAN_METHOD_GATHER_WAYPOINT)) {
        step->comp_aggregation = UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_GATHER;
    } else if ((phase->method == UCG_PLAN_METHOD_REDUCE_TERMINAL) ||
               (phase->method == UCG_PLAN_METHOD_REDUCE_RECURSIVE)) {
        if (is_fragmented) {
            step->dtype_length = send_dt_len;
        }
        step->comp_aggregation = UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REDUCE;
        ucs_assert(params->recv.count > 0);
    } else {
        step->comp_aggregation = UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_WRITE;
    }

    /* Choose a completion criteria to be checked by the incoming AM handler */
    if (phase->ep_cnt == 1) {
        step->flags |= UCG_BUILTIN_OP_STEP_FLAG_SINGLE_ENDPOINT;
        if (is_fragmented) {
            if (is_pipelined) {
                step->comp_criteria =
                        UCG_BUILTIN_OP_STEP_COMP_CRITERIA_BY_FRAGMENT_OFFSET;
            } else {
                step->comp_criteria = is_zcopy ?
                        UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES_ZCOPY :
                        UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES;
            }
        } else {
            step->comp_criteria =
                    UCG_BUILTIN_OP_STEP_COMP_CRITERIA_SINGLE_MESSAGE;
        }
    } else {
        step->comp_criteria = is_zcopy ?
                UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES_ZCOPY :
                UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES;
    }

    if ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND) == 0) {
        step->comp_criteria = UCG_BUILTIN_OP_STEP_COMP_CRITERIA_SEND;
    }

    /* Choose the completion action to be taken by the incoming AM handler */
    step->comp_action = is_last ? UCG_BUILTIN_OP_STEP_COMP_OP :
                                  UCG_BUILTIN_OP_STEP_COMP_STEP;
    if (is_send) {
        step->flags |= send_flags;
        if (is_send_after_recv) {
            step->comp_action = UCG_BUILTIN_OP_STEP_COMP_SEND;
        }
    }

    *flags = step->flags; // TODO: handle case with UCT_COLL_TYPE_PACK/UNPACK
    return UCS_OK;
}

ucs_status_t ucg_builtin_step_create_rkey_bcast(ucg_builtin_plan_t *plan,
                                                const ucg_collective_params_t *params,
                                                ucg_builtin_op_step_t *step)
{
    ucg_collective_params_t step_params;

    ucg_builtin_op_step_t *zcopy_step = step + 1;
    ucg_group_member_index_t root     = UCG_PARAM_TYPE(params).root;
    unsigned is_bcast                 = zcopy_step->flags &
                                        UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST;

    ucs_assert((zcopy_step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_PUT_ZCOPY) ||
               (zcopy_step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_GET_ZCOPY));

    /* Prepare to send out my remote key */
    size_t rkey_size          = step->phase->md_attr->rkey_packed_size;
    size_t info_size          = sizeof(uint64_t) + rkey_size;
    size_t total_size         = is_bcast ? info_size :
                                           info_size * plan->super.group_size;

    uint8_t *info_buffer      = UCS_ALLOC_CHECK(total_size, "builtin_rkey_info");

    /* Set some parameters for step creation */
    memset(&step_params, 0, sizeof(step_params));
    UCG_PARAM_TYPE(&step_params).modifiers = is_bcast |
                                             UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_SOURCE;
    UCG_PARAM_TYPE(&step_params).root      = root;
    step_params.send.buffer                = info_buffer;
    step_params.send.count                 = 1;
    step_params.recv.buffer                = info_buffer;
    step_params.recv.count                 = 1;

    if (root == plan->super.my_index) {
        ucs_assert(step->send_buffer == step->recv_buffer); // TODO: choose the right buffer!
        *((uint64_t*)info_buffer) = (uint64_t)step->send_buffer;
        info_buffer              += sizeof(uint64_t);

        ucs_status_t status = uct_md_mkey_pack(step->uct_md,
                                               step->zcopy.memh,
                                               info_buffer);
        if (status != UCS_OK) {
            return status;
        }

        if (!is_bcast) {
            /* Copy the same memory key before scattering among the peers */
            int idx;
            uint8_t *info_iter = info_buffer;
            for (idx = 1; idx < plan->super.group_size; idx++) {
                info_iter += sizeof(uint64_t) + rkey_size;
                /*
                 * Note: we can't set the final pointers here just yet, because
                 *       the counters/displacements pointer may be given, but
                 *       may point to an array containing different values on
                 *       every call...
                 */
                memcpy(info_iter, info_buffer, rkey_size);
            }
        }
    }

    /* Create the preliminary remote-key-exchange step */
    int dummy_skip;
    enum ucg_builtin_op_step_flags flags = UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED;
    return ucg_builtin_step_create(plan, step->phase, &flags, &step_params,
                                   NULL, 1, 1, info_size, NULL, NULL, step,
                                   &dummy_skip);
}

static inline size_t ucg_builtin_op_get_send_dt_length(ucg_builtin_op_t *op)
{
    const ucg_collective_params_t *params = &op->super.params;

    if (params->send.count == 0) {
        return 0;
    }

    if (UCP_DT_IS_CONTIG(op->send_dt)) {
        return ucp_contig_dt_length(op->send_dt, 1);
    }

    ucg_builtin_init_state(op, 1);

    ucp_dt_generic_t *dt_gen = ucp_dt_to_generic(op->send_dt);

    size_t len = dt_gen->ops.packed_size(op->rstate.dt.generic.state);

    ucg_builtin_finalize_state(op, 1);

    return len;
}

ucs_status_t ucg_builtin_op_create(ucg_plan_t *plan,
                                   const ucg_collective_params_t *params,
                                   ucg_op_t **new_op)
{
    ucs_status_t status;
    ucg_builtin_plan_t *builtin_plan     = (ucg_builtin_plan_t*)plan;
    ucg_builtin_plan_phase_t *next_phase = &builtin_plan->phss[0];
    unsigned phase_count                 = builtin_plan->phs_cnt;
    ucg_builtin_op_t *op                 = (ucg_builtin_op_t*)
                                            ucs_mpool_get_inline(&builtin_plan->op_mp);
    ucg_builtin_op_step_t *next_step     = &op->steps[0];
    int is_send_dt_contig                = 1;
    int is_recv_dt_contig                = 1;
    int zcopy_step_skip                  = 0;
    int8_t *current_data_buffer          = NULL;
    op->init_cb                          = NULL;
    op->fini_cb                          = NULL;

    /* obtain UCX datatypes corresponding to the extenral datatypes passed */
    if (params->send.count > 0) {
        status = ucg_builtin_convert_datatype(params->send.dtype, &op->send_dt);
        if (ucs_unlikely(status != UCS_OK)) {
            return status;
        }

        is_send_dt_contig = UCP_DT_IS_CONTIG(op->send_dt);
    }

    if (params->recv.count > 0) {
        status = ucg_builtin_convert_datatype(params->recv.dtype, &op->recv_dt);
        if (ucs_unlikely(status != UCS_OK)) {
            return status;
        }

        is_recv_dt_contig = UCP_DT_IS_CONTIG(op->recv_dt);
    }

    /* copy the parameters aside, and use those from now on */
    memcpy(&op->super.params, params, sizeof(*params));
    params = &op->super.params;
    size_t send_dt_len = ucg_builtin_op_get_send_dt_length(op);
    /* Note: this needs to be after op->params and op->send_dt are set */

    /* Check for non-zero-root trees */
    if (ucs_unlikely(UCG_PARAM_TYPE(params).root != 0)) {
        /* Assume the plan is tree-based, since Recursive K-ing has no root */
        status = ucg_builtin_topo_tree_set_root(UCG_PARAM_TYPE(params).root,
                                                plan->my_index, builtin_plan,
                                                &next_phase, &phase_count);
        if (ucs_unlikely(status != UCS_OK)) {
            return status;
        }
    }

    /* Create a step in the op for each phase in the topology */
    enum ucg_builtin_op_step_flags flags = 0;
    if (phase_count == 1) {
        /* The only step in the plan */
        flags = UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP;
        status = ucg_builtin_step_create(builtin_plan, next_phase, &flags,
                                         params, &current_data_buffer,
                                         is_send_dt_contig, is_recv_dt_contig,
                                         send_dt_len, &op->init_cb, &op->fini_cb,
                                         next_step, &zcopy_step_skip);

        if ((status == UCS_OK) && ucs_unlikely(zcopy_step_skip)) {
            status = ucg_builtin_step_create_rkey_bcast(builtin_plan,
                                                        params,
                                                        next_step);
        }
    } else {
        /* First step of many */
        status = ucg_builtin_step_create(builtin_plan, next_phase, &flags,
                                         params, &current_data_buffer,
                                         is_send_dt_contig, is_recv_dt_contig,
                                         send_dt_len, &op->init_cb, &op->fini_cb,
                                         next_step, &zcopy_step_skip);
        if (ucs_unlikely(status != UCS_OK)) {
            goto op_cleanup;
        }

        if (ucs_unlikely(zcopy_step_skip)) {
            status = ucg_builtin_step_create_rkey_bcast(builtin_plan,
                                                        params,
                                                        next_step);
            if (ucs_unlikely(status != UCS_OK)) {
                goto op_cleanup;
            }

            zcopy_step_skip = 0;
            next_step++;
        }

        ucg_step_idx_t step_cnt;
        for (step_cnt = 1; step_cnt < phase_count - 1; step_cnt++) {
            status = ucg_builtin_step_create(builtin_plan, ++next_phase, &flags,
                                             params, &current_data_buffer,
                                             is_send_dt_contig, is_recv_dt_contig,
                                             send_dt_len, NULL, NULL, ++next_step,
                                             &zcopy_step_skip);
            if (ucs_unlikely(status != UCS_OK)) {
                goto op_cleanup;
            }

            if (ucs_unlikely(zcopy_step_skip)) {
                status = ucg_builtin_step_create_rkey_bcast(builtin_plan,
                                                            params,
                                                            next_step);
                if (ucs_unlikely(status != UCS_OK)) {
                    goto op_cleanup;
                }

                zcopy_step_skip = 0;
                next_step++;
            }
        }

        /* Last step gets a special flag */
        flags |= UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP;
        status = ucg_builtin_step_create(builtin_plan, ++next_phase, &flags,
                                         params, &current_data_buffer,
                                         is_send_dt_contig, is_recv_dt_contig,
                                         send_dt_len, NULL, NULL, ++next_step,
                                         &zcopy_step_skip);
        if ((status == UCS_OK) && ucs_unlikely(zcopy_step_skip)) {
            status = ucg_builtin_step_create_rkey_bcast(builtin_plan,
                                                        params,
                                                        next_step);
        }
    }
    if (ucs_unlikely(status != UCS_OK)) {
        goto op_cleanup;
    }

    /* Handle non-contiguous datatypes */
    // TODO: handle send-only or recieve-only collectives where the other
    //       datatype should be disregarded, contiguous or not.
    if (!is_send_dt_contig) {
        ucs_assert(op->init_cb == NULL);
        ucs_assert(op->fini_cb == NULL);

        if (!is_recv_dt_contig) {
            op->init_cb = ucg_builtin_init_pack_and_unpack;
            op->fini_cb = ucg_builtin_finalize_pack_and_unpack;
        } else {
            op->init_cb = ucg_builtin_init_pack;
            op->fini_cb = ucg_builtin_finalize_pack;
        }
    } else if (!is_recv_dt_contig) {
        ucs_assert(op->init_cb == NULL);
        ucs_assert(op->fini_cb == NULL);

        op->init_cb = ucg_builtin_init_unpack;
        op->fini_cb = ucg_builtin_finalize_unpack;
    }

    /* Select the right optimization callback */
    status = ucg_builtin_op_consider_optimization(op, builtin_plan->config);
    if (status != UCS_OK) {
        goto op_cleanup;
    }

    UCS_STATIC_ASSERT(sizeof(ucg_builtin_header_t) <= UCP_WORKER_HEADROOM_PRIV_SIZE);
    UCS_STATIC_ASSERT(sizeof(ucg_builtin_header_t) == sizeof(uint64_t));

    op->super.trigger_f = ucg_builtin_op_trigger;
    op->super.discard_f = ucg_builtin_op_discard;
    op->super.plan      = plan;
    op->gctx            = builtin_plan->gctx;
    *new_op             = &op->super;

    return UCS_OK;

op_cleanup:
    ucs_mpool_put_inline(op);
    return status;
}

void ucg_builtin_op_discard(ucg_op_t *op)
{
    ucg_builtin_op_t *builtin_op = (ucg_builtin_op_t*)op;
    ucg_builtin_op_step_t *step = &builtin_op->steps[0];
    do {
        if (step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY) {
            uct_md_mem_dereg(step->uct_md, step->zcopy.memh);
            uct_rkey_release(step->zcopy.cmpt, &step->zcopy.rkey);
        }

        if (step->flags & UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED) {
            ucs_free(step->recv_buffer);
        }

        if (step->flags & UCG_BUILTIN_OP_STEP_FLAG_PIPELINED) {
            ucs_free((void*)step->fragment_pending);
        }
    } while (!((step++)->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP));

    ucs_mpool_put_inline(op);
}

#define UCG_BUILTIN_OP_GET_SLOT_PTR(_gctx, _slot_id) \
    UCS_PTR_BYTE_OFFSET(_gctx, _slot_id * sizeof(ucg_builtin_comp_slot_t))

ucs_status_t ucg_builtin_op_trigger(ucg_op_t *op,
                                    ucg_coll_id_t coll_id,
                                    void *request)
{
    /* Allocate a "slot" for this operation, from a per-group array of slots */
    ucg_builtin_op_t *builtin_op  = (ucg_builtin_op_t*)op;
    unsigned slot_idx             = coll_id % UCG_BUILTIN_MAX_CONCURRENT_OPS;
    ucg_builtin_group_ctx_t *gctx = builtin_op->gctx;
    ucg_builtin_comp_slot_t *slot = UCG_BUILTIN_OP_GET_SLOT_PTR(gctx, slot_idx);

    if (ucs_unlikely(slot->req.expecting.local_id != 0)) {
        ucs_error("UCG Builtin planner exceeded its concurrent collectives limit.");
        return UCS_ERR_NO_RESOURCE;
    }

    /* Initialize the request structure, located inside the selected slot s*/
    ucg_builtin_request_t *builtin_req = &slot->req;
    builtin_req->op                    = builtin_op;
    ucg_builtin_op_step_t *first_step  = builtin_op->steps;
    builtin_req->step                  = first_step;
    builtin_req->pending               = first_step->fragments_total;
    ucg_builtin_header_t header        = first_step->am_header;
    builtin_req->comp_req              = request;
    builtin_op->current                = &builtin_req->step;

    /* Sanity checks */
    ucs_assert(first_step->am_header.msg.step_idx != 0);
    ucs_assert(first_step->iter_offset == 0);
    ucs_assert(first_step->iter_ep == 0);
    ucs_assert(request != NULL);

    /*
     * For some operations, like MPI_Reduce, MPI_Allreduce or MPI_Gather, the
     * local data has to be aggregated along with the incoming data. In others,
     * some shuffle is required once before starting (e.g. Bruck algorithms).
     */
    if (ucs_unlikely(builtin_op->init_cb != NULL)) {
        builtin_op->init_cb(builtin_op, coll_id);
    }

    /* Start the first step, which may actually complete the entire operation */
    header.msg.coll_id = coll_id;
    return ucg_builtin_step_execute(builtin_req, header);
}
