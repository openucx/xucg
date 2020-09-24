/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <string.h>
#include <ucs/arch/atomic.h>
#include <ucs/profile/profile.h>

#include "builtin_ops.h"
#include "builtin_comp_step.inl"

/******************************************************************************
 *                                                                            *
 *                            Operation Execution                             *
 *                                                                            *
 ******************************************************************************/

#define UCG_BUILTIN_ASSERT_SEND(step, send_type) \
    ucs_assert(((step)->flags & (UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT | \
                                 UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY | \
                                 UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY)) == \
                                 UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ ## send_type);

ucs_status_t static UCS_F_ALWAYS_INLINE
ucg_builtin_step_dummy_send(ucg_builtin_request_t *req,
        ucg_builtin_op_step_t *step, uct_ep_h ep, int is_pipelined)
{
    return UCS_OK;
}

static UCS_F_ALWAYS_INLINE ucs_status_t
ucg_builtin_step_am_short_one(ucg_builtin_request_t *req,
        ucg_builtin_op_step_t *step, uct_ep_h ep, int is_pipelined)
{
    UCG_BUILTIN_ASSERT_SEND(step, SHORT);

    return step->uct_iface->ops.ep_am_short(ep, step->am_id,
            step->am_header.header, step->send_buffer + step->iter_offset,
            step->buffer_length);
}

static UCS_F_ALWAYS_INLINE ucs_status_t
ucg_builtin_step_am_short_max(ucg_builtin_request_t *req,
        ucg_builtin_op_step_t *step, uct_ep_h ep, int is_pipelined)
{
    ucs_status_t status;
    unsigned am_id               = step->am_id;
    ucg_offset_t frag_size       = step->fragment_length;
    int is_packed                = ((step->flags &
                                     UCG_BUILTIN_OP_STEP_FLAG_PACKED_DTYPE_MODE)
                                    != 0);
    frag_size                    = ((frag_size) >>
                                    (UCT_COLL_DTYPE_MODE_BITS * is_packed));
    int8_t *sbuf                 = step->send_buffer;
    int8_t *buffer_iter          = sbuf + step->iter_offset;
    int8_t *buffer_iter_limit    = sbuf + step->buffer_length - frag_size;
    ucg_builtin_header_t am_iter = { .header = step->am_header.header };
    am_iter.remote_offset       += step->iter_offset;
    ucs_status_t (*ep_am_short)(uct_ep_h, uint8_t, uint64_t, const void*, unsigned) =
            step->uct_iface->ops.ep_am_short;

    UCG_BUILTIN_ASSERT_SEND(step, SHORT);
    ucs_assert(step->iter_offset != UCG_BUILTIN_OFFSET_PIPELINE_READY);
    ucs_assert(step->iter_offset != UCG_BUILTIN_OFFSET_PIPELINE_PENDING);
    ucs_assert(frag_size == (is_packed ?
               UCT_COLL_DTYPE_MODE_UNPACK_VALUE(step->fragment_length) :
               step->fragment_length));

    /* send every fragment but the last */
    if (ucs_likely(buffer_iter < buffer_iter_limit)) {
        do {
            status = ep_am_short(ep, am_id, am_iter.header, buffer_iter, frag_size);

            if (is_pipelined) {
                return status;
            }

            buffer_iter           += frag_size;
            am_iter.remote_offset += frag_size;
        } while ((status == UCS_OK) && (buffer_iter < buffer_iter_limit));

        /* send last fragment of the message */
        if (ucs_unlikely(status != UCS_OK)) {
            /* assuming UCS_ERR_NO_RESOURCE, restore the state for re-entry */
            step->iter_offset = buffer_iter - frag_size - sbuf;
            return status;
        }
    }

    status = ep_am_short(ep, am_id, am_iter.header, buffer_iter,
                         sbuf + step->buffer_length - buffer_iter);
    step->iter_offset = (status == UCS_OK) ? 0 : buffer_iter - sbuf;
    return status;
}

ucs_status_t static UCS_F_ALWAYS_INLINE
ucg_builtin_step_am_bcopy_one(ucg_builtin_request_t *req,
        ucg_builtin_op_step_t *step, uct_ep_h ep, int is_pipelined)
{
    UCG_BUILTIN_ASSERT_SEND(step, BCOPY);

    /* send active message to remote endpoint */
    ssize_t len = step->uct_iface->ops.ep_am_bcopy(ep, step->am_id,
            step->bcopy.pack_single_cb, req, step->uct_flags);
    return (ucs_unlikely(len < 0)) ? (ucs_status_t)len : UCS_OK;
}

ucs_status_t static UCS_F_ALWAYS_INLINE
ucg_builtin_step_am_bcopy_max(ucg_builtin_request_t *req,
        ucg_builtin_op_step_t *step, uct_ep_h ep, int is_pipelined)
{
    ssize_t len;
    unsigned am_id          = step->am_id;
    ucg_offset_t frag_size  = step->fragment_length;
    ucg_offset_t iter_limit = step->buffer_length - frag_size;
    packed_send_t send_func = step->uct_iface->ops.ep_am_bcopy;

    UCG_BUILTIN_ASSERT_SEND(step, BCOPY);
    ucs_assert(step->iter_offset != UCG_BUILTIN_OFFSET_PIPELINE_READY);
    ucs_assert(step->iter_offset != UCG_BUILTIN_OFFSET_PIPELINE_PENDING);

    /* check if this is not, by any chance, the last fragment */
    if (ucs_likely(step->iter_offset < iter_limit)) {
        /* send every fragment but the last */
        do {
            len = send_func(ep, am_id, step->bcopy.pack_full_cb, req, step->uct_flags);

            if (is_pipelined) {
                return ucs_unlikely(len < 0) ? (ucs_status_t)len : UCS_OK;
            }

            step->am_header.remote_offset += frag_size;
            step->iter_offset             += frag_size;
        } while ((len >= 0) && (step->iter_offset < iter_limit));

        if (ucs_unlikely(len < 0)) {
            step->am_header.remote_offset -= frag_size;
            step->iter_offset             -= frag_size;
            return (ucs_status_t)len;
        }
    }

    /* Send last fragment of the message */
    len = send_func(ep, am_id, step->bcopy.pack_part_cb, req, step->uct_flags);
    if (ucs_unlikely(len < 0)) {
        return (ucs_status_t)len;
    }

    step->am_header.remote_offset = 0;
    step->iter_offset = 0;
    return UCS_OK;
}

ucs_status_t static UCS_F_ALWAYS_INLINE
ucg_builtin_step_am_zcopy_one(ucg_builtin_request_t *req,
        ucg_builtin_op_step_t *step, uct_ep_h ep, int is_pipelined)
{
    uct_iov_t iov = {
            .buffer = step->send_buffer,
            .length = step->buffer_length,
            .memh   = step->zcopy.memh,
            .stride = 0,
            .count  = 1
    };

    UCG_BUILTIN_ASSERT_SEND(step, ZCOPY);
    ucg_builtin_zcomp_t *zcomp = &step->zcopy.zcomp[step->iter_ep];
    zcomp->req = req;

    ucs_status_t status = step->uct_iface->ops.ep_am_zcopy(ep, step->am_id,
           &step->am_header, sizeof(step->am_header), &iov, 1, 0, &zcomp->comp);
    return ucs_unlikely(status != UCS_INPROGRESS) ? status : UCS_OK;
}

ucs_status_t static UCS_F_ALWAYS_INLINE
ucg_builtin_step_am_zcopy_max(ucg_builtin_request_t *req,
        ucg_builtin_op_step_t *step, uct_ep_h ep, int is_pipelined)
{
    ucs_status_t status;
    unsigned am_id             = step->am_id;
    ucg_offset_t frag_size     = step->fragment_length;
    int8_t *sbuf               = step->send_buffer;
    void* iov_buffer_limit     = sbuf + step->buffer_length - frag_size;
    unsigned zcomp_index       = step->iter_ep * step->fragments +
                                 step->iter_offset / step->fragment_length;
    ucg_builtin_zcomp_t *zcomp = &step->zcopy.zcomp[zcomp_index];
    ucs_status_t (*ep_am_zcopy)(uct_ep_h, uint8_t, const void*, unsigned,
            const uct_iov_t*, size_t, unsigned, uct_completion_t*) =
                    step->uct_iface->ops.ep_am_zcopy;

    uct_iov_t iov = {
            .buffer = sbuf + step->iter_offset,
            .length = frag_size,
            .memh   = step->zcopy.memh,
            .stride = 0,
            .count  = 1
    };

    UCG_BUILTIN_ASSERT_SEND(step, ZCOPY);
    ucs_assert(step->iter_offset != UCG_BUILTIN_OFFSET_PIPELINE_READY);
    ucs_assert(step->iter_offset != UCG_BUILTIN_OFFSET_PIPELINE_PENDING);

    /* check if this is not, by any chance, the last fragment */
    if (ucs_likely(iov.buffer < iov_buffer_limit)) {
        /* send every fragment but the last */
        do {
            status = ep_am_zcopy(ep, am_id, &step->am_header,
                                 sizeof(step->am_header), &iov,
                                 1, 0, &zcomp->comp);
            (zcomp++)->req = req;

            if (is_pipelined) {
                return status;
            }

            step->am_header.remote_offset += frag_size;
            iov.buffer = (void*)((int8_t*)iov.buffer + frag_size);
        } while ((status == UCS_INPROGRESS) && (iov.buffer < iov_buffer_limit));

        if (ucs_unlikely(status != UCS_INPROGRESS)) {
            step->iter_offset = (int8_t*)iov.buffer - sbuf - frag_size;
            return status;
        }
    }

    /* Send last fragment of the message */
    zcomp->req = req;
    iov.length = sbuf + step->buffer_length - (int8_t*)iov.buffer;
    status     = ep_am_zcopy(ep, am_id, &step->am_header,
                             sizeof(step->am_header),
                             &iov, 1, 0, &zcomp->comp);
    if (ucs_unlikely(status != UCS_INPROGRESS)) {
        step->iter_offset = (int8_t*)iov.buffer - sbuf;
        return status;
    }

    step->am_header.remote_offset = 0;
    step->iter_offset = 0;
    return UCS_OK;
}

/*
 * Below is a set of macros, generating most bit-field combinations of
 * step->flags in the switch-case inside @ref ucg_builtin_step_execute() .
 */

#define case_send_full(req, ureq, step, phase, _is_last, _is_1ep, _is_strided, \
                       _is_pipelined, _is_recv, _is_rs1, _is_r1s, _send_flag,  \
                       _send_func)                                             \
   case ((_is_last      ? UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP          : 0) |   \
         (_is_1ep       ? UCG_BUILTIN_OP_STEP_FLAG_SINGLE_ENDPOINT    : 0) |   \
         (_is_strided   ? UCG_BUILTIN_OP_STEP_FLAG_SEND_STRIDED       : 0) |   \
         (_is_pipelined ? UCG_BUILTIN_OP_STEP_FLAG_PIPELINED          : 0) |   \
         (_is_recv      ? UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND    : 0) |   \
         (_is_rs1       ? UCG_BUILTIN_OP_STEP_FLAG_RECV_BEFORE_SEND1  : 0) |   \
         (_is_r1s       ? UCG_BUILTIN_OP_STEP_FLAG_RECV1_BEFORE_SEND  : 0) |   \
         _send_flag):                                                          \
                                                                               \
        is_zcopy = (_send_flag) & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY;      \
        if ((_is_rs1 || _is_r1s) && (step->iter_ep == 0)) {                    \
            uint32_t new_cnt = step->iter_ep = _is_r1s ? 1 : phase->ep_cnt - 1;\
            if (_is_pipelined) {                                               \
               memset((void*)step->fragment_pending, new_cnt, step->fragments);\
            }                                                                  \
            if (!is_zcopy) {                                                   \
                req->pending = new_cnt * step->fragments;                      \
            } /* Otherwise default init of ep_cnt*num_fragments is correct */  \
            break; /* Beyond the switch-case we fall-back to receiving */      \
        }                                                                      \
                                                                               \
        if (_is_recv && is_zcopy) {                                            \
            /* Both zcopy callbacks and incoming messages use pending, so ...*/\
            req->pending = 2 * step->fragments * phase->ep_cnt;                \
        }                                                                      \
                                                                               \
        /* Perform one or many send operations, unless an error occurs */      \
        if (_is_1ep) {                                                         \
            status = _send_func (req, step, phase->single_ep, 0);              \
            if (ucs_unlikely(UCS_STATUS_IS_ERR(status))) {                     \
                goto step_execute_error;                                       \
            }                                                                  \
        } else {                                                               \
            if ((_is_pipelined) && (ucs_unlikely(step->iter_offset ==          \
                    UCG_BUILTIN_OFFSET_PIPELINE_PENDING))) {                   \
                /* find a pending offset to progress */                        \
                unsigned frag_idx = 0;                                         \
                while ((frag_idx < step->fragments) &&                         \
                       (step->fragment_pending[frag_idx] ==                    \
                               UCG_BUILTIN_FRAG_PENDING)) {                    \
                    frag_idx++;                                                \
                }                                                              \
                ucs_assert(frag_idx < step->fragments);                        \
                step->iter_offset = frag_idx * step->fragment_length;          \
            }                                                                  \
                                                                               \
            uct_ep_h *ep_iter, *ep_last;                                       \
            ep_iter = ep_last = phase->multi_eps;                              \
            ep_iter += step->iter_ep;                                          \
            ep_last += phase->ep_cnt;                                          \
            if (_is_strided) {                                                 \
                item_interval = step->buffer_length;                           \
            }                                                                  \
                                                                               \
            do {                                                               \
                status = _send_func (req, step, *ep_iter, _is_pipelined);      \
                if (ucs_unlikely(UCS_STATUS_IS_ERR(status))) {                 \
                    /* Store the pointer, e.g. for UCS_ERR_NO_RESOURCE */      \
                    step->iter_ep = ep_iter - phase->multi_eps;                \
                    goto step_execute_error;                                   \
                }                                                              \
                                                                               \
                if (_is_strided) {                                             \
                    step->iter_offset += item_interval;                        \
                }                                                              \
            } while (++ep_iter < ep_last);                                     \
                                                                               \
            if (_is_pipelined) {                                               \
                /* Reset the iterator for the next pipelined incoming packet */\
                step->iter_ep = _is_r1s ? 1 : phase->ep_cnt - 1;               \
                ucs_assert(_is_r1s + _is_rs1 > 0);                             \
                                                                               \
                /* Check if this invocation is a result of a resend attempt */ \
                unsigned idx = step->iter_offset / step->fragment_length;      \
                if (ucs_unlikely(step->fragment_pending[idx] ==                \
                        UCG_BUILTIN_FRAG_PENDING)) {                           \
                    step->fragment_pending[idx] = 0;                           \
                                                                               \
                    /* Look for other packets in need of resending */          \
                    for (idx = 0; idx < step->fragments; idx++) {              \
                        if (step->fragment_pending[idx] ==                     \
                                UCG_BUILTIN_FRAG_PENDING) {                    \
                            /* Found such packets - mark for next resend */    \
                            step->iter_offset = idx * step->fragment_length;   \
                            status            = UCS_ERR_NO_RESOURCE;           \
                            goto step_execute_error;                           \
                        }                                                      \
                    }                                                          \
                } else {                                                       \
                    ucs_assert(step->fragment_pending[idx] == 0);              \
                }                                                              \
                step->iter_offset = UCG_BUILTIN_OFFSET_PIPELINE_READY;         \
            } else {                                                           \
                step->iter_ep = 0; /* Reset the per-step endpoint iterator */  \
                if (_is_strided) {                                             \
                    step->iter_offset = 0;                                     \
                } else {                                                       \
                    ucs_assert(step->iter_offset == 0);                        \
                }                                                              \
            }                                                                  \
        }                                                                      \
                                                                               \
        /* Potential completions (the operation may have finished by now) */   \
        if ((!_is_recv && !is_zcopy) || (req->pending == 0)) {                 \
            /* Nothing else to do - complete this step */                      \
            if (_is_last) {                                                    \
                if (!ureq) {                                                   \
                    ucg_builtin_comp_last_step_cb(req, UCS_OK);                \
                }                                                              \
                return UCS_OK;                                                 \
            } else {                                                           \
                slot->req.latest.coll_id = step->am_header.msg.coll_id;        \
                return ucg_builtin_comp_step_cb(req, ureq);                    \
            }                                                                  \
        }                                                                      \
        break;

#define  case_send_1ep(r, u, s, p,    _is_1ep, _is_strided, _is_pipelined, _is_recv, _is_rs1, _is_r1s, _send_flag, _send_func) \
        case_send_full(r, u, s, p, 0, _is_1ep, _is_strided, _is_pipelined, _is_recv, _is_rs1, _is_r1s, _send_flag, _send_func) \
        case_send_full(r, u, s, p, 1, _is_1ep, _is_strided, _is_pipelined, _is_recv, _is_rs1, _is_r1s, _send_flag, _send_func)

#define case_send_strided(r, u, s, p,    _is_strided, _is_pipelined, _is_recv, _is_rs1, _is_r1s, _send_flag, _send_func) \
            case_send_1ep(r, u, s, p, 0, _is_strided, _is_pipelined, _is_recv, _is_rs1, _is_r1s, _send_flag, _send_func) \
            case_send_1ep(r, u, s, p, 1, _is_strided, _is_pipelined, _is_recv, _is_rs1, _is_r1s, _send_flag, _send_func)

#define case_send_pipelined(r, u, s, p,    _is_pipelined, _is_recv, _is_rs1, _is_r1s, _send_flag, _send_func) \
          case_send_strided(r, u, s, p, 0, _is_pipelined, _is_recv, _is_rs1, _is_r1s, _send_flag, _send_func) \
          case_send_strided(r, u, s, p, 1, _is_pipelined, _is_recv, _is_rs1, _is_r1s, _send_flag, _send_func)

#define    case_send_method(r, u, s, p,    _is_recv, _is_rs1, _is_r1s, _send_flag, _send_func) \
        case_send_pipelined(r, u, s, p, 0, _is_recv, _is_rs1, _is_r1s, _send_flag, _send_func) \
        case_send_pipelined(r, u, s, p, 1, _is_recv, _is_rs1, _is_r1s, _send_flag, _send_func)

#define        case_send(r, u, s, p,          _send_flag, _send_func) \
        case_send_method(r, u, s, p, 0, 0, 0, _send_flag, _send_func) \
        case_send_method(r, u, s, p, 1, 0, 0, _send_flag, _send_func) \
        case_send_method(r, u, s, p, 0, 1, 0, _send_flag, _send_func) \
        case_send_method(r, u, s, p, 0, 0, 1, _send_flag, _send_func)

/*
 * Executing a single step is the heart of the Builtin planner.
 * This function advances to the next step (some invocations negate that...),
 * sends and then recieves according to the instructions of this step.
 * The function returns the status, typically one of the following:
 * > UCS_OK - collective operation (not just this step) has been completed.
 * > UCS_INPROGRESS - sends complete, waiting on some messages to be recieved.
 * > otherwise - an error has occurred.
 *
 * For example, a "complex" case is when the message is fragmented, and requires
 * both recieveing and sending in a single step, like in REDUCE_WAYPOINT. The
 * first call, coming from @ref ucg_builtin_op_trigger() , will enter the first
 * branch ("step_ep" is zero when a new step is starting), will process some
 * potential incoming messages (arriving beforehand) - returning UCS_INPROGRESS.
 * Subsequent calls to "progress()" will handle the rest of the incoming
 * messages for this step, and eventually call this function again from within
 * @ref ucg_builtin_comp_step_cb() . This call will choose the second branch,
 * the swith-case, which will send the message and
 */
UCS_PROFILE_FUNC(ucs_status_t, ucg_builtin_step_execute, (req, user_req),
                 ucg_builtin_request_t *req, ucg_request_t *user_req)
{
    int is_zcopy;
    ucs_status_t status;
    size_t item_interval;

    ucg_builtin_op_step_t *step     = req->step;
    ucg_builtin_plan_phase_t *phase = step->phase;
    ucg_builtin_comp_slot_t *slot   = ucs_container_of(req, ucg_builtin_comp_slot_t, req);

    /* This step either starts by sending or contains no send operations */
    switch (step->flags & UCG_BUILTIN_OP_STEP_FLAG_SWITCH_MASK) {
    /* Single-send operations (only one fragment passed to UCT) */
    case_send(req, user_req, step, phase, 0, /* for recv-only steps */
              ucg_builtin_step_dummy_send)
    case_send(req, user_req, step, phase, UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT,
              ucg_builtin_step_am_short_one)
    case_send(req, user_req, step, phase, UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY,
              ucg_builtin_step_am_bcopy_one)
    case_send(req, user_req, step, phase, UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY,
              ucg_builtin_step_am_zcopy_one)

    /* Multi-send operations (using iter_ep and iter_offset for context) */
    case_send(req, user_req, step, phase, UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED,
              ucg_builtin_step_dummy_send)
    case_send(req, user_req, step, phase, UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED |
                                          UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT,
              ucg_builtin_step_am_short_max)
    case_send(req, user_req, step, phase, UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED |
                                          UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY,
              ucg_builtin_step_am_bcopy_max)
    case_send(req, user_req, step, phase, UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED |
                                          UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY,
              ucg_builtin_step_am_zcopy_max)

    default:
        ucs_error("Invalid method for a collective operation step.");
        ucg_builtin_print_flags(step);
        status = UCS_ERR_INVALID_PARAM;
        goto step_execute_error;
    }

    /* Initialize the users' request object, if applicable */
    req->comp_req             = user_req;
    slot->req.latest.local_id = step->am_header.msg.local_id;
    ucs_assert(slot->req.latest.local_id != 0);
    return ucg_builtin_step_check_pending(slot);

    /************************** Error flows ***********************************/
step_execute_error:
    req->comp_req = user_req;
    if (status == UCS_ERR_NO_RESOURCE) {
        /* Special case: send incomplete - enqueue for resend upon progress */
        if (step->flags & UCG_BUILTIN_OP_STEP_FLAG_PIPELINED) {
            step->fragment_pending[step->iter_offset / step->fragment_length] =
                    UCG_BUILTIN_FRAG_PENDING;
            step->iter_offset = UCG_BUILTIN_OFFSET_PIPELINE_PENDING;
        }

        /* Set this slot as "pending a resend" */
        ucs_atomic_or64(phase->resends, UCS_BIT(slot->req.latest.coll_id %
                                                UCG_BUILTIN_MAX_CONCURRENT_OPS));
        return UCS_INPROGRESS;
    }

    /* Generic error - reset the collective and mark the request as completed */
    ucg_builtin_comp_last_step_cb(req, status);
    return status;
}
