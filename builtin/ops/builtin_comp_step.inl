/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <ucp/core/ucp_request.inl> /* For @ref ucp_recv_desc_release */

void static UCS_F_ALWAYS_INLINE
ucg_builtin_comp_last_step_cb(ucg_builtin_request_t *req, ucs_status_t status)
{
    ucg_builtin_op_t *op = req->op;

    /* Finalize the collective operation (including user-defined callback) */
    if (ucs_unlikely(op->fini_cb != NULL)) {
        /*
         * Note: the "COMPLETED" flag cannot be set until UCG's internal
         *       callback finishes it's job. For example, barrier-release needs
         *       to happen before we give the all-clear to the user. This also
         *       means the function needs to run regardless of the status.
         */
        op->fini_cb(op);
    }

    /* Consider optimization, if this operation is used often enough */
    if (ucs_likely(status == UCS_OK) && ucs_unlikely(--op->opt_cnt == 0)) {
        status = op->optm_cb(op);
    }

    /* Mark (per-group) slot as available */
    req->expecting.local_id = 0;

    ucg_global_params.completion.coll_comp_cb_f(req->comp_req, status);
    ucs_assert(status != UCS_INPROGRESS);

    UCS_PROFILE_REQUEST_EVENT(req->comp_req, "complete_coll", 0);
    ucs_trace_req("collective returning completed request=%p (status: %s)",
                  req->comp_req, ucs_status_string(status));
}

ucs_status_t static UCS_F_ALWAYS_INLINE
ucg_builtin_comp_ft_end_step(ucg_builtin_op_step_t *step)
{
#if ENABLE_FAULT_TOLERANCE
    if (ucs_unlikely(step->flags & UCG_BUILTIN_OP_STEP_FLAG_FT_ONGOING))) {
        ucg_builtin_plan_phase_t *phase = step->phase;
        if (phase->ep_cnt == 1) {
            ucg_ft_end(phase->handles[0], phase->indexes[0]);
        } else {
            /* Removing in reverse order is good for FT's timerq performance */
            unsigned peer_idx = phase->ep_cnt;
            while (peer_idx--) {
                ucg_ft_end(phase->handles[peer_idx], phase->indexes[peer_idx]);
            }
        }
    }
#endif
    return UCS_OK;
}

ucs_status_t static UCS_F_ALWAYS_INLINE
ucg_builtin_comp_step_cb(ucg_builtin_request_t *req)
{
    /* Sanity checks */
    if (req->step->flags & UCG_BUILTIN_OP_STEP_FLAG_PIPELINED) {
        unsigned frag_idx;
        unsigned frag_per_ep = req->step->fragments_total / req->step->ep_cnt;
        ucs_assert(req->step->fragments_total % req->step->ep_cnt == 0);
        ucs_assert(req->step->fragment_pending != NULL);
        for (frag_idx = 0; frag_idx < frag_per_ep; frag_idx++) {
            ucs_assert(req->step->fragment_pending[frag_idx] == 0);
        }
    }

    /* Check if this is the last step */
    if (req->step->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP) {
        ucg_builtin_comp_last_step_cb(req, UCS_OK);
        ucg_builtin_comp_ft_end_step(req->step);
        return UCS_OK;
    }

    /* Start on the next step for this collective operation */
    ucg_builtin_op_step_t *prev_step  = req->step;
    ucg_builtin_op_step_t *next_step  = req->step = prev_step + 1;
    next_step->am_header.msg.coll_id  = prev_step->am_header.msg.coll_id;
    req->pending                      = next_step->fragments_total;

    ucg_builtin_comp_ft_end_step(next_step - 1);

    ucs_assert(next_step->iter_offset == 0);
    ucs_assert(next_step->iter_ep == 0);

    return ucg_builtin_step_execute(req);
}
static void UCS_F_ALWAYS_INLINE
ucg_builtin_mpi_reduce(void *mpi_op, void *src, void *dst,
                       int dcount, void* mpi_datatype)
{
    UCS_PROFILE_CALL_VOID(ucg_global_params.reduce_op.reduce_cb_f, mpi_op,
                          (char*)src, (char*)dst, (unsigned)dcount, mpi_datatype);
}

static void UCS_F_ALWAYS_INLINE
ucg_builtin_mpi_reduce_single(uint8_t *dst, uint8_t *src,
                              const ucg_collective_params_t *params)
{
    ucg_builtin_mpi_reduce(UCG_PARAM_OP(params), src, dst,
                           params->recv.count, params->recv.dtype);
}

static void UCS_F_ALWAYS_INLINE
ucg_builtin_mpi_reduce_fragment(uint8_t *dst, uint8_t *src,
                                size_t length, size_t dtype_length,
                                const ucg_collective_params_t *params)
{
    ucg_builtin_mpi_reduce(UCG_PARAM_OP(params), src, dst,
                           length / dtype_length,
                           params->recv.dtype);
}

static void UCS_F_ALWAYS_INLINE
ucg_builtin_comp_gather(uint8_t *recv_buffer, uint64_t offset, uint8_t *data,
                        size_t per_rank_length, size_t length,
                        ucg_group_member_index_t root)
{
    size_t my_offset = per_rank_length * root; // TODO: fix... likely broken
    if ((offset > my_offset) ||
        (offset + length <= my_offset)) {
        memcpy(recv_buffer + offset, data, length);
        return;
    }

    /* The write would overlap with my own contribution to gather */
    size_t first_part = offset + length - my_offset;
    recv_buffer += offset;
    memcpy(recv_buffer, data, first_part);
    data += first_part;
    memcpy(recv_buffer + first_part + length, data, length - first_part);
}

static ucs_status_t UCS_F_ALWAYS_INLINE
ucg_builtin_comp_unpack_rkey(ucg_builtin_op_step_t *step, uint64_t remote_addr,
                             uint8_t *packed_remote_key)
{
    /* Unpack the information from the payload to feed the next (0-copy) step */
    step->zcopy.raddr           = remote_addr;

    return uct_rkey_unpack(step->zcopy.cmpt, packed_remote_key, &step->zcopy.rkey);
}

static int UCS_F_ALWAYS_INLINE
ucg_builtin_comp_send_check_frag_by_offset(ucg_builtin_request_t *req,
                                           uint64_t offset, uint8_t batch_cnt)
{
    ucg_builtin_op_step_t *step = req->step;
    unsigned frag_idx = offset / step->fragment_length;
    ucs_assert(step->fragment_pending[frag_idx] >= batch_cnt);
    step->fragment_pending[frag_idx] -= batch_cnt;

    if (step->fragment_pending[frag_idx] == 0) {
        if (ucs_unlikely(step->iter_offset == UCG_BUILTIN_OFFSET_PIPELINE_PENDING)) {
            step->fragment_pending[frag_idx] = UCG_BUILTIN_FRAG_PENDING;
        } else {
            step->iter_offset = offset;
            return 1;
        }
    }

    return step->iter_offset != UCG_BUILTIN_OFFSET_PIPELINE_READY;
}

static void UCS_F_ALWAYS_INLINE
ucg_builtin_comp_unpack(ucg_builtin_op_t *op, uint64_t offset, void *data, size_t length)
{
    ucp_dt_generic_t *dt_gen = ucp_dt_to_generic(op->recv_dt);
    dt_gen->ops.unpack(op->wstate.dt.generic.state, offset, data,
                       length / op->super.params.recv.count);
}

static ucs_status_t UCS_F_ALWAYS_INLINE
ucg_builtin_step_recv_handle_chunk(enum ucg_builtin_op_step_comp_aggregation ag,
                                   uint8_t *dst, uint8_t *src, size_t length,
                                   size_t offset, int is_fragment,
                                   int is_dt_packed, ucg_builtin_request_t *req)
{
    ucs_status_t status;

    switch (ag) {
    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_NOP:
        status = UCS_OK;
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_GATHER:
        ucg_builtin_comp_gather(req->step->recv_buffer, offset, src,
                                req->step->buffer_length, length,
                                UCG_PARAM_TYPE(&req->op->super.params).root);
        status = UCS_OK;
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_WRITE:
        if (is_dt_packed) {
            ucg_builtin_comp_unpack(req->op, offset, src, length);
        } else {
            memcpy(dst, src, length);
        }
        status = UCS_OK;
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REDUCE:
        if (is_fragment) {
            ucg_builtin_mpi_reduce_fragment(dst, src, length,
                                            req->step->dtype_length,
                                            &req->op->super.params);
        } else {
            ucg_builtin_mpi_reduce_single(dst, src, &req->op->super.params);
        }
        status = UCS_OK;
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REMOTE_KEY:
        /* zero-copy prepares the key for the next step */
        ucs_assert(length == req->step->phase->md_attr->rkey_packed_size);
        status = ucg_builtin_comp_unpack_rkey(req->step + 1, offset, src);
        break;
    }

    return status;
}

#define case_recv_full(aggregation, _is_batched, _is_fragmented,               \
                       _is_len_packed, _is_dt_packed, _is_buf_long)            \
   case ((_is_batched    ? UCG_BUILTIN_OP_STEP_COMP_FLAG_BATCHED_DATA    : 0) |\
         (_is_fragmented ? UCG_BUILTIN_OP_STEP_COMP_FLAG_FRAGMENTED_DATA : 0) |\
         (_is_len_packed ? UCG_BUILTIN_OP_STEP_COMP_FLAG_PACKED_LENGTH   : 0) |\
         (_is_dt_packed  ? UCG_BUILTIN_OP_STEP_COMP_FLAG_PACKED_DATATYPE : 0) |\
         (_is_buf_long   ? UCG_BUILTIN_OP_STEP_COMP_FLAG_LONG_BUFFERS    : 0)):\
                                                                               \
        if (_is_batched) {                                                     \
            uint8_t index;                                                     \
            size_t chunk_size;                                                 \
            if (_is_fragmented) {                                              \
               if (_is_len_packed) {                                           \
                   chunk_size =                                                \
                       UCT_COLL_DTYPE_MODE_UNPACK_VALUE(step->fragment_length);\
               } else {                                                        \
                   chunk_size = step->fragment_length;                         \
               }                                                               \
            } else {                                                           \
               if (_is_len_packed) {                                           \
                   chunk_size =                                                \
                       UCT_COLL_DTYPE_MODE_UNPACK_VALUE(step->buffer_length);  \
               } else {                                                        \
                   chunk_size = step->buffer_length;                           \
               }                                                               \
            }                                                                  \
            if (_is_batched) {                                                 \
                offset *= chunk_size;                                          \
            }                                                                  \
            length += sizeof(ucg_builtin_header_t);                            \
            for (index = 0; index < step->batch_cnt; index++, data += length) {\
                status = ucg_builtin_step_recv_handle_chunk(aggregation,       \
                                                            dest_buffer, data, \
                                                            chunk_size, offset,\
                                                            _is_fragmented,    \
                                                            _is_dt_packed, req);\
                if (ucs_unlikely(status != UCS_OK)) {                          \
                    goto recv_handle_error;                                    \
                }                                                              \
            }                                                                  \
        } else {                                                               \
            if (_is_batched) {                                                 \
                offset *= step->buffer_length;                                 \
            }                                                                  \
            status = ucg_builtin_step_recv_handle_chunk(aggregation,           \
                                                        dest_buffer, data,     \
                                                        length, offset,        \
                                                        _is_fragmented,        \
                                                        _is_dt_packed, req);   \
            if (ucs_unlikely(status != UCS_OK)) {                              \
                goto recv_handle_error;                                        \
            }                                                                  \
        }                                                                      \
                                                                               \
        break;

#define case_recv_fragmented(a,    _is_fragmented, _is_len_packed, _is_dt_packed, _is_buf_long) \
              case_recv_full(a, 0, _is_fragmented, _is_len_packed, _is_dt_packed, _is_buf_long) \
              case_recv_full(a, 1, _is_fragmented, _is_len_packed, _is_dt_packed, _is_buf_long)

#define case_recv_len_packed(a,    _is_len_packed, _is_dt_packed, _is_buf_long) \
        case_recv_fragmented(a, 0, _is_len_packed, _is_dt_packed, _is_buf_long) \
        case_recv_fragmented(a, 1, _is_len_packed, _is_dt_packed, _is_buf_long)

#define  case_recv_dt_packed(a,    _is_dt_packed, _is_buf_long) \
        case_recv_len_packed(a, 0, _is_dt_packed, _is_buf_long) \
        case_recv_len_packed(a, 1, _is_dt_packed, _is_buf_long)

#define   case_recv_buf_long(a,    _is_buf_long) \
         case_recv_dt_packed(a, 0, _is_buf_long) \
         case_recv_dt_packed(a, 1, _is_buf_long)

#define case_recv(a) \
        case a: \
            switch ((uint8_t)step->comp_flags) { \
                case_recv_buf_long(a, 1) \
                case_recv_buf_long(a, 0) \
            } \
        break;

static void UCS_F_ALWAYS_INLINE
ucg_builtin_step_recv_handle_data(ucg_builtin_request_t *req, uint64_t offset,
                                  uint8_t *data, size_t length)
{
    ucs_status_t status;
    ucg_builtin_op_step_t *step = req->step;
    uint8_t *dest_buffer        = step->recv_buffer + offset;

    switch (step->comp_aggregation) {
        case_recv(UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_NOP)
        case_recv(UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_WRITE)
        case_recv(UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_GATHER)
        case_recv(UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REDUCE)
        case_recv(UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REMOTE_KEY)
    }

    return;

recv_handle_error:
    ucg_builtin_comp_last_step_cb(req, status);
}

#define UCG_IF_STILL_PENDING(req, num, dec) \
    ucs_assert(req->pending > (num)); \
    req->pending -= dec; \
    if (ucs_unlikely(req->pending != (num)))
    /* note: not really likely, but we optimize for the positive case */

static int UCS_F_ALWAYS_INLINE
ucg_builtin_step_recv_handle_comp(ucg_builtin_request_t *req, uint64_t offset,
                                  void *data, size_t length)
{
    ucg_builtin_op_step_t *step = req->step;

    /* Check according to the requested completion criteria */
    switch (step->comp_criteria) {
    case UCG_BUILTIN_OP_STEP_COMP_CRITERIA_SEND:
        return 0;

    case UCG_BUILTIN_OP_STEP_COMP_CRITERIA_SINGLE_MESSAGE:
        break;

    case UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES:
        UCG_IF_STILL_PENDING(req, 0, 1) {
            return 0;
        }
        break;

    case UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES_ZCOPY:
        UCG_IF_STILL_PENDING(req, step->fragments_total, 1) {
            return 0;
        }
        break;

    case UCG_BUILTIN_OP_STEP_COMP_CRITERIA_BY_FRAGMENT_OFFSET:
        if (!ucg_builtin_comp_send_check_frag_by_offset(req, offset, 1)) {
            return 0;
        }
        break;
    }


    /* Act according to the requested completion action */
    switch (step->comp_action) {
    case UCG_BUILTIN_OP_STEP_COMP_OP:
        ucg_builtin_comp_last_step_cb(req, UCS_OK);
        break;

    case UCG_BUILTIN_OP_STEP_COMP_STEP:
        (void) ucg_builtin_comp_step_cb(req);
        break;

    case UCG_BUILTIN_OP_STEP_COMP_SEND:
        (void) ucg_builtin_step_execute(req);
        break;
    }

    return 1;
}

static int UCS_F_ALWAYS_INLINE
ucg_builtin_step_recv_cb(ucg_builtin_request_t *req, uint64_t offset,
                         void *data, size_t length)
{
    ucg_builtin_step_recv_handle_data(req, offset, data, length);

    return ucg_builtin_step_recv_handle_comp(req, offset, data, length);
}

ucs_status_t static UCS_F_ALWAYS_INLINE
ucg_builtin_step_check_pending(ucg_builtin_comp_slot_t *slot)
{
    /* Check pending incoming messages - invoke the callback on each one */
    unsigned msg_index;
    ucp_recv_desc_t *rdesc;
    ucg_builtin_header_t *header;
    uint16_t local_id = slot->req.expecting.local_id;
    ucs_ptr_array_for_each(rdesc, msg_index, &slot->messages) {
        header = (ucg_builtin_header_t*)(rdesc + 1);
        ucs_assert((header->msg.coll_id  != slot->req.expecting.coll_id) ||
                   (header->msg.step_idx >= slot->req.expecting.step_idx));
        /*
         * Note: stored message coll_id can be either larger or smaller than
         * the one currently handled - due to coll_id wrap-around.
         */

        if (ucs_likely(header->msg.local_id == local_id)) {
            /* Remove the packet (next call may lead here recursively) */
            ucs_ptr_array_remove(&slot->messages, msg_index);

            /* Handle this "waiting" packet, possibly completing the step */
            int is_step_done = ucg_builtin_step_recv_cb(&slot->req,
                    header->remote_offset, header + 1,
                    rdesc->length - sizeof(ucg_builtin_header_t));

            /* Dispose of the packet, according to its allocation */
            ucp_recv_desc_release(rdesc
#ifdef HAVE_UCP_EXTENSIONS
#ifdef UCS_MEMUNITS_INF /* Backport to UCX v1.6.0 */
                    , slot->req.step->uct_iface
#endif
#endif
                    );

            /* If the step has indeed completed - check the entire operation */
            if (is_step_done) {
                int is_last_step = (slot->req.expecting.local_id == 0);
                return is_last_step ? UCS_OK : UCS_INPROGRESS;
            }
        }
    }

    return UCS_INPROGRESS;
}
