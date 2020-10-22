/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <ucp/core/ucp_request.inl> /* For @ref ucp_recv_desc_release */

void static UCS_F_ALWAYS_INLINE
ucg_builtin_comp_last_step_cb(ucg_builtin_request_t *req, ucs_status_t status)
{
    /* Sanity checks */
    ucg_request_t *user_req = req->comp_req;
    ucs_assert(user_req != NULL);

    /* Mark (per-group) slot as available */
    ucs_container_of(req, ucg_builtin_comp_slot_t, req)->req.latest.local_id = 0;

    /* Store request return value, in case the user-defined callback checks */
    user_req->status = status;

    /* Finalize the collective operation (including user-defined callback) */
    ucg_builtin_op_t *op = req->op;
    if (ucs_unlikely(op->fini_cb != NULL)) {
        /*
         * Note: the "COMPLETED" flag cannot be set until UCG's internal
         *       callback finishes it's job. For example, barrier-release needs
         *       to happen before we give the all-clear to the user.
         */
        op->fini_cb(op, user_req, status);
        // TODO: always call fini_cb (possibly dummy), where MPI will pass a
        //       completion-callback to finish MPI_Request structures directly.

        /* Consider optimization, if this operation is used often enough */
        if (ucs_likely(status == UCS_OK) && ucs_unlikely(--op->opt_cnt == 0)) {
            user_req->status = op->optm_cb(op);
        }
        // TODO: make sure optimizations are used - by setting fini_cb...
    }

    /* Mark this request as complete */
    user_req->flags = UCP_REQUEST_FLAG_COMPLETED;

    UCS_PROFILE_REQUEST_EVENT(user_req, "complete_coll", 0);
    ucs_trace_req("collective returning completed request=%p (status: %s)",
                  user_req, ucs_status_string(status));
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
    ucg_builtin_op_step_t *next_step  = ++req->step;
    req->pending                      = next_step->fragments_total;
    req->latest.step_idx              = next_step->am_header.msg.step_idx;
    next_step->am_header.msg.coll_id  = req->latest.coll_id;

    ucs_status_t status = ucg_builtin_step_execute(req);
    ucg_builtin_comp_ft_end_step(next_step - 1);
    return status;
}
static void UCS_F_ALWAYS_INLINE
ucg_builtin_mpi_reduce(void *mpi_op, void *src, void *dst,
                       int dcount, void* mpi_datatype)
{
    UCS_PROFILE_CALL_VOID(ucg_global_params.reduce_cb_f, mpi_op, (char*)src,
                          (char*)dst, (unsigned)dcount, mpi_datatype);
}

static void UCS_F_ALWAYS_INLINE
ucg_builtin_mpi_reduce_single(uint8_t *dst, uint8_t *src, size_t length,
                              ucg_collective_params_t *params)
{
    ucs_assert((params->recv.dt_len * params->recv.count) == length);
    ucg_builtin_mpi_reduce(params->recv.op_ext, src, dst,
                           params->recv.count,
                           params->recv.dt_ext);
}

static void UCS_F_ALWAYS_INLINE
ucg_builtin_mpi_reduce_fragment(uint8_t *dst, uint8_t *src, size_t length,
                                ucg_collective_params_t *params)
{
    ucs_assert((length % params->recv.dt_len) == 0);
    ucg_builtin_mpi_reduce(params->recv.op_ext, src, dst,
                           length / params->recv.dt_len,
                           params->recv.dt_ext);
}

static void UCS_F_ALWAYS_INLINE
ucg_builtin_comp_reduce_batched(ucg_builtin_request_t *req, uint64_t offset,
                                uint8_t *src, size_t size, size_t stride)
{
    uint8_t *dst = req->step->recv_buffer + offset;
    unsigned index, count = req->step->batch_cnt;
    for (index = 0; index < count; index++, src += stride) {
        ucg_builtin_mpi_reduce_single(dst, src, size, &req->op->super.params);
    }
}

static void ucg_builtin_comp_gather(uint8_t *recv_buffer, uint8_t *data,
                                    uint64_t offset, size_t size, size_t length,
                                    ucg_group_member_index_t root)
{
    size_t my_offset = size * root;
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

static UCS_F_ALWAYS_INLINE ucs_status_t
ucg_builtin_comp_unpack_rkey(ucg_builtin_request_t *req, uint64_t remote_addr,
                             uint8_t *packed_remote_key)
{
    /* Unpack the information from the payload to feed the next (0-copy) step */
    ucg_builtin_op_step_t *step = req->step + 1;
    step->zcopy.raddr           = remote_addr;

    ucs_status_t status = uct_rkey_unpack(step->zcopy.cmpt,
                                          packed_remote_key,
                                          &step->zcopy.rkey);
    if (ucs_unlikely(status != UCS_OK)) {
        ucg_builtin_comp_last_step_cb(req, status);
    }

    return status;
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
ucg_builtin_step_recv_handle_data(ucg_builtin_request_t *req, uint64_t offset,
                                  void *data, size_t length)
{
    size_t size;
    ucg_builtin_op_step_t *step = req->step;

    ucs_assert((length != 0) || (step->comp_aggregation ==
                                 UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_NOP));

    /* Act according to the requested data action */
    switch (step->comp_aggregation) {
    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_NOP:
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_WRITE:
        memcpy(step->recv_buffer + offset, data, length);
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_WRITE_UNPACKED:
        /* Note: worker is NULL since it isn't required for host-based memory */
        ucp_dt_unpack_only(NULL, step->recv_buffer + offset, step->unpack_count,
                           step->bcopy.datatype, UCS_MEMORY_TYPE_HOST, data, length, 0);
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_GATHER_TERMINAL:
        ucg_builtin_comp_gather(step->recv_buffer, data, offset,
                                step->buffer_length, length,
                                req->op->super.params.type.root);
        break;
    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_GATHER_WAYPOINT:
        ucs_assert(offset == 0);
        memcpy(step->recv_buffer + step->buffer_length, data, length);
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REDUCE_SINGLE:
        ucg_builtin_mpi_reduce_single(step->recv_buffer + offset, data,
                                      length, &req->op->super.params);
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REDUCE_BATCHED:
        size = step->buffer_length;
        ucg_builtin_comp_reduce_batched(req, offset, data, size,
                                        length + sizeof(ucg_builtin_header_t));
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REDUCE_FRAGMENT:
        ucg_builtin_mpi_reduce_fragment(step->recv_buffer + offset, data,
                                        length, &req->op->super.params);
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REMOTE_KEY:
        ucs_assert(length == step->phase->md_attr->rkey_packed_size);
        ucg_builtin_comp_unpack_rkey(req, offset, data);
        break;
    }
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
    uint16_t local_id = slot->req.latest.local_id;
    ucs_ptr_array_for_each(rdesc, msg_index, &slot->messages) {
        header = (ucg_builtin_header_t*)(rdesc + 1);
        ucs_assert((header->msg.coll_id  != slot->req.latest.coll_id) ||
                   (header->msg.step_idx >= slot->req.latest.step_idx));
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

            /* If the step has indeed completed - check the entire op */
            if (is_step_done) {
                return (slot->req.comp_req->flags == UCP_REQUEST_FLAG_COMPLETED)?
                        slot->req.comp_req->status : UCS_INPROGRESS;
            }
        }
    }

    return UCS_INPROGRESS;
}
