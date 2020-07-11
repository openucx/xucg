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
    ucs_assert((user_req->flags == 0) || (user_req->status != UCS_OK));

    /* Mark (per-group) slot as available */
    ucs_container_of(req, ucg_builtin_comp_slot_t, req)->req.latest.local_id = 0;

    /* Store request return value, in case the user-defined callback checks */
    user_req->status = status;

    /* Finalize the collective operation (including user-defined callback) */
    if (ucs_unlikely(req->op->fini_cb != NULL)) {
        /*
         * Note: the "COMPLETED" flag cannot be set until UCG's internal
         *       callback finishes it's job. For example, barrier-release needs
         *       to happen before we give the all-clear to the user.
         */
        req->op->fini_cb(req->op, user_req, status);
        // TODO: always call fini_cb (possibly dummy), where MPI will pass a
        //       completion-callback to finish MPI_Request structures directly.
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
ucg_builtin_comp_step_cb(ucg_builtin_request_t *req, ucg_request_t *user_req)
{
    /* Sanity checks */
    if (req->step->flags & UCG_BUILTIN_OP_STEP_FLAG_PIPELINED) {
        unsigned frag_idx;
        ucs_assert(req->step->fragment_pending != NULL);
        for (frag_idx = 0; frag_idx < req->step->fragments; frag_idx++) {
            ucs_assert(req->step->fragment_pending[frag_idx] == 0);
        }
    }

    /* Check if this is the last step */
    if (req->step->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP) {
        ucs_assert(user_req == NULL); /* not directly from step_execute() */
        ucg_builtin_comp_last_step_cb(req, UCS_OK);
        ucg_builtin_comp_ft_end_step(req->step);
        return UCS_OK;
    }

    /* Start on the next step for this collective operation */
    ucg_builtin_op_step_t *next_step  = ++req->step;
    req->pending                      = next_step->fragments *
                                        next_step->phase->ep_cnt;
    next_step->am_header.msg.coll_id  = req->latest.coll_id;
    req->latest.step_idx              = next_step->am_header.msg.step_idx;

    ucs_status_t status = ucg_builtin_step_execute(req, user_req);
    ucg_builtin_comp_ft_end_step(next_step - 1);
    return status;
}
static void UCS_F_ALWAYS_INLINE
ucg_builtin_mpi_reduce(void *mpi_op, void *src, void *dst,
                       int dcount, void* mpi_datatype)
{
    UCS_PROFILE_CALL_VOID(ucg_params.mpi_reduce_f, mpi_op, (char*)src,
            (char*)dst, (unsigned)dcount, mpi_datatype);
}

static void UCS_F_ALWAYS_INLINE
ucg_builtin_mpi_reduce_req(uint8_t *dst, uint8_t *src, size_t length,
                           ucg_collective_params_t *params)
{
    ucs_assert((params->recv.dt_len * params->recv.count) == length);
    ucg_builtin_mpi_reduce(params->recv.op_ext, src, dst,
                           params->recv.count, params->recv.dt_ext);
}

static void UCS_F_ALWAYS_INLINE
ucg_builtin_comp_reduce_batched(ucg_builtin_request_t *req, uint64_t offset,
                                uint8_t *src, size_t size, size_t stride)
{
    uint8_t *dst = req->step->recv_buffer + offset;
    unsigned index, count = req->step->batch_cnt;
    for (index = 0; index < count; index++, src += stride) {
        ucg_builtin_mpi_reduce_req(dst, src, size, &req->op->super.params);
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

static void ucg_builtin_comp_rndv_write(ucg_builtin_request_t *req,
                                         uint64_t offset, uint8_t *payload,
                                         int is_to_remote)
{
    ucg_builtin_op_step_t *step             = req->step;
    ucg_builtin_op_step_remote_info_t *info = &step->zcopy.info;
    uct_completion_t *comp                  = &step->zcopy.zcomp->comp;
    uct_rkey_bundle_t *rkey                 =
             &info->remote_buffer[UCG_BUILTIN_OP_REMOTE_BUFFER_USAGE_DATA];
    ucs_assert(info->usage_mask & UCG_BUILTIN_OP_REMOTE_BUFFER_USAGE_DATA);

    /* Unpack the incoming key (from binary form) */
    ucs_status_t status = uct_rkey_unpack(req->step->zcopy.cmpt, payload, rkey);
    if (ucs_unlikely(status != UCS_OK)) {
        goto rndv_read_err;
    }

    /* Initiate the read request */
    const uct_iov_t iov[0];
    uct_ep_h ep = NULL; // TODO
    uint64_t remote_addr = 0; // TODO
    status = uct_ep_get_zcopy(ep, iov, 1, remote_addr, rkey->rkey, comp);
    if (ucs_unlikely(status != UCS_OK)) {
        goto rndv_read_err;
    }

    return; /* All good! */

rndv_read_err:
    ucg_builtin_comp_last_step_cb(req, status);
}

static void ucg_builtin_comp_rndv_read(ucg_builtin_request_t *req,
                                       uint64_t offset, uint8_t *payload,
                                       int is_from_remote, int is_reduced)
{

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

    if (step->comp_aggregation != UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_NOP) {
        ucs_assert(length != 0);
    }

    /* Act according to the requested data action */
    switch (step->comp_aggregation) {
    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_NOP:
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_WRITE:
        memcpy(step->recv_buffer + offset, data, length);
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

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REDUCE:
        ucg_builtin_mpi_reduce_req(step->recv_buffer + offset, data,
                                   length, &req->op->super.params);
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REDUCE_BATCHED:
        size = step->buffer_length;
        ucg_builtin_comp_reduce_batched(req, offset, data, size,
                                        length + sizeof(ucg_builtin_header_t));
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_RNDV_READ_FROM_REMOTE_AND_REDUCE:
        ucg_builtin_comp_rndv_read(req, offset, data, 1, 1);
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_RNDV_READ_FROM_REMOTE:
        ucg_builtin_comp_rndv_read(req, offset, data, 1, 0);
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_RNDV_READ_FROM_LOCAL:
        ucg_builtin_comp_rndv_read(req, offset, data, 0, 0);
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_RNDV_WRITE_TO_REMOTE:
        ucg_builtin_comp_rndv_write(req, offset, data, 1);
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_RNDV_WRITE_TO_LOCAL:
        ucg_builtin_comp_rndv_write(req, offset, data, 0);
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
        UCG_IF_STILL_PENDING(req, step->fragments, 1) {
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
        (void) ucg_builtin_comp_step_cb(req, NULL);
        break;

    case UCG_BUILTIN_OP_STEP_COMP_SEND:
        (void) ucg_builtin_step_execute(req, NULL);
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
