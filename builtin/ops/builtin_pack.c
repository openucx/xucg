/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "builtin_ops.h"

int ucg_builtin_atomic_reduce_full(ucg_builtin_request_t *req, uint64_t offset,
        void *src, void *dst, size_t length);
int ucg_builtin_atomic_reduce_part(ucg_builtin_request_t *req, uint64_t offset,
        void *src, void *dst, size_t length);

#define UCG_BUILTIN_BCOPY_PACK_CB(_source, _offset, _length) { \
    ucg_builtin_header_t *header = (ucg_builtin_header_t*)dest; \
    ucg_builtin_request_t *req   = (ucg_builtin_request_t*)arg; \
    ucg_builtin_op_step_t *step  = req->step; \
    size_t buffer_length         = (_length); \
    header->header               = step->am_header.header; \
    ucs_assert(((uintptr_t)arg & UCT_PACK_CALLBACK_REDUCE) == 0); \
    memcpy(header + 1, (_source) + (_offset), buffer_length); \
    return sizeof(*header) + buffer_length; \
}

UCG_BUILTIN_PACKER_DECLARE(_, _single, _sbuf)
UCG_BUILTIN_BCOPY_PACK_CB(step->send_buffer, 0,                 step->buffer_length)

UCG_BUILTIN_PACKER_DECLARE(_, _full, _sbuf)
UCG_BUILTIN_BCOPY_PACK_CB(step->send_buffer, step->iter_offset, step->fragment_length)

UCG_BUILTIN_PACKER_DECLARE(_, _part, _sbuf)
UCG_BUILTIN_BCOPY_PACK_CB(step->send_buffer, step->iter_offset, step->buffer_length - step->iter_offset)

UCG_BUILTIN_PACKER_DECLARE(_, _single, _rbuf)
UCG_BUILTIN_BCOPY_PACK_CB(step->recv_buffer, 0,                 step->buffer_length)

UCG_BUILTIN_PACKER_DECLARE(_, _full, _rbuf)
UCG_BUILTIN_BCOPY_PACK_CB(step->recv_buffer, step->iter_offset, step->fragment_length)

UCG_BUILTIN_PACKER_DECLARE(_, _part, _rbuf)
UCG_BUILTIN_BCOPY_PACK_CB(step->recv_buffer, step->iter_offset, step->buffer_length - step->iter_offset)

#define UCG_BUILTIN_COLL_PACK_CB(source, offset, length, part) { \
    /* First writer to this buffer - overwrite the existing data */ \
    int is_reduce_needed = (uintptr_t)arg & UCT_PACK_CALLBACK_REDUCE; \
    if (ucs_unlikely(is_reduce_needed)) { \
        /* Otherwise - reduce onto existing data */ \
        ucg_builtin_request_t *req   = (ucg_builtin_request_t*)((uintptr_t)arg \
                                        ^ UCT_PACK_CALLBACK_REDUCE); \
        ucg_builtin_op_step_t *step  = req->step; \
        ucg_builtin_header_t *header = (ucg_builtin_header_t*)dest; \
        header->header               = step->am_header.header; \
        return sizeof(*header) + ucg_builtin_atomic_reduce_ ## part \
                (req, offset, source, header + 1, length); \
    } else { \
        UCG_BUILTIN_BCOPY_PACK_CB(source, offset, length) \
    } \
}

UCG_BUILTIN_PACKER_DECLARE(_locked, _single, _sbuf)
UCG_BUILTIN_COLL_PACK_CB(step->send_buffer, 0,                 step->buffer_length, part)

UCG_BUILTIN_PACKER_DECLARE(_locked, _full, _sbuf)
UCG_BUILTIN_COLL_PACK_CB(step->send_buffer, step->iter_offset, step->fragment_length, full)

UCG_BUILTIN_PACKER_DECLARE(_locked, _part, _sbuf)
UCG_BUILTIN_COLL_PACK_CB(step->send_buffer, step->iter_offset, step->buffer_length - step->iter_offset, part)

UCG_BUILTIN_PACKER_DECLARE(_locked, _single, _rbuf)
UCG_BUILTIN_COLL_PACK_CB(step->recv_buffer, 0,                 step->buffer_length, part)

UCG_BUILTIN_PACKER_DECLARE(_locked, _full, _rbuf)
UCG_BUILTIN_COLL_PACK_CB(step->recv_buffer, step->iter_offset, step->fragment_length, full)

UCG_BUILTIN_PACKER_DECLARE(_locked, _part, _rbuf)
UCG_BUILTIN_COLL_PACK_CB(step->recv_buffer, step->iter_offset, step->buffer_length - step->iter_offset, part)
