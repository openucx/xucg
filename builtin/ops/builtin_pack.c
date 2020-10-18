/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "builtin_ops.h"

#include <ucs/arch/atomic.h>

#ifndef HAVE_UCT_COLLECTIVES
#define UCT_PACK_CALLBACK_REDUCE ((uintptr_t)-1)
#endif

int ucg_builtin_atomic_reduce_full(ucg_builtin_request_t *req,
        void *src, void *dst, size_t length);
int ucg_builtin_atomic_reduce_part(ucg_builtin_request_t *req,
        void *src, void *dst, size_t length);

#define UCG_BUILTIN_PACK_CB(_offset, _length) { \
    ucg_builtin_header_t *header = (ucg_builtin_header_t*)dest; \
    ucg_builtin_request_t *req   = (ucg_builtin_request_t*)arg; \
    ucg_builtin_op_step_t *step  = req->step; \
    size_t buffer_length         = (_length); \
    header->header               = step->am_header.header; \
    ucs_assert(((uintptr_t)arg & UCT_PACK_CALLBACK_REDUCE) == 0); \
    memcpy(header + 1, step->send_buffer + (_offset), buffer_length); \
    return sizeof(*header) + buffer_length; \
}

UCG_BUILTIN_PACKER_DECLARE(_, single)
UCG_BUILTIN_PACK_CB(0,                 step->buffer_length)

UCG_BUILTIN_PACKER_DECLARE(_, full)
UCG_BUILTIN_PACK_CB(step->iter_offset, step->fragment_length)

UCG_BUILTIN_PACKER_DECLARE(_, part)
UCG_BUILTIN_PACK_CB(step->iter_offset, step->buffer_length - step->iter_offset)

#define UCG_BUILTIN_REDUCING_PACK_CB(_offset, _length, _part) { \
    if ((uintptr_t)arg & UCT_PACK_CALLBACK_REDUCE) { \
        ucg_builtin_request_t *req   = (ucg_builtin_request_t*)((uintptr_t)arg \
                                        ^ UCT_PACK_CALLBACK_REDUCE); \
        ucg_builtin_op_step_t *step  = req->step; \
        ucg_builtin_header_t *header = (ucg_builtin_header_t*)dest; \
        ucs_assert(header->header == step->am_header.header); \
        return sizeof(*header) + ucg_builtin_atomic_reduce_ ## _part \
                (req, step->send_buffer + (_offset), header + 1, (_length)); \
    } else { \
        UCG_BUILTIN_PACK_CB((_offset), (_length)) \
    } \
}

UCG_BUILTIN_PACKER_DECLARE(_reducing_, single)
UCG_BUILTIN_REDUCING_PACK_CB(0,                 step->buffer_length, full)

UCG_BUILTIN_PACKER_DECLARE(_reducing_, full)
UCG_BUILTIN_REDUCING_PACK_CB(step->iter_offset, step->fragment_length, part)

UCG_BUILTIN_PACKER_DECLARE(_reducing_, part)
UCG_BUILTIN_REDUCING_PACK_CB(step->iter_offset, step->buffer_length -
                                                step->iter_offset, part)

#define UCG_BUILTIN_VARIADIC_PACK_CB(_offset, _length, _part) { \
        { /* Separate scope for a separate name-space (e.g. "step" conflict)*/ \
            ucg_builtin_request_t *req  = (ucg_builtin_request_t*)arg; \
            ucg_builtin_op_step_t *step = req->step; \
\
            uint8_t *buffer; \
            size_t length; \
\
            ucg_builtin_step_get_local_address(step + 1, 1, &buffer, &length); \
            ucg_builtin_step_set_remote_address(step, &buffer); \
        } \
        { \
            UCG_BUILTIN_PACK_CB((_offset), (_length)) \
        } \
}

UCG_BUILTIN_PACKER_DECLARE(_variadic_, single)
UCG_BUILTIN_VARIADIC_PACK_CB(0,                 step->buffer_length, full)

UCG_BUILTIN_PACKER_DECLARE(_variadic_, full)
UCG_BUILTIN_VARIADIC_PACK_CB(step->iter_offset, step->fragment_length, part)

UCG_BUILTIN_PACKER_DECLARE(_variadic_, part)
UCG_BUILTIN_VARIADIC_PACK_CB(step->iter_offset, step->buffer_length -
                                                step->iter_offset, part)

#define UCG_BUILTIN_ATOMIC_SINGLE_PACK_CB(_integer_bits) { \
    ucg_builtin_header_t *header = (ucg_builtin_header_t*)dest; \
    ucg_builtin_request_t *req   = (ucg_builtin_request_t*)arg; \
    ucg_builtin_op_step_t *step  = req->step; \
    uint##_integer_bits##_t *ptr = (uint##_integer_bits##_t *)(header + 1); \
    ucs_atomic_add##_integer_bits (ptr, \
            *(uint##_integer_bits##_t *)step->send_buffer); \
printf("UCG_BUILTIN_ATOMIC_SINGLE_PACK_CB\n");\
    return sizeof(uint##_integer_bits##_t); \
}

#define UCG_BUILTIN_ATOMIC_MULTIPLE_PACK_CB(_integer_bits) { \
    ucg_builtin_header_t *header = (ucg_builtin_header_t*)dest; \
    ucg_builtin_request_t *req   = (ucg_builtin_request_t*)arg; \
    ucg_builtin_op_step_t *step  = req->step; \
    uint##_integer_bits##_t *ptr = (uint##_integer_bits##_t *)(header + 1); \
    size_t length                = step->buffer_length; \
    unsigned index, count        = length / sizeof(*ptr); \
    ucs_assert((step->buffer_length % sizeof(*ptr)) == 0); \
    \
    for (index = 0; index < count; ptr++, index++) { \
        ucs_atomic_add##_integer_bits (ptr, \
                *(uint##_integer_bits##_t *)step->send_buffer); \
    } \
printf("UCG_BUILTIN_ATOMIC_MULTIPLE_PACK_CB\n");\
    return length; \
}

UCG_BUILTIN_PACKER_DECLARE(_atomic_single_, 8)
UCG_BUILTIN_ATOMIC_SINGLE_PACK_CB(8)

UCG_BUILTIN_PACKER_DECLARE(_atomic_multiple_, 8)
UCG_BUILTIN_ATOMIC_MULTIPLE_PACK_CB(8)

UCG_BUILTIN_PACKER_DECLARE(_atomic_single_, 16)
UCG_BUILTIN_ATOMIC_SINGLE_PACK_CB(16)

UCG_BUILTIN_PACKER_DECLARE(_atomic_multiple_, 16)
UCG_BUILTIN_ATOMIC_MULTIPLE_PACK_CB(16)

UCG_BUILTIN_PACKER_DECLARE(_atomic_single_, 32)
UCG_BUILTIN_ATOMIC_SINGLE_PACK_CB(32)

UCG_BUILTIN_PACKER_DECLARE(_atomic_multiple_, 32)
UCG_BUILTIN_ATOMIC_MULTIPLE_PACK_CB(32)

UCG_BUILTIN_PACKER_DECLARE(_atomic_single_, 64)
UCG_BUILTIN_ATOMIC_SINGLE_PACK_CB(64)

UCG_BUILTIN_PACKER_DECLARE(_atomic_multiple_, 64)
UCG_BUILTIN_ATOMIC_MULTIPLE_PACK_CB(64)
