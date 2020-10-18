/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_MPI_H_
#define UCG_MPI_H_

#include <ucg/api/ucg.h>

BEGIN_C_DECLS

/*
 * Below are the definitions targeted specifically for the MPI standard.
 * This includes a list of predefined collective operations, and the modifiers
 * that describe each. The macros generate functions with prototypes matching
 * the MPI requirement, including the same arguments the user would pass.
 */

enum ucg_predefined {
    UCG_PRIMITIVE_BARRIER,
    UCG_PRIMITIVE_REDUCE,
    UCG_PRIMITIVE_GATHER,
    UCG_PRIMITIVE_GATHERV,
    UCG_PRIMITIVE_BCAST,
    UCG_PRIMITIVE_SCATTER,
    UCG_PRIMITIVE_SCATTERV,
    UCG_PRIMITIVE_ALLREDUCE,
    UCG_PRIMITIVE_ALLTOALL,
    UCG_PRIMITIVE_REDUCE_SCATTER,
    UCG_PRIMITIVE_ALLGATHER,
    UCG_PRIMITIVE_ALLGATHERV,
    UCG_PRIMITIVE_ALLTOALLW,
    UCG_PRIMITIVE_NEIGHBOR_ALLTOALLW
};

static uint16_t ucg_predefined_modifiers[] = {
    [UCG_PRIMITIVE_BARRIER]            = UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE |
                                         UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST |
                                         UCG_GROUP_COLLECTIVE_MODIFIER_BARRIER |
                                         UCG_GROUP_COLLECTIVE_MODIFIER_IGNORE_SEND |
                                         UCG_GROUP_COLLECTIVE_MODIFIER_IGNORE_RECV,
    [UCG_PRIMITIVE_REDUCE]             = UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE |
                                         UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_DESTINATION,
    [UCG_PRIMITIVE_GATHER]             = UCG_GROUP_COLLECTIVE_MODIFIER_CONCATENATE |
                                         UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_DESTINATION,
    [UCG_PRIMITIVE_GATHERV]            = UCG_GROUP_COLLECTIVE_MODIFIER_CONCATENATE |
                                         UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_DESTINATION |
                                         UCG_GROUP_COLLECTIVE_MODIFIER_VARIADIC,
    [UCG_PRIMITIVE_BCAST]              = UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST |
                                         UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_SOURCE,
    [UCG_PRIMITIVE_SCATTER]            = UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_SOURCE,
    [UCG_PRIMITIVE_SCATTERV]           = UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_SOURCE |
                                         UCG_GROUP_COLLECTIVE_MODIFIER_VARIADIC,
    [UCG_PRIMITIVE_ALLREDUCE]          = UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE |
                                         UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST,
    [UCG_PRIMITIVE_ALLTOALL]           = 0,
    [UCG_PRIMITIVE_REDUCE_SCATTER]     = UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE |
                                         UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_SOURCE,
    [UCG_PRIMITIVE_ALLGATHER]          = UCG_GROUP_COLLECTIVE_MODIFIER_CONCATENATE |
                                         UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST,
    [UCG_PRIMITIVE_ALLGATHERV]         = UCG_GROUP_COLLECTIVE_MODIFIER_CONCATENATE |
                                         UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST |
                                         UCG_GROUP_COLLECTIVE_MODIFIER_VARIADIC,
    [UCG_PRIMITIVE_ALLTOALLW]          = UCG_GROUP_COLLECTIVE_MODIFIER_VARIADIC |
                                         UCG_GROUP_COLLECTIVE_MODIFIER_VARIADIC,
    [UCG_PRIMITIVE_NEIGHBOR_ALLTOALLW] = UCG_GROUP_COLLECTIVE_MODIFIER_NEIGHBOR |
                                         UCG_GROUP_COLLECTIVE_MODIFIER_VARIADIC |
                                         UCG_GROUP_COLLECTIVE_MODIFIER_VARIADIC,
};

static UCS_F_ALWAYS_INLINE ucs_status_t ucg_coll_barrier_init(               \
        ucg_collective_callback_t cb, ucg_group_h group, ucg_coll_h *coll_p) \
{                                                                            \
    ucg_collective_params_t params;                                          \
    params.type.modifiers = ucg_predefined_modifiers[UCG_PRIMITIVE_BARRIER]; \
    params.recv_only.type.modifiers = params.type.modifiers;                 \
    params.recv_only.type.root      = 0;                                     \
    params.recv_only.comp_cb        = cb;                                    \
    params.send.dt_len              = 0;                                     \
    return ucg_collective_create(group, &params, coll_p);                    \
}

#define UCG_COLL_PARAMS_BUF_R(_buf, _count, _dt_len, _dt_ext) \
    .buffer   = _buf,                                         \
    .count    = _count,                                       \
    .dt_len   = _dt_len,                                      \
    .dt_ext   = _dt_ext,                                      \
    .op_ext   = op,                                           \
    .reserved = 0

#define UCG_COLL_PARAMS_BUF_V(_buf, _counts, _dt_len, _dt_ext, _displs) \
    .buffer   = _buf,                                                   \
    .counts   = _counts,                                                \
    .dt_len   = _dt_len,                                                \
    .dt_ext   = _dt_ext,                                                \
    .displs   = _displs,                                                \
    .reserved = 0

#define UCG_COLL_PARAMS_BUF_W(_buf, _counts, _dts_len, _dts_ext, _displs) \
    .buffer   = _buf,                                                     \
    .counts   = _counts,                                                  \
    .dts_len  = _dts_len,                                                 \
    .dts_ext  = _dts_ext,                                                 \
    .displs   = _displs,                                                  \
    .reserved = 0

#define UCG_COLL_INIT_HALF(_lname, _uname, _non_root_only_sends, _stype,       \
                           _sargs, _rtype, _rargs, ...)                        \
static UCS_F_ALWAYS_INLINE ucs_status_t ucg_coll_##_lname##_init(__VA_ARGS__,  \
        ucg_collective_callback_t cb, void *op, ucg_group_member_index_t root, \
        unsigned modifiers, int is_root, ucg_group_h group, ucg_coll_h *coll_p)\
{                                                                              \
    uint16_t md = modifiers | ucg_predefined_modifiers[UCG_PRIMITIVE_##_uname];\
    if (is_root) {                                                             \
        ucg_collective_params_t full_params = {                                \
            .type = {                                                          \
                .modifiers = md,                                               \
                .root      = root                                              \
            },                                                                 \
            .comp_cb = cb,                                                     \
            .send = {                                                          \
                UCG_COLL_PARAMS_BUF##_stype _sargs                             \
            },                                                                 \
            .recv = {                                                          \
                UCG_COLL_PARAMS_BUF##_rtype _rargs                             \
            },                                                                 \
            .recv_only = { { 0, 0 }, NULL }                                    \
        };                                                                     \
        return ucg_collective_create(group, &full_params, coll_p);             \
    } else {                                                                   \
        if (_non_root_only_sends) {                                            \
            md |= UCG_GROUP_COLLECTIVE_MODIFIER_IGNORE_RECV;                   \
            ucg_collective_params_t send_params = {                            \
                .type = {                                                      \
                    .modifiers = md,                                           \
                    .root      = root                                          \
                },                                                             \
                .comp_cb = cb,                                                 \
                .send = {                                                      \
                    UCG_COLL_PARAMS_BUF##_stype _sargs                         \
                }                                                              \
            };                                                                 \
            return ucg_collective_create(group, &send_params, coll_p);         \
        } else {                                                               \
            md |= UCG_GROUP_COLLECTIVE_MODIFIER_IGNORE_SEND;                   \
            ucg_collective_params_t recv_params = {                            \
                .type = {                                                      \
                    .modifiers = md,                                           \
                },                                                             \
                .recv = {                                                      \
                    UCG_COLL_PARAMS_BUF##_rtype _rargs                         \
                },                                                             \
                .recv_only = {                                                 \
                    .type = {                                                  \
                        .modifiers = md,                                       \
                        .root      = root                                      \
                    },                                                         \
                    .comp_cb = cb                                              \
                }                                                              \
            };                                                                 \
            return ucg_collective_create(group, &recv_params, coll_p);         \
        }                                                                      \
    }                                                                          \
}

#define UCG_COLL_INIT_FULL(_lname, _uname, _ignored, _stype, _sargs, _rtype,   \
                           _rargs, ...)                                        \
static UCS_F_ALWAYS_INLINE ucs_status_t ucg_coll_##_lname##_init(__VA_ARGS__,  \
        ucg_collective_callback_t cb, void *op, ucg_group_member_index_t root, \
        unsigned modifiers, ucg_group_h group, ucg_coll_h *coll_p)             \
{                                                                              \
    uint16_t md = modifiers | ucg_predefined_modifiers[UCG_PRIMITIVE_##_uname];\
    ucg_collective_params_t params = {                                         \
        .type = {                                                              \
            .modifiers = md,                                                   \
            .root      = root                                                  \
        },                                                                     \
        .comp_cb = cb,                                                         \
        .send = {                                                              \
            UCG_COLL_PARAMS_BUF##_stype _sargs                                 \
        },                                                                     \
        .recv = {                                                              \
            UCG_COLL_PARAMS_BUF##_rtype _rargs                                 \
        },                                                                     \
        .recv_only = { { 0, 0 }, NULL }                                        \
    };                                                                         \
    return ucg_collective_create(group, &params, coll_p);                      \
}

#define UCG_COLL_INIT_FUNC_SR1_RR1(_lname, _uname, _mode, _treat_root) \
UCG_COLL_INIT_##_mode(_lname, _uname, _treat_root,                     \
                   _R, ((char*)sbuf, count, len_dtype, mpi_dtype),     \
                   _R, (       rbuf, count, len_dtype, mpi_dtype),     \
                   const void *sbuf, void *rbuf, int count,            \
                   size_t len_dtype, void *mpi_dtype)

#define UCG_COLL_INIT_FUNC_SR1_RRN(_lname, _uname, _mode, _treat_root) \
UCG_COLL_INIT_##_mode(_lname, _uname, _treat_root,                     \
                   _R, ((char*)sbuf, scount, len_sdtype, mpi_sdtype),  \
                   _R, (       rbuf, rcount, len_rdtype, mpi_rdtype),  \
                   const void *sbuf, int scount, size_t len_sdtype, void *mpi_sdtype,\
                         void *rbuf, int rcount, size_t len_rdtype, void *mpi_rdtype)

#define UCG_COLL_INIT_FUNC_SR1_RVN(_lname, _uname, _mode, _treat_root)          \
UCG_COLL_INIT_##_mode(_lname, _uname, _treat_root,                              \
                   _R, ((char*)sbuf, scount,  len_sdtype, mpi_sdtype),          \
                   _V, (       rbuf, rcounts, len_rdtype, mpi_rdtype, rdispls), \
                   const void *sbuf,       int  scount,                      size_t len_sdtype, void *mpi_sdtype, \
                         void *rbuf, const int *rcounts, const int *rdispls, size_t len_rdtype, void *mpi_rdtype)

#define UCG_COLL_INIT_FUNC_SVN_RR1(_lname, _uname, _mode, _treat_root)          \
UCG_COLL_INIT_##_mode(_lname, _uname, _treat_root,                              \
                   _V, ((char*)sbuf, scounts, len_sdtype, mpi_sdtype, sdispls), \
                   _R, (       rbuf, rcount,  len_rdtype, mpi_rdtype),          \
                   const void *sbuf, const int *scounts, const int *sdispls, size_t len_sdtype, void *mpi_sdtype, \
                         void *rbuf,       int  rcount,                      size_t len_rdtype, void *mpi_rdtype)

#define UCG_COLL_INIT_FUNC_SWN_RWN(_lname, _uname, _mode, _treat_root)            \
UCG_COLL_INIT_##_mode(_lname, _uname, _treat_root,                                \
                   _W, ((char*)sbuf, scounts, len_sdtypes, mpi_sdtypes, sdispls), \
                   _W, (       rbuf, rcounts, len_rdtypes, mpi_rdtypes, rdispls), \
                   const void *sbuf, int *scounts, int *sdispls, size_t *len_sdtypes, void **mpi_sdtypes, \
                         void *rbuf, int *rcounts, int *rdispls, size_t *len_rdtypes, void **mpi_rdtypes)

#define IDENT(x)  ( x)
#define INVERT(x) (!x)

UCG_COLL_INIT_FUNC_SR1_RR1(reduce,             REDUCE,             HALF, 1)
UCG_COLL_INIT_FUNC_SR1_RRN(gather,             GATHER,             HALF, 1)
UCG_COLL_INIT_FUNC_SR1_RVN(gatherv,            GATHERV,            HALF, 1)
UCG_COLL_INIT_FUNC_SR1_RR1(bcast,              BCAST,              HALF, 0)
UCG_COLL_INIT_FUNC_SR1_RRN(scatter,            SCATTER,            HALF, 0)
UCG_COLL_INIT_FUNC_SVN_RR1(scatterv,           SCATTERV,           HALF, 0)
UCG_COLL_INIT_FUNC_SR1_RR1(allreduce,          ALLREDUCE,          FULL, 0)
UCG_COLL_INIT_FUNC_SR1_RRN(alltoall,           ALLTOALL,           FULL, 0)
UCG_COLL_INIT_FUNC_SR1_RRN(allgather,          ALLGATHER,          FULL, 0)
UCG_COLL_INIT_FUNC_SR1_RVN(allgatherv,         ALLGATHERV,         FULL, 0)
UCG_COLL_INIT_FUNC_SWN_RWN(alltoallw,          ALLTOALLW,          FULL, 0)
UCG_COLL_INIT_FUNC_SWN_RWN(neighbor_alltoallw, NEIGHBOR_ALLTOALLW, FULL, 0)

END_C_DECLS

#endif
