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
                                         UCG_GROUP_COLLECTIVE_MODIFIER_BARRIER,
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

static UCS_F_ALWAYS_INLINE ucs_status_t
ucg_coll_barrier_init(int ign, ucg_group_h group, ucg_coll_h *coll_p)
{
    ucg_collective_params_t params    = {0};
    UCG_PARAM_TYPE(&params).modifiers =
            ucg_predefined_modifiers[UCG_PRIMITIVE_BARRIER];

    return ucg_collective_create(group, &params, coll_p);
}

#define UCG_COLL_PARAMS_BUF_R(_buffer, _count, _dtype) \
    .buffer = _buffer, \
    .count  = _count,  \
    .dtype  = _dtype

#define UCG_COLL_PARAMS_BUF_O(_buffer, _count, _dtype) \
    .op     = op, \
    UCG_COLL_PARAMS_BUF_R(_buffer, _count, _dtype)

#define UCG_COLL_PARAMS_BUF_V(_buffer, _counts, _dtype, _displs) \
    .displs = _displs, \
    .buffer = _buffer, \
    .counts = _counts, \
    .dtype  = _dtype

#define UCG_COLL_PARAMS_BUF_W(_buffer, _counts, _dtypes, _displs) \
    .displs = _displs, \
    .buffer = _buffer, \
    .counts = _counts, \
    .dtypes = _dtypes

#define UCG_COLL_INIT(_lname, _uname, _stype, _sargs, _rtype, _rargs,...)\
static UCS_F_ALWAYS_INLINE ucs_status_t ucg_coll_##_lname##_init(__VA_ARGS__,  \
        void *op, ucg_group_member_index_t root, unsigned modifiers,           \
        ucg_group_h group, ucg_coll_h *coll_p)                                 \
{                                                                              \
    uint16_t md = modifiers | ucg_predefined_modifiers[UCG_PRIMITIVE_##_uname];\
    ucg_collective_params_t params = {                                         \
        .send = {                                                              \
            .type = {                                                          \
                .modifiers = md,                                               \
                .root      = root                                              \
            },                                                                 \
            UCG_COLL_PARAMS_BUF##_stype _sargs                                 \
        },                                                                     \
        .recv = {                                                              \
            UCG_COLL_PARAMS_BUF##_rtype _rargs                                 \
        },                                                                     \
    };                                                                         \
    return ucg_collective_create(group, &params, coll_p);                      \
}

#define UCG_COLL_INIT_FUNC_SR1_RR1(_lname, _uname) \
UCG_COLL_INIT(_lname, _uname, \
              _R, ((char*)sbuf, count, mpi_dtype), \
              _O, (       rbuf, count, mpi_dtype), \
              const void *sbuf, void *rbuf, int count, void *mpi_dtype)

#define UCG_COLL_INIT_FUNC_SR1_RRN(_lname, _uname) \
UCG_COLL_INIT(_lname, _uname, \
              _R, ((char*)sbuf, scount, mpi_sdtype), \
              _O, (       rbuf, rcount, mpi_rdtype), \
              const void *sbuf, int scount, void *mpi_sdtype,\
                    void *rbuf, int rcount, void *mpi_rdtype)

#define UCG_COLL_INIT_FUNC_SR1_RVN(_lname, _uname) \
UCG_COLL_INIT(_lname, _uname, \
              _R, ((char*)sbuf, scount,  mpi_sdtype), \
              _V, (       rbuf, rcounts, mpi_rdtype, rdispls), \
              const void *sbuf,       int  scount,                      void *mpi_sdtype, \
                    void *rbuf, const int *rcounts, const int *rdispls, void *mpi_rdtype)

#define UCG_COLL_INIT_FUNC_SVN_RR1(_lname, _uname) \
UCG_COLL_INIT(_lname, _uname, \
              _V, ((char*)sbuf, scounts, mpi_sdtype, sdispls), \
              _R, (       rbuf, rcount,  mpi_rdtype), \
              const void *sbuf, const int *scounts, const int *sdispls, void *mpi_sdtype, \
                    void *rbuf,       int  rcount,                      void *mpi_rdtype)

#define UCG_COLL_INIT_FUNC_SWN_RWN(_lname, _uname) \
UCG_COLL_INIT(_lname, _uname, \
              _W, ((char*)sbuf, scounts, mpi_sdtypes, sdispls), \
              _W, (       rbuf, rcounts, mpi_rdtypes, rdispls), \
              const void *sbuf, int *scounts, int *sdispls, void **mpi_sdtypes, \
                    void *rbuf, int *rcounts, int *rdispls, void **mpi_rdtypes)

UCG_COLL_INIT_FUNC_SR1_RR1(reduce,             REDUCE)
UCG_COLL_INIT_FUNC_SR1_RRN(gather,             GATHER)
UCG_COLL_INIT_FUNC_SR1_RVN(gatherv,            GATHERV)
UCG_COLL_INIT_FUNC_SR1_RR1(bcast,              BCAST)
UCG_COLL_INIT_FUNC_SR1_RRN(scatter,            SCATTER)
UCG_COLL_INIT_FUNC_SVN_RR1(scatterv,           SCATTERV)
UCG_COLL_INIT_FUNC_SR1_RR1(allreduce,          ALLREDUCE)
UCG_COLL_INIT_FUNC_SR1_RRN(alltoall,           ALLTOALL)
UCG_COLL_INIT_FUNC_SR1_RRN(allgather,          ALLGATHER)
UCG_COLL_INIT_FUNC_SR1_RVN(allgatherv,         ALLGATHERV)
UCG_COLL_INIT_FUNC_SWN_RWN(alltoallw,          ALLTOALLW)
UCG_COLL_INIT_FUNC_SWN_RWN(neighbor_alltoallw, NEIGHBOR_ALLTOALLW)

END_C_DECLS

#endif
