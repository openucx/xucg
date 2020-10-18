/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_BUILTIN_OPS_H_
#define UCG_BUILTIN_OPS_H_

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include "../plan/builtin_plan.h"
#include <ucp/core/ucp_request.h>
#include <ucs/datastruct/ptr_array.h>

#ifndef HAVE_UCP_EXTENSIONS
#define UCT_COLL_DTYPE_MODE_BITS (0)
#endif

BEGIN_C_DECLS

/*
 * The built-in collective operations are composed of one or more steps.
 * In each step, we apply a method to a subgroup of peer processes.
 * Collectives are planned using "templates", and once the user
 * provides the details a step is "instantiated" from a suitable
 * template and the instance is executed. Often more than one instance
 * is created from the same template, and instances can run side-by-side.
 *
 * Methods are the basic algorithmic building blocks, like fan-in and
 * fan-out for trees, or the "Recursive K-ing" algorithm.
 * For example, Allreduce can either be done in two step,
 * fan-in and fanout, or in a single Recursive K-ing step.
 * Once the user requests an Allreduce operation - the selected
 * step templates are used to generate an instance
 * (or it is fetched from cache) and that instance is executed.
 */

extern ucg_plan_component_t ucg_builtin_component;


typedef union ucg_builtin_header_step {
    struct {
        ucg_coll_id_t  coll_id;
        ucg_step_idx_t step_idx;
    };
    uint16_t local_id;
} ucg_builtin_header_step_t;

typedef union ucg_builtin_header {
    struct {
        ucg_group_id_t group_id;
        ucg_builtin_header_step_t msg;
        ucg_offset_t remote_offset;
    };
    uint64_t header;
} ucg_builtin_header_t;

/*
 * The builtin operation
 */
enum ucg_builtin_op_step_flags {
    /* General characteristics */
    UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP         = UCS_BIT(0),
    UCG_BUILTIN_OP_STEP_FLAG_SINGLE_ENDPOINT   = UCS_BIT(1),
    UCG_BUILTIN_OP_STEP_FLAG_SEND_STRIDED      = UCS_BIT(2),
    UCG_BUILTIN_OP_STEP_FLAG_SEND_VARIADIC     = UCS_BIT(3),
    UCG_BUILTIN_OP_STEP_FLAG_PIPELINED         = UCS_BIT(4),

    /* Alternative Methods for using the list of endpoints */
    UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND   = UCS_BIT(5),
    UCG_BUILTIN_OP_STEP_FLAG_RECV_BEFORE_SEND1 = UCS_BIT(6),
    UCG_BUILTIN_OP_STEP_FLAG_RECV1_BEFORE_SEND = UCS_BIT(7),

    /* Alternative Send types */
    UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT     = UCS_BIT(8),
    UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY     = UCS_BIT(9),
    UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY     = UCS_BIT(10),
    /* Note: AM_ZCOPY is like BCOPY with registered memory: it uses "eager"
     *       protocol, as opposed to PUT/GET (below) which use "rendezvous" */
    UCG_BUILTIN_OP_STEP_FLAG_SEND_PUT_ZCOPY    = UCS_BIT(11),
    UCG_BUILTIN_OP_STEP_FLAG_SEND_GET_ZCOPY    = UCS_BIT(12),
    UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED        = UCS_BIT(13),
    UCG_BUILTIN_OP_STEP_FLAG_WRITE_REMOTE_ADDR = UCS_BIT(14),

    UCG_BUILTIN_OP_STEP_FLAG_SWITCH_MASK       = UCS_MASK(15),

    /* Additional step information */
    UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED  = UCS_BIT(15),
    UCG_BUILTIN_OP_STEP_FLAG_PACKED_DTYPE_MODE = UCS_BIT(16),
    UCG_BUILTIN_OP_STEP_FLAG_FT_ONGOING        = UCS_BIT(17)
};

enum ucg_builtin_op_step_comp_aggregate {
    UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_NOP,

    /* Aggregation of short (Active-)messages */
    UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_WRITE,
    UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_GATHER_TERMINAL,
    UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_GATHER_WAYPOINT,
    UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REDUCE_SINGLE,
    UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REDUCE_BATCHED,
    UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REDUCE_FRAGMENT,

    /* Unpacking remote memory keys (for Rendezvous protocol) */
    UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REMOTE_KEY
} UCS_S_PACKED;

enum ucg_builtin_op_step_comp_criteria {
    UCG_BUILTIN_OP_STEP_COMP_CRITERIA_SEND,
    UCG_BUILTIN_OP_STEP_COMP_CRITERIA_SINGLE_MESSAGE,
    UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES,
    UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES_ZCOPY,
    UCG_BUILTIN_OP_STEP_COMP_CRITERIA_BY_FRAGMENT_OFFSET
} UCS_S_PACKED;

enum ucg_builtin_op_step_comp_action {
    UCG_BUILTIN_OP_STEP_COMP_OP,
    UCG_BUILTIN_OP_STEP_COMP_STEP,
    UCG_BUILTIN_OP_STEP_COMP_SEND
} UCS_S_PACKED;

/* Definitions of several callback functions, used during an operation */
typedef struct ucg_builtin_op ucg_builtin_op_t;
typedef struct ucg_builtin_request ucg_builtin_request_t;
typedef void         (*ucg_builtin_op_init_cb_t)  (ucg_builtin_op_t *op,
                                                   ucg_coll_id_t coll_id);
typedef void         (*ucg_builtin_op_fini_cb_t)  (ucg_builtin_op_t *op,
                                                   ucg_request_t *user_req,
                                                   ucs_status_t status);
typedef ucs_status_t (*ucg_builtin_op_optm_cb_t)  (ucg_builtin_op_t *op);

typedef struct ucg_builtin_zcomp {
    uct_completion_t           comp;
    ucg_builtin_request_t     *req;
} ucg_builtin_zcomp_t;

typedef struct ucg_builtin_op_step {
    uint32_t                   flags;       /* @ref ucg_builtin_op_step_flags */
    uint8_t                    iter_ep;     /* iterator, somewhat volatile */
#define UCG_BUILTIN_OFFSET_PIPELINE_READY   ((ucg_offset_t)-1)
#define UCG_BUILTIN_OFFSET_PIPELINE_PENDING ((ucg_offset_t)-2)
    /* TODO: consider modifying "send_buffer" and removing iter_offset */

    /* These values determine the behavior of the incoming message handler */
    enum ucg_builtin_op_step_comp_aggregate comp_aggregation;
    enum ucg_builtin_op_step_comp_criteria  comp_criteria;
    enum ucg_builtin_op_step_comp_action    comp_action;
    /* --- 8 bytes --- */

    uint8_t                    uct_flags; /* e.g. UCT_SEND_FLAG_PACK_LOCK */
    uint8_t                    am_id;
    uint8_t                    ep_cnt;
    uint8_t                    batch_cnt;

    ucg_offset_t               iter_offset; /* iterator, somewhat volatile */

    /* --- 16 bytes --- */

    ucg_builtin_plan_phase_t  *phase;
    int8_t                    *send_buffer;
    size_t                     buffer_length;
    ucg_builtin_header_t       am_header;
    uct_iface_h                uct_iface;

    uint32_t                   fragments_total; /* != 1 for fragmented operations */
    uint32_t                   fragment_length; /* only for fragmented operations */

    /* --- 64 bytes --- */

    /* To enable pipelining of fragmented messages, each fragment has a counter,
     * similar to the request's overall "pending" counter. Once it reaches zero,
     * the fragment can be "forwarded" regardless of the other fragments.
     * This optimization is only valid for "*_WAYPOINT" methods. */
#define UCG_BUILTIN_FRAG_PENDING ((uint8_t)-1)
    volatile uint8_t          *fragment_pending;

    int8_t                    *recv_buffer;
    int                       *var_counts;
    int                       *var_displs;
    uct_md_h                   uct_md;

    /* Send-type-specific fields */
    union {
        struct {
            uct_pack_callback_t  pack_full_cb;
            uct_pack_callback_t  pack_part_cb;
            uct_pack_callback_t  pack_single_cb;
        } bcopy;
        struct {
            uct_mem_h            memh;   /* Data buffer memory handle */
            uct_component_h      cmpt;   /* which component registered the memory */
            ucg_builtin_zcomp_t  zcomp;  /* completion context for UCT zcopy */
            uint64_t             raddr;  /* remote address (from previous step) */
            uct_rkey_bundle_t    rkey;   /* remote key (from previous step) */
        } zcopy;
    };
} UCS_S_PACKED UCS_V_ALIGNED(sizeof(void*)) ucg_builtin_op_step_t;

typedef struct ucg_builtin_comp_slot ucg_builtin_comp_slot_t;
struct ucg_builtin_op {
    ucg_op_t                 super;
    unsigned                 opt_cnt; /**< optimization count-down */
    ucg_builtin_op_optm_cb_t optm_cb; /**< optimization function for the operation */
    ucg_builtin_op_init_cb_t init_cb; /**< Initialization function for the operation */
    ucg_builtin_op_fini_cb_t fini_cb; /**< Finalization function for the operation */
    ucg_builtin_comp_slot_t *slots;   /**< slots pointer, for faster initialization */
    ucg_builtin_op_step_t    steps[]; /**< steps required to complete the operation */
} UCS_V_ALIGNED(UCS_SYS_CACHE_LINE_SIZE);

/*
 * For every instance of the builtin collective operation (op), we create allocate
 * a request to handle completion and interaction with the user (via API).
 */
struct ucg_builtin_request {
    volatile uint32_t         pending;   /**< number of step's pending messages */
    ucg_builtin_header_step_t latest;    /**< request iterator, mostly here for
                                              alignment reasons with slot structs */
    ucg_builtin_op_step_t    *step;      /**< indicator of current step within the op */
    ucg_builtin_op_t         *op;        /**< operation currently running */
    ucg_request_t            *comp_req;  /**< completion status is written here */
};

/*
 * Incoming messages are processed for one of the collective operations
 * currently outstanding - arranged as a window (think: TCP) of slots.
 */
struct ucg_builtin_comp_slot {
    ucg_builtin_request_t req;
    ucs_ptr_array_t       messages;
} UCS_V_ALIGNED(UCS_SYS_CACHE_LINE_SIZE);

typedef struct ucg_builtin_ctx {
    ucs_ptr_array_t      group_by_id;
    uint16_t             am_id;
    ucg_builtin_config_t config;
} ucg_builtin_ctx_t;

ucs_status_t ucg_builtin_step_create(ucg_builtin_plan_t *plan,
                                     ucg_builtin_plan_phase_t *phase,
                                     enum ucg_builtin_op_step_flags *flags,
                                     const ucg_collective_params_t *params,
                                     int8_t **current_data_buffer,
                                     ucg_builtin_op_init_cb_t *init_cb,
                                     ucg_builtin_op_fini_cb_t *fini_cb,
                                     ucg_builtin_op_step_t *step,
                                     int *zcopy_step_skip);

ucs_status_t ucg_builtin_step_create_rkey_bcast(ucg_builtin_plan_t *plan,
                                                const ucg_collective_params_t *params,
                                                ucg_builtin_op_step_t *step);

ucs_status_t ucg_builtin_step_execute(ucg_builtin_request_t *req);

ucs_status_t ucg_builtin_step_zcopy_prep(ucg_builtin_op_step_t *step);

ucs_status_t ucg_builtin_op_select_callback(ucg_builtin_plan_t *plan,
                                            ucg_builtin_op_init_cb_t *init_cb);

ucs_status_t ucg_builtin_op_consider_optimization(ucg_builtin_op_t *op,
                                                  ucg_builtin_config_t *config);

void ucg_builtin_step_select_packers(const ucg_collective_params_t *params,
                                     ucg_builtin_op_step_t *step);

ucs_status_t ucg_builtin_am_handler(void *worker, void *data, size_t length,
                                    unsigned am_flags);

ucs_status_t ucg_builtin_op_create (ucg_plan_t *plan,
                                    const ucg_collective_params_t *params,
                                    ucg_op_t **op);

void         ucg_builtin_op_discard(ucg_op_t *op);

ucs_status_t ucg_builtin_op_trigger(ucg_op_t *op,
                                    ucg_coll_id_t coll_id,
                                    ucg_request_t *request);


/* Callback functions exported for debugging */
void ucg_builtin_print_init_cb_name(ucg_builtin_op_init_cb_t init_cb);

void ucg_builtin_print_fini_cb_name(ucg_builtin_op_fini_cb_t fini_cb);

void ucg_builtin_print_pack_cb_name(uct_pack_callback_t pack_single_cb);

void ucg_builtin_print_flags(ucg_builtin_op_step_t *step);

/*
 * Macros to generate the headers of all bcopy packing callback functions.
 */
typedef ssize_t (*packed_send_t)(uct_ep_h, uint8_t, uct_pack_callback_t, void*, unsigned);

#define UCG_BUILTIN_PACKER_NAME(_modifier, _mode) \
    ucg_builtin_step_am_bcopy_pack ## _modifier ## _mode

#define UCG_BUILTIN_PACKER_DECLARE(_modifier, _mode) \
    size_t UCG_BUILTIN_PACKER_NAME(_modifier, _mode) (void *dest, void *arg)

UCG_BUILTIN_PACKER_DECLARE(_, single);
UCG_BUILTIN_PACKER_DECLARE(_, full);
UCG_BUILTIN_PACKER_DECLARE(_, part);
UCG_BUILTIN_PACKER_DECLARE(_reducing_, single);
UCG_BUILTIN_PACKER_DECLARE(_reducing_, full);
UCG_BUILTIN_PACKER_DECLARE(_reducing_, part);
UCG_BUILTIN_PACKER_DECLARE(_variadic_, single);
UCG_BUILTIN_PACKER_DECLARE(_variadic_, full);
UCG_BUILTIN_PACKER_DECLARE(_variadic_, part);
UCG_BUILTIN_PACKER_DECLARE(_atomic_single_, 8);
UCG_BUILTIN_PACKER_DECLARE(_atomic_single_, 16);
UCG_BUILTIN_PACKER_DECLARE(_atomic_single_, 32);
UCG_BUILTIN_PACKER_DECLARE(_atomic_single_, 64);
UCG_BUILTIN_PACKER_DECLARE(_atomic_multiple_, 8);
UCG_BUILTIN_PACKER_DECLARE(_atomic_multiple_, 16);
UCG_BUILTIN_PACKER_DECLARE(_atomic_multiple_, 32);
UCG_BUILTIN_PACKER_DECLARE(_atomic_multiple_, 64);

static UCS_F_ALWAYS_INLINE void
ucg_builtin_step_get_local_address(ucg_builtin_op_step_t *step, int var_stride,
                                   uint8_t **buffer, size_t *length)
{
    if (var_stride) {
        size_t tmp_length = step->buffer_length;
        *buffer           = step->send_buffer +
                            step->var_displs[step->iter_offset] * tmp_length;
        *length           = step->var_counts[step->iter_offset] * tmp_length;
    } else {
        *length           = step->buffer_length;
        *buffer           = step->send_buffer + step->iter_offset;
    }
}

static UCS_F_ALWAYS_INLINE void
ucg_builtin_step_set_remote_address(ucg_builtin_op_step_t *step, uint8_t **ptr)
{
    uint8_t *sent    = step->send_buffer +
                      (step->iter_offset * step->buffer_length);
    *(uint64_t*)sent = (uint64_t)*ptr;
    *ptr             = sent;

    ucs_assert(step->iter_offset < step->ep_cnt);
    ucs_assert(step->buffer_length == (sizeof(uint64_t) +
                                       step->phase->md_attr->rkey_packed_size));
}

/*
 * This number sets the number of slots available for collective operations.
 * Each operation occupies a slot, so no more than this number of collectives
 * can take place at the same time. The slot is determined by the collective
 * operation id (ucg_coll_id_t) - modulo this constant. Translating "coll_id"
 * to slot# happens on every incoming packet, so this constant is best kept
 * determinable at compile time, and set to a power of 2 (<= 64, to fit into
 * the resend bit-field).
 */
#define UCG_BUILTIN_MAX_CONCURRENT_OPS (16)

END_C_DECLS

#endif
