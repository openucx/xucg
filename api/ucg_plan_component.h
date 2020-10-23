/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_PLAN_COMPONENT_H_
#define UCG_PLAN_COMPONENT_H_

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <string.h>

#include <ucg/api/ucg.h>
#include <uct/api/uct.h>
#include <ucs/config/parser.h>
#include <ucs/sys/compiler_def.h>
#include <ucs/datastruct/mpool.h>
#include <ucs/datastruct/queue_types.h>
#include <ucs/datastruct/list.h>
#include <ucs/type/spinlock.h>

BEGIN_C_DECLS

#define UCG_MAX_IFACES (7) /* Why 7? to fit into a cache-line with a counter */

typedef uint8_t                   ucg_coll_id_t;  /* cyclic */
typedef uint8_t                   ucg_step_idx_t;
typedef uint32_t                  ucg_offset_t;
typedef void*                     ucg_plan_ctx_h;
typedef void*                     ucg_group_ctx_h;
typedef struct ucg_plan_component ucg_plan_component_t;

extern ucs_list_link_t ucg_plan_components_list;
extern ucg_params_t ucg_global_params;

/**
 * @ingroup UCG_RESOURCE
 * @brief Collectives' estimation of latency.
 *
 * This structure describes optional information which could be used
 * to select the best planner for a given collective operation..
 */
typedef struct ucg_plan_plogp_params {
    /* overhead time - per message and per byte, in seconds */
    struct {
        double sec_per_message;
        double sec_per_byte;
    } send, recv, gap;

    /* p2p latency, in seconds, by distance (assumes uniform network) */
    double latency_in_sec[UCG_GROUP_MEMBER_DISTANCE_LAST];

    /* number of peers on each level */
    ucg_group_member_index_t peer_count[UCG_GROUP_MEMBER_DISTANCE_LAST];
} ucg_plan_plogp_params_t;

typedef double (*ucg_plan_estimator_f)(ucg_plan_plogp_params_t plogp,
                                       ucg_collective_params_t *coll);

/**
 * @ingroup UCG_RESOURCE
 * @brief Collective planning resource descriptor.
 *
 * This structure describes a collective operation planning resource.
 */
enum ucg_plan_flags {
    UCG_PLAN_FLAG_PLOGP_LATENCY_ESTIMATOR = 0, /*< Supports PlogP latency estimation */
    UCG_PLAN_FLAG_FAULT_TOLERANCE_SUPPORT = 1, /*< Supported custom fault tolerance */
};

/**
 * @ingroup UCG_RESOURCE
 * @brief Collective planning resource descriptor.
 *
 * This structure describes a collective operation planning resource.
 */
#define UCG_PLAN_COMPONENT_NAME_MAX (16)
typedef struct ucg_plan_desc {
    char                  name[UCG_PLAN_COMPONENT_NAME_MAX]; /**< Name */
    ucg_plan_component_t *component;            /*< Component object */
    unsigned              modifiers_supported;       /*< @ref enum ucg_collective_modifiers */
    unsigned              flags;                     /*< @ref enum ucg_plan_flags */

    /* Optional parameters - depending on flags */
    ucg_plan_estimator_f  latency_estimator;         /*< @ref ucg_plan_estimator_f */
    unsigned              fault_tolerance_supported; /*< @ref enum ucg_fault_tolerance_mode */
} ucg_plan_desc_t;

/**
 * "Base" structure which defines planning configuration options.
 * Specific planning components extend this structure.
 *
 * Note: which components are actualy enabled/used is a configuration for UCG.
 */
typedef struct ucg_plan_config {
    uint8_t *am_id;  /**< Active-message ID dispenser */
} ucg_plan_config_t;

typedef struct ucg_plan {
    ucs_recursive_spinlock_t lock;
    ucs_list_link_t          op_head;   /**< List of requests following this plan */

    /* Plan progress */
    ucg_plan_desc_t         *planner;
    ucg_group_id_t           group_id;
    ucg_group_member_index_t group_size;
    ucg_group_member_index_t my_index;
    ucg_group_h              group;
    char                     priv[0];
} ucg_plan_t;

typedef struct ucg_op ucg_op_t;
typedef ucs_status_t (*ucg_op_trigger_f)(ucg_op_t *op,
                                         ucg_coll_id_t coll_id,
                                         ucg_request_t *request);

typedef void         (*ucg_op_discard_f)(ucg_op_t *op);
struct ucg_op {
    /* Collective-specific request content */
    ucg_op_trigger_f         trigger_f;   /**< shortcut for the trigger call */
    ucg_op_discard_f         discard_f;   /**< shortcut for the discard call */

    union {
        ucs_list_link_t      list;        /**< cache list member */
        struct {
            ucs_queue_elem_t queue;       /**< pending queue member */
            ucg_request_t   *pending_req; /**< original invocation request */
        };
    };

    ucg_plan_t              *plan;        /**< The group this belongs to */

    ucg_collective_params_t  params;      /**< original parameters for it */
    /* Note: the params field must be 64-byte-aligned */

    /* Component-specific request content */
    char                     priv[0];
};

struct ucg_plan_component {
    const char                     name[UCG_PLAN_COMPONENT_NAME_MAX];  /**< Component name */
    ucs_config_global_list_entry_t config;             /**< Configuration information */
    size_t                         global_ctx_size;    /**< Size to be allocated once, for context */
    size_t                         per_group_ctx_size; /**< Size to be allocated within each group */
    ucs_list_link_t                list;               /**< Entry in global list of components */

    /* test for support and other attribures of this component */
    ucs_status_t           (*query)   (ucg_plan_desc_t *descs,
                                       unsigned *desc_cnt_p);
    ucs_status_t           (*init)    (ucg_plan_ctx_h ctx,
                                       ucg_plan_config_t *config);
    void                   (*finalize)(ucg_plan_ctx_h ctx);

    /* create a new planner context for a group */
    ucs_status_t           (*create)  (ucg_plan_ctx_h ctx,
                                       ucg_group_ctx_h gctx,
                                       ucg_group_h group,
                                       const ucg_group_params_t *group_params);
    /* destroy a group context, along with all its operations and requests */
    void                   (*destroy) (ucg_group_ctx_h gctx);
    /* check a group context for progress */
    unsigned               (*progress)(ucg_group_ctx_h gctx);

    /* plan a collective operation with this component */
    ucs_status_t           (*plan)    (ucg_group_ctx_h gctx,
                                       const ucg_collective_type_t *coll_type,
                                       ucg_plan_t **plan_p);
    /* Prepare an operation to follow the given plan */
    ucs_status_t           (*prepare) (ucg_plan_t *plan,
                                       const ucg_collective_params_t *coll_params,
                                       ucg_op_t **op);
    /* Trigger an operation to start, generate a request handle for updates */
    ucg_op_trigger_f         trigger;

    /* Discard an operation previously prepared */
    ucg_op_discard_f         discard;

    /* print a plan object, for debugging purposes */
    void                   (*print)   (ucg_plan_t *plan,
                                       const ucg_collective_params_t *coll_params);

    /* Handle a fault in a given index of the  */
    ucs_status_t           (*fault)   (ucg_group_ctx_h gctx,
                                       ucg_group_member_index_t index);
};


/**
 * Define a planning component.
 *
 * @param _planc       Planning component structure to initialize.
 * @param _name        Planning component name.
 * @param _global_size Size of the global context allocated for this planner.
 * @param _group_size  Size of the per-group context allocated for this planner.
 * @param _query       Function to query planning resources.
 * @param _create      Function to create the component context.
 * @param _destroy     Function to destroy the component context.
 * @param _progress    Function to progress operations by this component.
 * @param _plan        Function to create a plan for future operations.
 * @param _prepare     Function to prepare an operation according to a plan.
 * @param _trigger     Function to start a prepared collective operation.
 * @param _discard     Function to release an operation and related objects.
 * @param _print       Function to output information useful for developers.
 * @param _cfg_prefix  Prefix for configuration environment variables.
 * @param _cfg_table   Defines the planning component's configuration values.
 * @param _cfg_struct  Planning component configuration structure.
 */
#define UCG_PLAN_COMPONENT_DEFINE(_planc, _name, _global_size, _group_size, \
                                  _query, _init, _finalize, _create, _destroy, \
                                  _progress, _plan, _prepare, _trigger, \
                                  _discard, _print, _fault, _cfg_prefix, \
                                  _cfg_table, _cfg_struct) \
    ucg_plan_component_t _planc = { \
        .name               = _name, \
        .config.name        = _name" planner",\
        .config.prefix      = _cfg_prefix, \
        .config.table       = _cfg_table, \
        .config.size        = sizeof(_cfg_struct), \
        .global_ctx_size    = _global_size, \
        .per_group_ctx_size = _group_size, \
        .query              = _query, \
        .init               = _init, \
        .finalize           = _finalize, \
        .create             = _create, \
        .destroy            = _destroy, \
        .progress           = _progress, \
        .plan               = _plan, \
        .prepare            = _prepare, \
        .trigger            = _trigger, \
        .discard            = _discard, \
        .print              = _print, \
        .fault              = _fault \
    }; \
    UCS_STATIC_INIT { \
        ucs_list_add_tail(&ucg_plan_components_list, &(_planc).list); \
    } \
    UCS_CONFIG_REGISTER_TABLE_ENTRY(&(_planc).config)

/* Helper function to generate a simple planner description */
ucs_status_t ucg_plan_single(ucg_plan_component_t *component,
                             ucg_plan_desc_t *descs,
                             unsigned *desc_cnt_p);

enum ucg_plan_connect_flags {
    UCG_PLAN_CONNECT_FLAG_WANT_INCAST    = UCS_BIT(0), /* want transport with incast */
    UCG_PLAN_CONNECT_FLAG_WANT_BCAST     = UCS_BIT(1), /* want transport with bcast */
    UCG_PLAN_CONNECT_FLAG_WANT_INTERNODE = UCS_BIT(2), /* want transport between hosts */
    UCG_PLAN_CONNECT_FLAG_WANT_INTRANODE = UCS_BIT(3)  /* want transport within a host */
};

/* Helper function for connecting to other group members - by their index */
typedef ucs_status_t (*ucg_plan_reg_handler_cb)(uct_iface_h iface, void *arg);
ucs_status_t ucg_plan_connect(ucg_group_h group,
                              ucg_group_member_index_t idx,
                              enum ucg_plan_connect_flags flags,
                              uct_ep_h *ep_p, const uct_iface_attr_t **ep_attr_p,
                              uct_md_h *md_p, const uct_md_attr_t    **md_attr_p);

/* Helper function for selecting other planners - to be used as fall-back */
ucs_status_t ucg_plan_choose(const ucg_collective_params_t *coll_params,
                             ucg_group_h group, ucg_plan_desc_t **desc_p,
                             ucg_group_ctx_h *gctx_p);

/* This combination of flags and structure provide additional info for planning */
enum ucg_plan_resource_flags {
    UCG_PLAN_RESOURCE_FLAG_REDUCTION_ACCELERATOR = UCS_BIT(0),
};

typedef struct ucg_plan_resources {
    uint64_t flags; /* @ref enum ucg_plan_resource_flags */
    struct {
        uint8_t operand_size_supported; /**< bitmask where i-th bit represents
                                             support for operand size 2**i */
        uint8_t reserved[7]; /**< TBD */
    } reduction_accelerator;

} ucg_plan_resources_t;

/* Helper function to obtain the worker which the given group is using */
ucp_worker_h ucg_plan_get_group_worker(ucg_group_h group);

/* Helper function for detecting the group's (network-related) resources */
ucs_status_t ucg_plan_query_resources(ucg_group_h group,
                                      ucg_plan_resources_t **resources);

/* Helper function for registering Active-Message handlers */
ucs_status_t ucg_context_set_am_handler(ucg_plan_ctx_h plan_ctx,
                                        uint8_t id,
                                        uct_am_callback_t cb,
                                        uct_am_tracer_t tracer);

/* Start/stop pending operations after a barrier has been completed */
ucs_status_t ucg_collective_acquire_barrier(ucg_group_h group);
ucs_status_t ucg_collective_release_barrier(ucg_group_h group);

/* Helper functions for periodic checks for faults on a remote group member */
typedef struct ucg_ft_handle* ucg_ft_h;
ucs_status_t ucg_ft_start(ucg_group_h group,
                          ucg_group_member_index_t index,
                          uct_ep_h optional_ep,
                          ucg_ft_h *handle_p);
ucs_status_t ucg_ft_end(ucg_ft_h handle,
                        ucg_group_member_index_t index);
ucs_status_t ucg_ft_propagate(ucg_group_h group,
                              const ucg_group_params_t *params,
                              uct_ep_h new_ep);


#ifndef UCS_ALLOC_CHECK
#define UCS_ALLOC_CHECK(size, name) ({ \
    void* ptr = ucs_malloc(size, name); \
    if (ptr == 0) return UCS_ERR_NO_MEMORY; \
    ptr; \
})
#endif

END_C_DECLS

#endif
