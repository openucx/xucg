/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "ucg_plan.h"
#include "ucg_group.h"
#include "ucg_context.h"

#include <ucg/api/ucg.h>
#include <ucp/core/ucp_types.h>
#include <ucp/core/ucp_ep.inl>
#include <ucp/core/ucp_ep.inl>
#include <ucp/core/ucp_proxy_ep.h>
#include <uct/base/uct_component.h>
#include <ucs/config/parser.h>
#include <ucs/debug/log.h>
#include <ucs/debug/assert.h>
#include <ucs/debug/memtrack.h>
#include <ucs/type/class.h>
#include <ucs/sys/module.h>
#include <ucs/sys/string.h>
#include <ucs/arch/cpu.h>

UCS_LIST_HEAD(ucg_plan_components_list);

__KHASH_IMPL(ucg_group_ep, static UCS_F_MAYBE_UNUSED inline,
             ucg_group_member_index_t, ucp_ep_h, 1, kh_int64_hash_func,
             kh_int64_hash_equal);

#define UCG_GROUP_PROGRESS_ADD(iface, ctx) { \
    unsigned _idx = 0; \
    if (ucs_unlikely(_idx == UCG_MAX_IFACES)) { \
        return UCS_ERR_EXCEEDS_LIMIT; \
    } \
    while (_idx < (ctx)->iface_cnt) { \
        if ((ctx)->ifaces[_idx] == (iface)) { \
            break; \
        } \
        _idx++; \
    } \
    if (_idx == (ctx)->iface_cnt) { \
        (ctx)->ifaces[(ctx)->iface_cnt++] = (iface); \
    } \
}

#define ucg_plan_foreach(_descs, _desc_cnt, _plan_ctx, _grp_ctx) \
    typeof(_desc_cnt) idx = 0; \
    ucg_plan_component_t* comp; \
    ucs_assert((_desc_cnt) > 0); \
    for (comp = (_descs)->component; \
         idx < (_desc_cnt); \
         idx++, (_descs)++, \
         (_plan_ctx) = UCS_PTR_BYTE_OFFSET((_plan_ctx), comp->global_ctx_size), \
         (_grp_ctx) = UCS_PTR_BYTE_OFFSET((_grp_ctx), comp->per_group_ctx_size), \
         comp = (idx < (_desc_cnt)) ? (_descs)->component : NULL)

#define ucg_group_foreach(_group) \
    ucg_plan_desc_t *descs = (_group)->context->planners; \
    unsigned desc_cnt      = (_group)->context->num_planners; \
    ucg_plan_ctx_h pctx    = (_group)->context->planners_ctx; \
    ucg_group_ctx_h gctx   = (_group) + 1; \
    ucg_plan_foreach(descs, desc_cnt, pctx, gctx)

static ucs_status_t ucg_plan_config_read(ucg_plan_component_t *component,
                                         const char *env_prefix,
                                         const char *filename,
                                         ucg_plan_config_t **config_p)
{
    uct_config_bundle_t *bundle = NULL;

    ucs_status_t status = uct_config_read(&bundle, component->config.table,
                                          component->config.size, env_prefix,
                                          component->config.prefix);
    if (status != UCS_OK) {
        ucs_error("failed to read CM configuration");
        return status;
    }

    *config_p = (ucg_plan_config_t*) bundle->data;
    /* coverity[leaked_storage] */
    return UCS_OK;
}

ucs_status_t ucg_plan_query(ucg_plan_desc_t **desc_p, unsigned *num_desc_p,
                            size_t *total_plan_ctx_size)
{
    UCS_MODULE_FRAMEWORK_DECLARE(ucg);
    UCS_MODULE_FRAMEWORK_LOAD(ucg, 0);

    /* Calculate how many descriptors to allocate */
    ucg_plan_component_t *component;
    unsigned i, desc_cnt, desc_total = 0;
    ucs_list_for_each(component, &ucg_plan_components_list, list) {
        ucs_status_t status = component->query(NULL, &desc_cnt);
        if (status != UCS_OK) {
            ucs_warn("Failed to query planner %s (for size): %m",
                     component->name);
            continue;
        }

        desc_total += desc_cnt;
    }

    /* Allocate the descriptors */
    size_t size                = desc_total * sizeof(ucg_plan_desc_t);
    ucg_plan_desc_t *desc_iter = *desc_p = UCS_ALLOC_CHECK(size, "ucg descs");

    size = 0;
    ucs_list_for_each(component, &ucg_plan_components_list, list) {
        ucs_status_t status = component->query(desc_iter, &desc_cnt);
        if (status != UCS_OK) {
            ucs_warn("Failed to query planner %s (for content): %m",
                     component->name);
            continue;
        }

        for (i = 0; i < desc_cnt; ++i) {
            size += desc_iter[i].component->global_ctx_size;

            ucs_assertv_always(!strncmp(component->name, desc_iter[i].name,
                                        strlen(component->name)),
                               "Planner name must begin with topology component name."
                               "Planner name: %s Plan component name: %s ",
                               desc_iter[i].name, component->name);
        }

        desc_iter += desc_cnt;
    }

    *num_desc_p          = desc_iter - *desc_p;
    *total_plan_ctx_size = size;

    return UCS_OK;
}

ucs_status_t ucg_plan_init(ucg_plan_desc_t *descs, unsigned desc_cnt,
                           ucg_plan_ctx_h plan, size_t *per_group_ctx_size)
{
    ucg_plan_config_t *plan_config;

    uint8_t am_id     = UCP_AM_ID_LAST;
    void *dummy_group = NULL;
    size_t total_size = 0;

    ucg_plan_foreach(descs, desc_cnt, plan, dummy_group) {
        ucs_status_t status = ucg_plan_config_read(comp, NULL, NULL, &plan_config);
        if (status != UCS_OK) {
            continue;
        }

#ifndef HAVE_UCP_EXTENSIONS
        /*
         * Find an unused AM_ID between 0 and UCP_AM_ID_LAST, because UCP will
         * disregard any value above that (since UCP_AM_ID_MAX isn't there).
         */
        if (am_id == UCP_AM_ID_LAST) {
            am_id = 1; /* AM ID #0 would complicate debugging */
        }

        while (ucp_am_handlers[am_id].cb != NULL) {
            am_id++;
        }

        ucs_assert_always(am_id < UCP_AM_ID_LAST);
#endif

        plan_config->am_id = &am_id;

        status = comp->init(plan, plan_config);

        uct_config_release(plan_config);

        if (status != UCS_OK) {
            ucs_warn("failed to initialize planner %s: %m", descs->name);
            continue;
        }

        total_size += comp->per_group_ctx_size;
    }

#if ENABLE_FAULT_TOLERANCE
    /* Initialize the fault-tolerance context for the entire UCG layer */
    status = ucg_ft_init(&worker->async, new_group, ucg_base_am_id + idx,
                         ucg_group_fault_cb, ctx, &ctx->ft_ctx);
    if (status != UCS_OK) {
        goto cleanup_pctx;
    }
#endif

    *per_group_ctx_size = total_size;

    return UCS_OK;
}

void ucg_plan_finalize(ucg_plan_desc_t *descs, unsigned desc_cnt,
                       ucg_plan_ctx_h plan)
{

#if ENABLE_FAULT_TOLERANCE
    if (ucs_list_is_empty(&group->list)) {
        ucg_ft_cleanup(&gctx->ft_ctx);
    }
#endif

    void *dummy_group = NULL;
    ucg_plan_foreach(descs, desc_cnt, plan, dummy_group) {
        comp->finalize(plan);
    }
}

static ucs_status_t ucg_plan_group_setup(ucg_group_h group)
{
    UCS_V_UNUSED int kh_dummy;
    UCS_V_UNUSED size_t length;
    UCS_V_UNUSED ucp_address_t *address;

    /* Create a loopback connection, since resolve_cb may fail loopback */
    ucp_worker_h worker = group->worker;
    ucs_status_t status = ucp_worker_get_address(worker, &address, &length);
    if (status != UCS_OK) {
        return status;
    }

    ucp_ep_h loopback_ep;
    ucp_ep_params_t ep_params = {
            .field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS,
            .address    = address
    };

    status = ucp_ep_create(worker, &ep_params, &loopback_ep);
    ucp_worker_release_address(worker, address);
    if (status != UCS_OK) {
        return status;
    }

    kh_init_inplace(ucg_group_ep, &group->eps);

    /* Store this loopback endpoint, for future reference */
    ucg_group_member_index_t my_index = group->params.member_index;
    ucs_assert(kh_get(ucg_group_ep, &group->eps, my_index) == kh_end(&group->eps));
    khiter_t iter = kh_put(ucg_group_ep, &group->eps, my_index, &kh_dummy);
    kh_value(&group->eps, iter) = loopback_ep;

    return UCS_OK;
}

ucs_status_t ucg_plan_group_create(ucg_group_h group)
{
    ucs_status_t status = ucg_plan_group_setup(group);
    if (status != UCS_OK) {
        return status;
    }

    ucg_group_foreach(group) {
        /* Create the per-planner per-group context */
        status = comp->create(pctx, gctx, group, &group->params);
        if (status != UCS_OK) {
            return status;
        }
    }


    return UCS_OK;
}

unsigned ucg_plan_group_progress(ucg_group_h group)
{
    unsigned ret = 0;

    ucg_group_foreach(group) {
        ret += comp->progress(gctx);
    }

    return ret;
}

void ucg_plan_group_destroy(ucg_group_h group)
{
    ucg_group_foreach(group) {
        comp->destroy(gctx);
    }

    kh_destroy_inplace(ucg_group_ep, &group->eps);
}

void ucg_plan_print_info(ucg_plan_desc_t *descs, unsigned desc_cnt, FILE *stream)
{
    void *dummy_plan  = NULL;
    void *dummy_group = NULL;

    ucg_plan_foreach(descs, desc_cnt, dummy_plan, dummy_group) {
        fprintf(stream, "#     planner %-2d :  %s\n", idx, comp->name);
    }
}

ucs_status_t ucg_plan_single(ucg_plan_component_t *component,
                             ucg_plan_desc_t *descs,
                             unsigned *desc_cnt_p)
{
    if (descs) {
        descs->component = component;
        ucs_snprintf_zero(&descs->name[0], UCG_PLAN_COMPONENT_NAME_MAX, "%s",
                          component->name);
    }

    *desc_cnt_p = 1;

    return UCS_OK;
}

ucs_status_t ucg_plan_choose(const ucg_collective_params_t *coll_params,
                             ucg_group_h group, ucg_plan_desc_t **desc_p,
                             ucg_group_ctx_h *gctx_p)
{
    ucs_assert(group->context->num_planners == 1); // TODO: support more...

    *desc_p = group->context->planners;
    *gctx_p = group + 1;

    return UCS_OK;
}

ucs_status_t ucg_plan_connect(ucg_group_h group,
                              ucg_group_member_index_t idx,
                              enum ucg_plan_connect_flags flags,
                              uct_ep_h *ep_p, const uct_iface_attr_t **ep_attr_p,
                              uct_md_h *md_p, const uct_md_attr_t    **md_attr_p)
{
    int ret;
    ucs_status_t status;
    size_t remote_addr_len;
    ucp_address_t *remote_addr = NULL;

    /* Look-up the UCP endpoint based on the index */
    ucp_ep_h ucp_ep;
    khiter_t iter = kh_get(ucg_group_ep, &group->eps, idx);
    if (iter != kh_end(&group->eps)) {
        /* Use the cached connection */
        ucp_ep = kh_value(&group->eps, iter);
    } else {
        /* fill-in UCP connection parameters */
        status = ucg_global_params.address.lookup_f(group->params.cb_context,
                                                    idx, &remote_addr,
                                                    &remote_addr_len);
        if (status != UCS_OK) {
            ucs_error("failed to obtain a UCP endpoint from the external callback");
            return status;
        }

        /* special case: connecting to a zero-length address means it's "debugging" */
        if (ucs_unlikely(remote_addr_len == 0)) {
            *ep_p = NULL;
            return UCS_OK;
        }

        /* create an endpoint for communication with the remote member */
        ucp_ep_params_t ep_params = {
                .field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS,
                .address = remote_addr
        };
        status = ucp_ep_create(group->worker, &ep_params, &ucp_ep);
        ucg_global_params.address.release_f(group->params.cb_context, remote_addr);
        if (status != UCS_OK) {
            return status;
        }

        /* Store this endpoint, for future reference */
        iter = kh_put(ucg_group_ep, &group->eps, idx, &ret);
        kh_value(&group->eps, iter) = ucp_ep;
    }

    /* Connect for point-to-point communication */
    ucp_lane_index_t lane;
am_retry:
#ifdef HAVE_UCT_COLLECTIVES
    if (flags & UCG_PLAN_CONNECT_FLAG_WANT_INCAST) {
        lane = ucp_ep_get_incast_lane(ucp_ep);
        if (ucs_unlikely(lane == UCP_NULL_LANE)) {
            ucs_warn("No transports with native incast support were found,"
                     " falling back to P2P transports (slower)");
            return UCS_ERR_UNREACHABLE;
        }
        *ep_p = ucp_ep_get_incast_uct_ep(ucp_ep);
    } else if (flags & UCG_PLAN_CONNECT_FLAG_WANT_BCAST) {
        lane = ucp_ep_get_bcast_lane(ucp_ep);
        if (ucs_unlikely(lane == UCP_NULL_LANE)) {
            ucs_warn("No transports with native broadcast support were found,"
                     " falling back to P2P transports (slower)");
            return UCS_ERR_UNREACHABLE;
        }
        *ep_p = ucp_ep_get_bcast_uct_ep(ucp_ep);
    } else
#endif /* HAVE_UCT_COLLECTIVES */
    {
        lane  = ucp_ep_get_am_lane(ucp_ep);
        *ep_p = ucp_ep_get_am_uct_ep(ucp_ep);
    }

    if (*ep_p == NULL) {
        status = ucp_wireup_connect_remote(ucp_ep, lane);
        if (status != UCS_OK) {
            return status;
        }
        goto am_retry; /* Just to obtain the right lane */
    }

    if (ucp_proxy_ep_test(*ep_p)) {
        ucp_proxy_ep_t *proxy_ep = ucs_derived_of(*ep_p, ucp_proxy_ep_t);
        *ep_p = proxy_ep->uct_ep;
        ucs_assert(*ep_p != NULL);
    }

    ucs_assert((*ep_p)->iface != NULL);
    if ((*ep_p)->iface->ops.ep_am_short ==
            (typeof((*ep_p)->iface->ops.ep_am_short))
            ucs_empty_function_return_no_resource) {
        ucp_worker_progress(group->worker);
        goto am_retry;
    }

    /* Register interfaces to be progressed in future calls */
    ucg_context_t *ctx = group->context;
    UCG_GROUP_PROGRESS_ADD((*ep_p)->iface, ctx);
    UCG_GROUP_PROGRESS_ADD((*ep_p)->iface, group);

    *md_p      = ucp_ep_md(ucp_ep, lane);
    *md_attr_p = ucp_ep_md_attr(ucp_ep, lane);
    *ep_attr_p = ucp_ep_get_iface_attr(ucp_ep, lane);

    return UCS_OK;
}

ucp_worker_h ucg_plan_get_group_worker(ucg_group_h group)
{
    return group->worker;
}

ucs_status_t ucg_plan_query_resources(ucg_group_h group,
                                      ucg_plan_resources_t **resources)
{
    return UCS_OK;
}
