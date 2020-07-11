/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include "ucg_plan.h"
#include "ucg_group.h"
#include "ucg_context.h"

#include <ucs/debug/debug.h>
#include <ucg/api/ucg_version.h>

#define UCG_CONFIG_ALL "all"

extern ucs_config_field_t ucp_config_table[];
ucs_config_field_t ucg_config_table[] = {
  {"UCP_", "", NULL,
   ucs_offsetof(ucg_config_t, super), UCS_CONFIG_TYPE_TABLE(ucp_config_table)},

  {"PLANNERS", UCG_CONFIG_ALL,
   "Specifies which collective planner(s) to use. The order is not meaningful.\n"
   "\"all\" would use all available planners.",
   ucs_offsetof(ucg_context_config_t, planners), UCS_CONFIG_TYPE_STRING_ARRAY},

  {NULL}
};

UCS_CONFIG_REGISTER_TABLE(ucg_config_table, "UCG context", NULL, ucg_config_t)

#define ucg_context_planners_foreach(_ctx) \
    unsigned idx; \
    ucg_plan_desc_t* planner; \
    ucg_plan_component_t* planc; \
    for (idx = 0, planner = (_ctx)->planners, planc = planner->plan_component; \
         idx < (_ctx)->num_planners; \
         idx++, planner++, planc = planner->plan_component)

static ucs_status_t ucg_context_set_am_handler(ucg_plan_ctx_h plan_ctx,
                                               unsigned assigned_am_id,
                                               uct_am_callback_t am_cb,
                                               uct_am_tracer_t am_tracer)
{
#ifdef HAVE_UCP_EXTENSIONS
    /*
     * Set the Active Message handler (before creating the UCT interfaces)
     *
     * @note: The reason this is handled as early as query, and not during init
     * time, is that other processes may finish init before others start. This
     * makes for a race condition, potentially causing Active Messages to arrive
     * before their handler is registered. Registering the Active Message ID now
     * guarantees once init is finished on any of the processes - the others are
     * aware of this ID and messages can be sent.
     */
    ucp_am_handler_t* am_handler     = ucp_am_handlers + assigned_am_id;
    am_handler->features             = UCP_FEATURE_GROUPS;
    am_handler->cb                   = am_cb;
    am_handler->tracer               = (ucp_am_tracer_t)am_tracer;
    am_handler->flags                = UCT_CB_FLAG_ALT_ARG;
    am_handler->alt_arg              = plan_ctx;
#else
    for (i = 0; i < worker->num_ifaces; i++) {
        ucs_status_t status = uct_iface_set_am_handler(worker->ifaces[i]->iface,
                                                       assigned_am_id,
                                                       am_cb, worker, 0);
        if (status != UCS_OK) {
            return status;
        }
    }
    /* TODO: need some synchronization to avoid AM_ID race condition */
#endif

    return UCS_OK;
}

static ucs_status_t ucg_context_init(void *groups_ctx)
{
    uct_am_callback_t am_cb;
    uct_am_tracer_t am_tracer;

    unsigned am_id  = UCP_AM_ID_LAST;
    ucg_context_t *ctx  = (ucg_context_t*)groups_ctx;
    ucs_status_t status = ucg_plan_query(&ctx->planners, &ctx->num_planners);
    if (status != UCS_OK) {
        return status;
    }

    ucg_plan_ctx_h plan_ctx = ctx + 1;
    ucg_context_planners_foreach(ctx) {
        status = planc->init(plan_ctx, am_id, &am_cb, &am_tracer);
        if (status != UCS_OK) {
            ucs_warn("failed to initialize planner %s: %m", planner->plan_name);
            continue;
        }

        status = ucg_context_set_am_handler(plan_ctx, am_id++, am_cb, am_tracer);
        if (status != UCS_OK) {
            ucs_error("failed to set AM handler for #%u: %m", am_id);
            continue;
        }

        plan_ctx = UCS_PTR_BYTE_OFFSET(plan_ctx, planc->global_ctx_size);
    }

    ctx->next_group_id       = 0;
    ctx->iface_cnt           = 0;
    ctx->total_planner_sizes = UCS_PTR_BYTE_DIFF(plan_ctx, ctx + 1);

    ucs_list_head_init(&ctx->groups_head);
    return UCS_OK;
}

static void ucg_context_cleanup(void *groups_ctx)
{
    ucg_context_t *ctx = (ucg_context_t*)groups_ctx;

    ucg_group_h group, tmp;
    if (!ucs_list_is_empty(&ctx->groups_head)) {
        ucs_list_for_each_safe(group, tmp, &ctx->groups_head, list) {
            ucg_group_destroy(group);
        }
    }

    ucg_plan_release_list(ctx->planners, ctx->num_planners);
}

static void ucg_context_copy_used_ucp_params(ucp_params_t *dst,
                                             const ucp_params_t *src)
{
    size_t ucp_params_size = sizeof(src->field_mask);

    if (src->field_mask != 0) {
        enum ucp_params_field msb_flag = UCS_BIT((sizeof(msb_flag) * 8) - 1 -
                ucs_count_leading_zero_bits(src->field_mask));

        switch (msb_flag) {
        case UCP_PARAM_FIELD_FEATURES:
            ucp_params_size = ucs_offsetof(ucp_params_t, request_size);
            break;

        case UCP_PARAM_FIELD_REQUEST_SIZE:
            ucp_params_size = ucs_offsetof(ucp_params_t, request_init);
            break;

        case UCP_PARAM_FIELD_REQUEST_INIT:
            ucp_params_size = ucs_offsetof(ucp_params_t, request_cleanup);
            break;

        case UCP_PARAM_FIELD_REQUEST_CLEANUP:
            ucp_params_size = ucs_offsetof(ucp_params_t, tag_sender_mask);
            break;

        case UCP_PARAM_FIELD_TAG_SENDER_MASK:
            ucp_params_size = ucs_offsetof(ucp_params_t, mt_workers_shared);
            break;

        case UCP_PARAM_FIELD_MT_WORKERS_SHARED:
            ucp_params_size = ucs_offsetof(ucp_params_t, estimated_num_eps);
            break;

        case UCP_PARAM_FIELD_ESTIMATED_NUM_EPS:
            ucp_params_size = ucs_offsetof(ucp_params_t, estimated_num_ppn);
            break;

        case UCP_PARAM_FIELD_ESTIMATED_NUM_PPN:
            ucp_params_size = ucs_offsetof(ucp_params_t, context_headroom);
            break;

        case UCP_PARAM_FIELD_CONTEXT_HEADROOM:
            ucp_params_size = ucs_offsetof(ucp_params_t, num_local_peers);
            break;

        case UCP_PARAM_FIELD_LOCAL_PEER_INFO:
            ucp_params_size = sizeof(ucp_params_t);
            break;
        }
    }

    memcpy(dst, src, ucp_params_size);
}

static void ucg_context_copy_used_ucg_params(ucg_params_t *dst,
                                             const ucg_params_t *src)
{
    size_t ucg_params_size = sizeof(src->field_mask) +
                             ucs_offsetof(ucg_params_t, field_mask);

    if (src->field_mask != 0) {
        enum ucg_params_field msb_flag = UCS_BIT((sizeof(msb_flag) * 8) - 1 -
                ucs_count_leading_zero_bits(src->field_mask));

        switch (msb_flag) {
        case UCG_PARAM_FIELD_ADDRESS_CB:
            ucg_params_size = ucs_offsetof(ucg_params_t, neighbors);
            break;

        case UCG_PARAM_FIELD_NEIGHBORS_CB:
            ucg_params_size = ucs_offsetof(ucg_params_t, mpi_reduce_f);
            break;

        case UCG_PARAM_FIELD_REDUCE_CB:
            ucg_params_size = ucs_offsetof(ucg_params_t, type_info);
            break;

        case UCG_PARAM_FIELD_TYPE_INFO_CB:
            ucg_params_size = ucs_offsetof(ucg_params_t, mpi_in_place);
            break;

        case UCG_PARAM_FIELD_MPI_IN_PLACE:
            ucg_params_size = ucs_offsetof(ucg_params_t, fault);
            break;

        case UCG_PARAM_FIELD_HANDLE_FAULT:
            ucg_params_size = sizeof(ucg_params_t);
            break;
        }
    }

    memcpy(dst, src, ucg_params_size);
}

ucg_params_t ucg_params; /* Ugly - but efficient */

ucs_status_t ucg_init_version(unsigned ucg_api_major_version,
                              unsigned ucg_api_minor_version,
                              unsigned ucp_api_major_version,
                              unsigned ucp_api_minor_version,
                              const ucg_params_t *params,
                              const ucg_config_t *config,
                              ucg_context_h *context_p)
{
    ucs_status_t status;
    unsigned major_version, minor_version, release_number;

    ucg_get_version(&major_version, &minor_version, &release_number);

    if ((ucg_api_major_version != major_version) ||
        ((ucg_api_major_version == major_version) &&
         (ucg_api_minor_version > minor_version))) {
        ucs_debug_address_info_t addr_info;
        status = ucs_debug_lookup_address(ucg_init_version, &addr_info);
        ucs_warn("UCG version is incompatible, required: %d.%d, actual: %d.%d "
                 "(release %d %s)",
                 ucg_api_major_version, ucg_api_minor_version,
                 major_version, minor_version, release_number,
                 status == UCS_OK ? addr_info.file.path : "");
    }

    ucg_config_t *dfl_config = NULL;
    if (config == NULL) {
        status = ucg_config_read(NULL, NULL, &dfl_config);
        if (status != UCS_OK) {
            goto err;
        }
        config = dfl_config;
    }

    /* Store the UCG params in a global location, for easy access */
    ucg_context_copy_used_ucg_params(&ucg_params, params);

    /* Avoid overwriting the headroom value by copying all UCP params aside */
    ucp_params_t ucp_params;
    ucg_context_copy_used_ucp_params(&ucp_params, params->super);

    if ((params->super->field_mask & UCP_PARAM_FIELD_CONTEXT_HEADROOM) == 0) {
        ucp_params.context_headroom = 0;
    }

    ucp_params.field_mask       |= UCP_PARAM_FIELD_CONTEXT_HEADROOM;
    ucp_params.context_headroom += ucs_offsetof(ucg_context_t, ucp_ctx);
    ucg_params.super             = NULL; /* Should never be accessed again */

    /* Create the UCP context, which should have room for UCG in its headroom */
    status = ucp_init_version(ucp_api_major_version, ucp_api_minor_version,
                              &ucp_params, &config->super,
                              (ucp_context_h*)context_p);
    if (status != UCS_OK) {
        goto err_config;
    }

    *context_p = ucs_container_of(*context_p, ucg_context_t, ucp_ctx);
    status     = ucg_context_init(*context_p);
    if (status != UCS_OK) {
        goto err_context;
    }

    return UCS_OK;

err_context:
    ucp_cleanup(&(*context_p)->ucp_ctx);
err_config:
    if (dfl_config != NULL) {
        ucg_config_release(dfl_config);
    }
err:
    return status;
}

ucs_status_t ucg_init(const ucg_params_t *params,
                      const ucg_config_t *config,
                      ucg_context_h *context_p)
{
    ucs_status_t status = ucg_init_version(UCG_API_MAJOR, UCG_API_MINOR,
                                           UCP_API_MAJOR, UCP_API_MINOR,
                                           params, config, context_p);

    if (status == UCS_OK) {
        (*context_p)->address.lookup_f  = params->address.lookup_f;
        (*context_p)->address.release_f = params->address.release_f;
    }

    return status;
}

void ucg_cleanup(ucg_context_h context)
{
    ucg_context_cleanup(context);
    ucp_cleanup(&context->ucp_ctx);
}

ucp_context_h ucg_context_get_ucp(ucg_context_h context)
{
    return &context->ucp_ctx;
}

void ucg_context_print_info(const ucg_context_h context, FILE *stream)
{
    fprintf(stream, "#\n");
    fprintf(stream, "# UCG context\n");
    fprintf(stream, "#\n");

    ucg_context_planners_foreach(context) {
        fprintf(stream, "#     planner %-2d :  %s\n", idx, planc->name);
    }
    fprintf(stream, "#\n");

    ucp_context_print_info(&context->ucp_ctx, stream);
}

ucs_status_t ucg_config_read(const char *env_prefix, const char *filename,
                             ucg_config_t **config_p)
{
    unsigned full_prefix_len = sizeof(UCS_DEFAULT_ENV_PREFIX) + 1;
    unsigned env_prefix_len  = 0;
    ucg_config_t *config;
    ucs_status_t status;

    config = ucs_malloc(sizeof(*config), "ucg config");
    if (config == NULL) {
        status = UCS_ERR_NO_MEMORY;
        goto err;
    }

    if (env_prefix != NULL) {
        env_prefix_len   = strlen(env_prefix);
        full_prefix_len += env_prefix_len;
    }

    config->super.env_prefix = ucs_malloc(full_prefix_len, "ucg config");
    if (config->super.env_prefix == NULL) {
        status = UCS_ERR_NO_MEMORY;
        goto err_free_config;
    }

    if (env_prefix_len != 0) {
        ucs_snprintf_zero(config->super.env_prefix, full_prefix_len, "%s_%s",
                          env_prefix, UCS_DEFAULT_ENV_PREFIX);
    } else {
        ucs_snprintf_zero(config->super.env_prefix, full_prefix_len, "%s",
                          UCS_DEFAULT_ENV_PREFIX);
    }

    status = ucs_config_parser_fill_opts(config, ucg_config_table,
                                         config->super.env_prefix, NULL, 0);
    if (status != UCS_OK) {
        goto err_free_prefix;
    }

    *config_p = config;
    return UCS_OK;

err_free_prefix:
    ucs_free(config->super.env_prefix);
err_free_config:
    ucs_free(config);
err:
    return status;
}

void ucg_config_release(ucg_config_t *config)
{
    ucs_config_parser_release_opts(config, ucg_config_table);
    ucs_free(config->super.env_prefix);
    ucs_free(config);
}

ucs_status_t ucg_config_modify(ucg_config_t *config, const char *name,
                               const char *value)
{
    return ucs_config_parser_set_value(config, ucg_config_table, name, value);
}

void ucg_config_print(const ucg_config_t *config, FILE *stream,
                      const char *title, ucs_config_print_flags_t print_flags)
{
    ucs_config_parser_print_opts(stream, title, config, ucg_config_table,
                                 NULL, UCS_DEFAULT_ENV_PREFIX, print_flags);
}
