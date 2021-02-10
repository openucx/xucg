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

ucs_config_field_t ucg_config_table[] = {
  {"PLANNERS", UCG_CONFIG_ALL,
   "Specifies which collective planner(s) to use. The order is not meaningful.\n"
   "\"all\" would use all available planners.",
   ucs_offsetof(ucg_context_config_t, planners), UCS_CONFIG_TYPE_STRING_ARRAY},

  {"GROUP_OP_CACHE_SIZE", "32",
   "How many operations can be stored in the per-group cache.\n",
   ucs_offsetof(ucg_context_config_t, group_cache_size_thresh), UCS_CONFIG_TYPE_UINT},

  {"COLL_IFACE_MEMBER_THRESH", "3",
   "How many members warrant the use of collective transports.\n",
   ucs_offsetof(ucg_context_config_t, coll_iface_member_thresh), UCS_CONFIG_TYPE_UINT},

  {NULL}
};

UCS_CONFIG_REGISTER_TABLE(ucg_config_table, "UCG context", NULL, ucg_config_t
#ifndef HAVE_UCT_CONFIG_TABLE_LIST_ARG
                          )
#else
                          , &ucs_config_global_list)
#endif

#ifndef HAVE_UCP_EXTENSIONS
typedef struct ucp_workaround {
    uct_am_callback_t wrapper_cb;
    uct_am_callback_t cb;
    void             *arg;
} ucp_workaround_t;

extern ucp_workaround_t ucp_workaround_cb[];

static ucs_status_t ucg_context_worker_wrapperA_am_cb(void *arg, void *data,
                                                     size_t length,
                                                     unsigned flags)
{
    return ucp_workaround_cb[0].cb(ucp_workaround_cb[0].cb, data, length, flags);
}

static ucs_status_t ucg_context_worker_wrapperB_am_cb(void *arg, void *data,
                                                     size_t length,
                                                     unsigned flags)
{
    return ucp_workaround_cb[1].cb(ucp_workaround_cb[1].cb, data, length, flags);
}

static ucs_status_t ucg_context_worker_wrapperC_am_cb(void *arg, void *data,
                                                     size_t length,
                                                     unsigned flags)
{
    return ucp_workaround_cb[2].cb(ucp_workaround_cb[2].cb, data, length, flags);
}

static unsigned ucp_workaround_cnt = 0;
ucp_workaround_t ucp_workaround_cb[] = {
        { .wrapper_cb = ucg_context_worker_wrapperA_am_cb },
        { .wrapper_cb = ucg_context_worker_wrapperB_am_cb },
        { .wrapper_cb = ucg_context_worker_wrapperC_am_cb }
};
#endif

ucs_status_t ucg_context_set_am_handler(ucg_plan_ctx_h plan_ctx, uint8_t id,
                                        uct_am_callback_t cb,
                                        uct_am_tracer_t tracer)
{
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
    ucp_am_handler_t* am_handler  = ucp_am_handlers + id;
    am_handler->tracer            = (ucp_am_tracer_t)tracer;
#ifdef HAVE_UCP_EXTENSIONS
    am_handler->features          = UCP_FEATURE_GROUPS;
    am_handler->flags             = UCT_CB_FLAG_ALT_ARG;
    am_handler->cb                = cb;
    am_handler->alt_arg           = plan_ctx;
#else
    unsigned wa_idx               = ucp_workaround_cnt++;
    am_handler->cb                = ucp_workaround_cb[wa_idx].wrapper_cb;
    ucp_workaround_cb[wa_idx].arg = cb;
    ucp_workaround_cb[wa_idx].arg = plan_ctx;
    am_handler->features          = 0;
    am_handler->flags             = 0;

    ucs_assert_always(ucp_workaround_cnt <= ucs_array_size(ucp_workaround_cb));
#endif

    return UCS_OK;
}

ucs_status_t ucg_context_set_async_timer(ucs_async_context_t *async,
                                         ucs_async_event_cb_t cb,
                                         void *cb_arg,
                                         ucs_time_t interval,
                                         int *timer_id_p)
{
    ucs_async_mode_t async_mode = async->mode;

    UCS_ASYNC_BLOCK(async);

    ucs_status_t status = ucs_async_add_timer(async_mode, interval, cb,
                                              cb_arg, async, timer_id_p);
    if (status != UCS_OK) {
        ucs_error("unable to add timer handler - %s",
                  ucs_status_string(status));
    }

    UCS_ASYNC_UNBLOCK(async);

    return status;
}

ucs_status_t ucg_context_unset_async_timer(ucs_async_context_t *async,
                                           int timer_id)
{
    UCS_ASYNC_BLOCK(async);

    ucs_status_t status = ucs_async_remove_handler(timer_id, 1);
    if (status != UCS_OK) {
        ucs_error("unable to remove timer handler %d - %s",
                  timer_id, ucs_status_string(status));
    }

    UCS_ASYNC_UNBLOCK(async);

    return status;
}


static ucs_status_t ucg_context_init(void *groups_ctx)
{
    ucg_context_t *ctx          = (ucg_context_t*)groups_ctx;
    ctx->next_group_id          = 0;

    /* Query all the available planners, to find the total space they require */
    size_t planners_total_size;
    ucs_status_t status = ucg_plan_query(&ctx->planners, &ctx->num_planners,
                                         &planners_total_size);
    if (status != UCS_OK) {
        return status;
    }

    ctx->planners_ctx = ucs_malloc(planners_total_size, "ucg planners context");
    if (ctx->planners_ctx == NULL) {
        status = UCS_ERR_NO_MEMORY;
        goto cleanup_desc;
    }

    status = ucg_plan_init(ctx->planners, ctx->num_planners, ctx->planners_ctx,
                           &ctx->per_group_planners_ctx);
    if (status != UCS_OK) {
        goto cleanup_pctx;
    }

    ucs_list_head_init(&ctx->groups_head);

    return UCS_OK;

cleanup_pctx:
    ucs_free(ctx->planners_ctx);

cleanup_desc:
    ucs_free(ctx->planners);
    return status;
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

    ucg_plan_finalize(ctx->planners, ctx->num_planners, ctx->planners_ctx);
    ucs_free(ctx->planners_ctx);
    ucs_free(ctx->planners);
}

#ifdef HAVE_UCP_EXTENSIONS
static void ucg_context_copy_used_ucp_params(ucp_params_t *dst,
                                             const ucp_params_t *src)
{
    size_t ucp_params_size = sizeof(src->field_mask);

    if (src->field_mask != 0) {
        enum ucp_params_field msb_flag = UCS_BIT((sizeof(uint64_t) * 8) - 1 -
                ucs_count_leading_zero_bits(src->field_mask));
        ucs_assert((msb_flag & src->field_mask) == msb_flag);

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
            ucp_params_size = ucs_offsetof(ucp_params_t, peer_info);
            break;

        case UCP_PARAM_FIELD_GROUP_PEER_INFO:
            ucp_params_size = sizeof(ucp_params_t);
            break;
        }
    }

    memcpy(dst, src, ucp_params_size);
}
#else
#define ucs_count_leading_zero_bits(_n) \
    ((sizeof(_n) <= 4) ? __builtin_clz((uint32_t)(_n)) : __builtin_clzl(_n))
#endif

static void ucg_context_copy_used_ucg_params(ucg_params_t *dst,
                                             const ucg_params_t *src)
{
    size_t ucg_params_size = sizeof(src->field_mask) +
                             ucs_offsetof(ucg_params_t, field_mask);

    if (src->field_mask != 0) {
        enum ucg_params_field msb_flag = UCS_BIT((sizeof(uint64_t) * 8) - 1 -
                ucs_count_leading_zero_bits(src->field_mask));
        ucs_assert((msb_flag & src->field_mask) == msb_flag);

        switch (msb_flag) {
        case UCG_PARAM_FIELD_JOB_UID:
            ucg_params_size = ucs_offsetof(ucg_params_t, address);
            break;

        case UCG_PARAM_FIELD_ADDRESS_CB:
            ucg_params_size = ucs_offsetof(ucg_params_t, neighbors);
            break;

        case UCG_PARAM_FIELD_NEIGHBORS_CB:
            ucg_params_size = ucs_offsetof(ucg_params_t, datatype);
            break;

        case UCG_PARAM_FIELD_DATATYPE_CB:
            ucg_params_size = ucs_offsetof(ucg_params_t, reduce_op);
            break;

        case UCG_PARAM_FIELD_REDUCE_OP_CB:
            ucg_params_size = ucs_offsetof(ucg_params_t, completion);
            break;

        case UCG_PARAM_FIELD_COMPLETION_CB:
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

ucg_params_t ucg_global_params; /* Ugly - but efficient */

static void ucg_context_comp_fallback(void *req, ucs_status_t status)
{
    uint32_t *flags = UCS_PTR_BYTE_OFFSET(req,
            ucg_global_params.completion.comp_flag_offset);

    ucs_status_t *result = UCS_PTR_BYTE_OFFSET(req,
            ucg_global_params.completion.comp_status_offset);

    *result = status;
    *flags |= 1;
}

static void ucg_context_comp_fallback_no_offsets(void *req, ucs_status_t status)
{
    *(uint8_t*)req = (uint8_t)-1;
}

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
        dfl_config->ucp_config = NULL;
        config                 = dfl_config;
    }

    /* Store the UCG params in a global location, for easy access */
    ucg_context_copy_used_ucg_params(&ucg_global_params, params);

    int params_have_comp = (params->field_mask & UCG_PARAM_FIELD_COMPLETION_CB);
    if ((!params_have_comp) || (!params->completion.coll_comp_cb_f)) {
        ucg_global_params.completion.coll_comp_cb_f = params_have_comp ?
                ucg_context_comp_fallback : ucg_context_comp_fallback_no_offsets;
    }

#ifndef HAVE_UCP_EXTENSIONS
    const ucp_params_t *ucp_params_arg = params->super;
#else
    /* Avoid overwriting the headroom value by copying all UCP params aside */
    ucp_params_t ucp_params;
    const ucp_params_t *ucp_params_arg = &ucp_params;
    ucg_context_copy_used_ucp_params(&ucp_params, params->super);

    /*
     * Note: This appears to be an overkill, but the reason is we want to change
     *       the value UCG passes to UCP during initialization, without changing
     *       the structure passed by the user, while avoiding potential access
     *       to undefined memory in the case of an executable built agains an
     *       older UCP header (where there the UCP parameters structure was
     *       shorter).
     */

    if ((params->super->field_mask & UCP_PARAM_FIELD_CONTEXT_HEADROOM) == 0) {
        ucp_params.context_headroom = 0;
    }

    ucp_params.field_mask       |= UCP_PARAM_FIELD_CONTEXT_HEADROOM;
    ucp_params.context_headroom += ucs_offsetof(ucg_context_t, ucp_ctx);
#endif
    ucg_global_params.super      = NULL; /* Should never be accessed again */

    /* Create the UCP context, which should have room for UCG in its headroom */
    status = ucp_init_version(ucp_api_major_version, ucp_api_minor_version,
                              ucp_params_arg, config->ucp_config,
                              (ucp_context_h*)context_p);
    if (status != UCS_OK) {
        goto err_config;
    }

#ifndef HAVE_UCP_EXTENSIONS
    /* Below is a somewhat "hacky" workaround, involving moving UCP context */
    size_t headroom = ucs_offsetof(ucg_context_t, ucp_ctx);
    ucg_context_h tmp = ucs_realloc(*context_p,
                                    headroom + sizeof(ucp_context_t),
                                    "ucg_context");
    if (tmp == NULL) {
        ucp_cleanup(*(ucp_context_h*)context_p);
        goto err_config;
    }

    memmove(UCS_PTR_BYTE_OFFSET(tmp, headroom), tmp, sizeof(ucp_context_t));
#endif

#if ENABLE_ASSERT
    /* Issue a warning to prevent measuring performance with a debug version */
    if (ucp_params_arg->peer_info.global_idx == 0) {
        printf("Note: UCG was built with some debugging enabled, and it should "
               "not be used for performance measurement.\n");
    }
#endif

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

    ucg_plan_print_info(context->planners, context->num_planners, stream);

    fprintf(stream, "#\n");

    ucp_context_print_info(&context->ucp_ctx, stream);
}

ucs_status_t ucg_config_read(const char *env_prefix, const char *filename,
                             ucg_config_t **config_p)
{
    ucg_config_t *config = UCS_ALLOC_CHECK(sizeof(*config), "ucg config");
    ucs_status_t status  = ucp_config_read(env_prefix, filename,
                                           &config->ucp_config);
    if (status != UCS_OK) {
        goto err_free_config;
    }

    status = ucs_config_parser_fill_opts(config, ucg_config_table,
                                         config->ucp_config->env_prefix,
                                         NULL, 0);
    if (status != UCS_OK) {
        goto err_free_ucp_config;
    }

    *config_p = config;
    return UCS_OK;

err_free_ucp_config:
    ucp_config_release(config->ucp_config);
err_free_config:
    ucs_free(config);
err:
    return status;
}

void ucg_config_release(ucg_config_t *config)
{
    ucs_config_parser_release_opts(config, ucg_config_table);
    ucp_config_release(config->ucp_config);
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
