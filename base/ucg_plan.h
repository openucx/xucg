/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_PLAN_H_
#define UCG_PLAN_H_

#include "../api/ucg_plan_component.h"

size_t ucg_plan_get_context_size(ucg_plan_desc_t *descs, unsigned desc_cnt);

ucs_status_t ucg_plan_query(ucg_plan_desc_t **desc_p, unsigned *num_desc_p,
                            size_t *total_plan_ctx_size);

ucs_status_t ucg_plan_init(ucg_plan_desc_t *descs, unsigned desc_cnt,
                           ucg_plan_ctx_h plan, size_t *per_group_ctx_size);

void ucg_plan_finalize(ucg_plan_desc_t *descs, unsigned desc_cnt,
                       ucg_plan_ctx_h plan);

ucs_status_t ucg_plan_group_create(ucg_group_h group);

void ucg_plan_group_destroy(ucg_group_h group);

void ucg_plan_print_info(ucg_plan_desc_t *descs, unsigned desc_cnt, FILE *stream);

#endif /* UCG_PLAN_H_ */
