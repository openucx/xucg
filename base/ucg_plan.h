/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_PLAN_H_
#define UCG_PLAN_H_

#include "../api/ucg_plan_component.h"

/* Functions on all planning components */
ucs_status_t ucg_plan_query(ucg_plan_desc_t **desc_p, unsigned *num_desc_p);
void ucg_plan_release_list(ucg_plan_desc_t *desc, unsigned desc_cnt);

ucs_status_t ucg_plan_select_component(ucg_plan_desc_t *planners,
                                       unsigned num_planners,
                                       ucs_config_names_array_t* config_planners,
                                       const ucg_group_params_t *group_params,
                                       const ucg_collective_params_t *coll_params,
                                       ucg_plan_component_t **planc_p,
                                       size_t *planc_offset_p);

#endif /* UCG_PLAN_H_ */
