/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_DEF_H_
#define UCG_DEF_H_

#include <ucs/type/status.h>
#include <ucs/config/types.h>
#include <stddef.h>
#include <stdint.h>


/**
 * @ingroup UCG_CONTEXT
 * @brief UCG Application Context
 *
 * UCG application context (or just a context) is an opaque handle that holds a
 * UCG communication instance's global information.  It represents a single UCG
 * communication instance.  The communication instance could be an OS process
 * (an application) that uses UCP library.  This global information includes
 * communication resources, endpoints, memory, temporary file storage, and
 * other communication information directly associated with a specific UCG
 * instance.  The context also acts as an isolation mechanism, allowing
 * resources associated with the context to manage multiple concurrent
 * communication instances. For example, users using both MPI and OpenSHMEM
 * sessions simultaneously can isolate their communication by allocating and
 * using separate contexts for each of them. Alternatively, users can share the
 * communication resources (memory, network resource context, etc.) between
 * them by using the same application context. A message sent or a RMA
 * operation performed in one application context cannot be received in any
 * other application context.
 */
typedef struct ucg_context               *ucg_context_h;


/**
 * @ingroup UCG_CONTEXT
 * @brief UCG configuration descriptor
 *
 * This descriptor defines the configuration for @ref ucg_context_h
 * "UCG application context". The configuration is loaded from the run-time
 * environment (using configuration files of environment variables)
 * using @ref ucg_config_read "ucg_config_read" routine and can be printed
 * using @ref ucg_config_print "ucg_config_print" routine. In addition,
 * application is responsible to release the descriptor using
 * @ref ucg_config_release "ucg_config_release" routine.
 *
 * @todo This structure will be modified through a dedicated function.
 */
typedef struct ucg_config                ucg_config_t;


 /**
  * @ingroup UCG_GROUP
  * @brief UCG Group
  *
  * UCG group is an opaque object representing a set of connected remote workers.
  * This object is used for collective operations - like the ones defined by the
  * Message Passing Interface (MPI). Groups are created with respect to a local
  * worker, and share its endpoints for communication with the remote workers.
  */
typedef struct ucg_group                *ucg_group_h;


 /**
  * @ingroup UCG_GROUP
  * @brief UCG collective operation handle
  *
  * UCG collective is an opaque object representing a description of a collective
  * operation. Much like in object-oriented paradigms, a collective is like a
  * "class" which can be instantiated - an instance would be a UCG request to
  * perform this collective operation once. The description holds all the
  * necessary information to perform collectives, so re-starting an operation
  * requires no additional parameters.
  */
typedef void                            *ucg_coll_h;


/**
 * @ingroup UCG_GROUP
 * @brief UCG group identifier.
 *
 * Each UCG group requires a unique identifier, to detect which messages are
 * part of the collective operations on it. Disjoint groups (with no members in
 * common) could have identical group identifiers.
 */
typedef uint16_t                         ucg_group_id_t;


/**
 * @ingroup UCG_GROUP
 * @brief UCG group member index.
 *
 * UCG groups have multiple peers: remote worker objects acting as group members.
 * Each group member, including the local worker which was used to create the
 * group, has an unique identifier within the group - an integer between 0 and
 * the number of peers in it. The same worker may have different identifiers
 * in different groups, identifiers which are passed by user during creation.
 */
typedef uint64_t                         ucg_group_member_index_t;

/**
 * @ingroup UCG_GROUP
 * @brief Completion callback for non-blocking collective operations.
 *
 * This callback routine is invoked whenever the @ref ucg_collective
 * "collective operation" is completed. It is important to note that the call-back is
 * only invoked in a case when the operation cannot be completed in place.
 *
 * @param [in]  request   The completed collective operation request.
 * @param [in]  status    Completion status. If the send operation was completed
 *                        successfully UCX_OK is returned. If send operation was
 *                        canceled UCS_ERR_CANCELED is returned.
 *                        Otherwise, an @ref ucs_status_t "error status" is
 *                        returned.
 */
typedef void (*ucg_collective_callback_t)(void *request, ucs_status_t status);

/**
 * @ingroup UCG_GROUP
 * @brief Progress a specific collective operation request.
 *
 * This routine would explicitly progress a collective operation request.
 *
 * @param [in]  coll      The collective operation to be progressed.
 *
 * @return Non-zero if any communication was progressed, zero otherwise.
 *
 */
typedef unsigned (*ucg_collective_progress_t)(ucg_coll_h coll);

#endif
