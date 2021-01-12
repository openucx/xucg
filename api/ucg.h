/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_H_
#define UCG_H_

#include <ucg/api/ucg_def.h>
#include <ucg/api/ucg_version.h>

#include <ucp/api/ucp.h>

BEGIN_C_DECLS

/**
 * @defgroup UCG_API Unified Communication Protocol (UCG) API
 * @{
 * This section describes UCG API.
 * @}
 */


/**
 * @defgroup UCG_CONTEXT UCG Application Context
 * @ingroup UCG_API
 * @{
 * Application context is a primary concept of UCG design which
 * provides an isolation mechanism, allowing resources associated
 * with the context to separate or share network communication context
 * across multiple instances of applications.
 *
 * This section provides a detailed description of this concept and
 * routines associated with it.
 *
 * @}
 */


 /**
 * @defgroup UCG_GROUP UCG Group
 * @ingroup UCG_API
 * @{
 * UCG Group routines
 * @}
 */


/**
* @defgroup UCG_COLLECTIVE UCG Collective operation
* @ingroup UCG_API
* @{
* UCG Collective operations
* @}
*/


/**
 * @ingroup UCG_CONTEXT
 * @brief UCG group parameters field mask.
 *
 * The enumeration allows specifying which fields in @ref ucg_group_params_t are
 * present. It is used to enable backward compatibility support.
 */
enum ucg_params_field {
    UCG_PARAM_FIELD_JOB_UID       = UCS_BIT(0), /**< Unique ID for this job */
    UCG_PARAM_FIELD_ADDRESS_CB    = UCS_BIT(1), /**< Peer address lookup */
    UCG_PARAM_FIELD_NEIGHBORS_CB  = UCS_BIT(2), /**< Neighborhood info */
    UCG_PARAM_FIELD_DATATYPE_CB   = UCS_BIT(3), /**< Callback for datatypes */
    UCG_PARAM_FIELD_REDUCE_OP_CB  = UCS_BIT(4), /**< Callback for reduce ops */
    UCG_PARAM_FIELD_COMPLETION_CB = UCS_BIT(5), /**< Actions upon completion */
    UCG_PARAM_FIELD_MPI_IN_PLACE  = UCS_BIT(6), /**< MPI_IN_PLACE value */
    UCG_PARAM_FIELD_HANDLE_FAULT  = UCS_BIT(7)  /**< Fault-tolerance support */
};

enum ucg_fault_tolerance_mode {
    UCG_FAULT_IS_FATAL = 0,      /**< Fault will cause the run to terminate */
    UCG_FAULT_IS_RETURNED,       /**< Fault will be returned to the user */
    UCG_FAULT_IS_TRANSPARENT,    /**< Fault will be circumvented by UCG */
    UCG_FAULT_IS_HANDLED_BY_USER /**< Fault will be handled by the user - the
                                      return value of the handler will indicate
                                      success or failure to handle the fault.*/
};

/**
 * @ingroup UCG_CONTEXT
 * @brief Creation parameters for the UCG context.
 *
 * The structure defines the parameters that are used during the UCG
 * initialization by @ref ucg_init .
 */
typedef struct ucg_params {
    ucp_params_t *super; /** Pointer is better suited for ABI compatibility */

    /**
     * Mask of valid fields in this structure, using bits from
     * @ref ucg_params_field. Fields not specified in this mask will be ignored.
     * Provides ABI compatibility with respect to adding new fields.
     */
    uint64_t field_mask;

    uint32_t job_uid; /** Unique ID of the job which this process is a member of */

    /* Callback functions for address lookup, used at connection establishment */
    struct {
        int (*lookup_f)(void *cb_group_context,
                        ucg_group_member_index_t index,
                        ucp_address_t **addr,
                        size_t *addr_len);
        void (*release_f)(ucp_address_t *addr);
    } address;

    /*
     * The two callback functions below are used for Neighborhood collectives:
     * the first checks the local in-degree and out-degree of the communicator
     * graph, and the second fills in the indexes of these peers (assuming the
     * array is large enough to store all these indexes).
     */
    struct {
        int (*vertex_count_f)(void *cb_group_context,
                              unsigned *in_degree,
                              unsigned *out_degree);
        int (*vertex_query_f)(void *cb_group_context,
                              ucg_group_member_index_t *in,
                              ucg_group_member_index_t *out);
    } neighbors;

    /* Information about datatypes */
    struct {
        /* Convert the opaque data-type into UCX's structure (should return 0) */
        int (*convert)(void *datatype, ucp_datatype_t *ucp_datatype);

        /* Check if the data-type is an integer (of any length) */
        int (*is_integer_f)(void *datatype, int *is_signed);

        /* Check if the data-type is a floating-point (of any length) */
        int (*is_floating_point_f)(void *datatype);
    } datatype;

    /* Information about reduction operations */
    struct {
        /*
         * To support any type of reduction for an MPI implementation, this callback
         * function can be called (when a new message arrives) to reduce the data
         * into a buffer (which already contains a partial result). Below are some
         * additional functions to detect the type of reduction, so that simple
         * reductions (e.g. sum on integers) doesn't require using this callback.
         */
        int (*reduce_cb_f)(void *reduce_op, char *src, char *dst,
                           unsigned count, void *datatype);

        /* Check if the reduction operation is a summation (e.g. MPI_SUM) */
        int (*is_sum_f)(void *reduce_op);

        /* Check if the reduction also expects a location (e.g. MPI_MINLOC) */
        int (*is_loc_expected_f)(void *reduce_op);

        /* Check if the reduction operation is commutative (e.g. MPI_MINLOC) */
        int (*is_commutative_f)(void *reduce_op);
    } reduce_op;

    /* Requested action upon completion (for non-blocking calls) */
    struct {
        /* Callback function to invoke upon completion of a collective call */
        void (*coll_comp_cb_f)(void *req, ucs_status_t status);

        /* offset where to set completion (ignored unless coll_comp_cb is NULL) */
        size_t comp_flag_offset;

        /* offset where to write status (ignored unless coll_comp_cb is NULL) */
        size_t comp_status_offset;
    } completion;

    /* The value of MPI_IN_PLACE, which can replace send or receive buffers */
    void* mpi_in_place;

    /* Fault-tolerance can be enabled by passing */
    struct {
        enum ucg_fault_tolerance_mode mode;
        void *context;
        int (*handler_f)(void *context, int *error_code, ...);
        void (*err_str_f)(int error_code, char **error_description);
    } fault;
} ucg_params_t;

/**
 * @ingroup UCG_GROUP
 * @brief UCG group collective operation characteristics.
 *
 * The enumeration allows specifying modifiers to describe the requested
 * collective operation, as part of @ref ucg_collective_params_t
 * passed to @ref ucg_collective_start . For example, for MPI_Reduce:
 *
 * modifiers = UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE |
 *             UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_DESTINATION;
 *
 * The premise is that (a) any collective type can be described as a combination
 * of the flags below, and (b) the implementation can benefit from applying
 * logic based on these flags. For example, we can check if a collective has
 * a single rank as the source, which will be true for both MPI_Bcast and
 * MPI_Scatterv today, and potentially other types in the future.
 *
 * @note
 * For simplicity, some rarely used collectives were intentionally omitted. For
 * instance, MPI_Scan and MPI_Exscan could be supported using additional flags,
 * which are not part of the API at this time.
 */
enum ucg_collective_modifiers {
    /* Network Pattern Considerations */
    UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_SOURCE      = UCS_BIT( 0), /* otherwise from all */
    UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_DESTINATION = UCS_BIT( 1), /* otherwise to all */
    UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE          = UCS_BIT( 2), /* buffer reduction */
    UCG_GROUP_COLLECTIVE_MODIFIER_CONCATENATE        = UCS_BIT( 3), /* buffer concatenation */
    UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST          = UCS_BIT( 4), /* otherwise scatter */
    UCG_GROUP_COLLECTIVE_MODIFIER_VARIADIC           = UCS_BIT( 5), /* MPI_*v */
    UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE_PARTIAL  = UCS_BIT( 6), /* MPI_Scan (+VAR -> Exscan) */
    UCG_GROUP_COLLECTIVE_MODIFIER_NEIGHBOR           = UCS_BIT( 7), /* Neighbor collectives */

    /* Buffer/Data Management Considerations */
    UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE_STABLE   = UCS_BIT( 8), /* stable reduction */
    UCG_GROUP_COLLECTIVE_MODIFIER_NONCONTIG_DATATYPE = UCS_BIT( 9), /* some may be non-contiguous */
    UCG_GROUP_COLLECTIVE_MODIFIER_PERSISTENT         = UCS_BIT(10), /* otherwise destroy coll_h */
    UCG_GROUP_COLLECTIVE_MODIFIER_SYMMETRIC          = UCS_BIT(11), /* persistent on all ranks */
    UCG_GROUP_COLLECTIVE_MODIFIER_BARRIER            = UCS_BIT(12), /* prevent others from starting */
    UCG_GROUP_COLLECTIVE_MODIFIER_MOCK_EPS           = UCS_BIT(13), /* information gathering only */
};

/**
 * @ingroup UCG_GROUP
 * @brief UCG group collective operation description.
 *
 * Some collective operations have one special rank. For example MPI_Bcast has
 * the root of the broadcast, and MPI_Reduce has the root where the final result
 * must be written. The "root" field is used in cases where "modifiers" includes:
 *   (a) UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_SOURCE
 *   (b) UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_DESTINATION
 * In other cases, the "root" field is ignored.
 */
typedef struct ucg_collective_type {
    uint16_t                 modifiers; /* Collective description, using
                                           @ref ucg_collective_modifiers */
    ucg_group_member_index_t root :48;  /* Root rank, if applicable */
} UCS_S_PACKED ucg_collective_type_t;

/**
 * @ingroup UCG_GROUP
 * @brief UCG group member distance.
 *
 * During group creation, the caller can pass information about the distance of
 * each other member of the group. This information may be used to select the
 * best logical topology for collective operations inside UCG.
 */
enum ucg_group_member_distance {
    UCG_GROUP_MEMBER_DISTANCE_SELF   = 0, /* This is the calling member */
    UCG_GROUP_MEMBER_DISTANCE_CACHE  = UCS_MASK(1), /* member shares cache memory */
    /* Reserved for in-socket proximity values */
    UCG_GROUP_MEMBER_DISTANCE_SOCKET = UCS_MASK(3), /* member is on the same socket */
    /* Reserved for in-host proximity values */
    UCG_GROUP_MEMBER_DISTANCE_HOST   = UCS_MASK(4), /* member is on the same host */
    /* Reserved for network proximity values */
    UCG_GROUP_MEMBER_DISTANCE_NET    = UCS_MASK(8) - 2, /* member is on the network */

    UCG_GROUP_MEMBER_DISTANCE_FAULT  = UCS_MASK(8) - 1,
    UCG_GROUP_MEMBER_DISTANCE_LAST   = UCS_MASK(8)
} UCS_S_PACKED;

/**
 * @ingroup UCG_GROUP
 * @brief UCG group parameters field mask.
 *
 * The enumeration allows specifying which fields in @ref ucg_group_params_t are
 * present. It is used to enable backward compatibility support.
 */
enum ucg_group_params_field {
    UCG_GROUP_PARAM_FIELD_ID           = UCS_BIT(0), /**< Unique identifier */
    UCG_GROUP_PARAM_FIELD_MEMBER_COUNT = UCS_BIT(1), /**< Number of members */
    UCG_GROUP_PARAM_FIELD_MEMBER_INDEX = UCS_BIT(2), /**< My member index */
    UCG_GROUP_PARAM_FIELD_CB_CONTEXT   = UCS_BIT(3), /**< context for callbacks */
    UCG_GROUP_PARAM_FIELD_DISTANCES    = UCS_BIT(4)  /**< Member distance array */
};

/**
 * @ingroup UCG_GROUP
 * @brief Creation parameters for the UCG group.
 *
 * The structure defines the parameters that are used during the UCG group
 * @ref ucg_group_create "creation".
 */
typedef struct ucg_group_params {
    /**
     * Mask of valid fields in this structure, using bits from
     * @ref ucg_group_params_field. Fields not specified in this mask will be
     * ignored. Provides ABI compatibility with respect to adding new fields.
     */
    uint64_t field_mask;

    ucg_group_id_t id; /* Unique group identifier */

    ucg_group_member_index_t member_count; /* Number of group members */
    ucg_group_member_index_t member_index; /* My member index within the group */

    void *cb_context; /* Opaque context object for address/neighbor callbacks.
                         In MPI implementations this would likely be MPI_Comm */

    /*
     * This array contains information about the process placement of different
     * group members, which is used to select the best topology for collectives.
     *
     * For example, for 2 nodes, 3 sockets each, 4 cores per socket, each member
     * should be passed the distance array contents as follows:
     *   1st group member distance array:  0111222222223333333333333333
     *   2nd group member distance array:  1011222222223333333333333333
     *   3rd group member distance array:  1101222222223333333333333333
     *   4th group member distance array:  1110222222223333333333333333
     *   5th group member distance array:  2222011122223333333333333333
     *   6th group member distance array:  2222101122223333333333333333
     *   7th group member distance array:  2222110122223333333333333333
     *   8th group member distance array:  2222111022223333333333333333
     *    ...
     *   12th group member distance array: 3333333333333333011122222222
     *   13th group member distance array: 3333333333333333101122222222
     *    ...
     */
    enum ucg_group_member_distance *distance;
} ucg_group_params_t;


/**
 * @ingroup UCG_GROUP
 * @brief Creation parameters for the UCG collective operation.
 *
 * The structure defines the parameters that are used during the UCG collective
 * @ref ucg_collective_create "creation". The size of this structure is critical
 * to performance, as well as it being contiguous, because its entire contents
 * are accessed during run-time.
 */
typedef struct ucg_collective {
    struct {
        union {
            /* only in "send" (see @ref UCG_PARAM_TYPE ) */
            ucg_collective_type_t type;   /**< type and root of the collective */

            /* only in "recv" (see @ref UCG_PARAM_OP , @ref UCG_PARAM_DISPLS ) */
            void                 *op;     /**< external reduce operation handle */
            const int            *displs; /**< item displacement array */
        };
        void                     *buffer;  /**< buffer location to use */
        union {
            int64_t               count;   /**< item count (not int - for OSHMEM) */
            const int            *counts;  /**< item count array */
        };
        union {
            void                 *dtype;   /**< external data-type context */
            void                 *dtypes;  /**< external data-type context array */
            /*
             * Note: if UCG_PARAM_FIELD_DATATYPE_CB is not passed during UCG
             *       initialization, UCG will assume that dtype is already a
             *       UCP datatype (will perform static cast to ucp_datatpe_t)
             *       and dtypes points to an array of such UCP datatpes.
             */
        };
    } send, recv;
} UCS_S_PACKED UCS_V_ALIGNED(64) ucg_collective_params_t;

#define UCG_PARAM_TYPE(_params)   (_params)->send.type
#define UCG_PARAM_OP(_params)     (_params)->recv.op
#define UCG_PARAM_DISPLS(_params) (_params)->recv.displs


/**
 * @ingroup UCG_GROUP
 * @brief Create a group object.
 *
 * This routine allocates and initializes a @ref ucg_group_h "group" object.
 * This routine is a "collective operation", meaning it has to be called for
 * each worker participating in the group - before the first call on the group
 * is invoked on any of those workers. The call does not contain a barrier,
 * meaning a call on one worker can complete regardless of call on others.
 *
 * @note The group object is allocated within context of the calling thread
 *
 * @param [in] worker      Worker to create a group on top of.
 * @param [in] params      User defined @ref ucg_group_params_t configurations for the
 *                         @ref ucg_group_h "UCG group".
 * @param [out] group_p    A pointer to the group object allocated by the
 *                         UCG library
 *
 * @return Error code as defined by @ref ucs_status_t
 */
ucs_status_t ucg_group_create(ucp_worker_h worker,
                              const ucg_group_params_t *params,
                              ucg_group_h *group_p);


/**
 * @ingroup UCG_GROUP
 * @brief Destroy a group object.
 *
 * This routine releases the resources associated with a @ref ucg_group_h
 * "UCG group". This routine is also a "collective operation", similarly to
 * @ref ucg_group_create, meaning it must be called on each worker participating
 * in the group.
 *
 * @warning Once the UCG group handle is destroyed, it cannot be used with any
 * UCG routine.
 *
 * The destroy process releases and shuts down all resources associated with
 * the @ref ucg_group_h "group".
 *
 * @param [in]  group       Group object to destroy.
 */
void ucg_group_destroy(ucg_group_h group);


/**
 * @ingroup UCG_COLLECTIVE
 * @brief Creates a collective operation on a group object.
 * The parameters are intentionally non-constant, to allow UCG to write-back some
 * information and avoid redundant actions on the next call. For example, memory
 * registration handles are written back to the parameters pointer passed to the
 * function, and are re-used in subsequent calls.
 *
 * @param [in]  group       Group of participants in this collective operation.
 * @param [in]  params      Collective operation parameters.
 * @param [out] coll        Collective operation handle.
 *
 * @return Error code as defined by @ref ucs_status_t
 */
ucs_status_t ucg_collective_create(ucg_group_h group,
                                   const ucg_collective_params_t *params,
                                   ucg_coll_h *coll);


/**
 * @ingroup UCG_COLLECTIVE
 * @brief Starts a collective operation.
 *
 * @param [in]  coll        Collective operation handle.
 * @param [in]  req         Request handle, allocated by the user.
 *
 * @return UCS_OK           - The collective operation was completed immediately.
 * @return UCS_INPROGRESS   - The collective was not completed and is in progress.
 *
 * @return Error code as defined by @ref ucs_status_t
 */
ucs_status_t ucg_collective_start(ucg_coll_h coll, void *req);


/**
 * @ingroup UCG_COLLECTIVE
 * @brief Obtain the progress function to be applied to a collective operation.
 *
 * This routine returns a pointer to another one. The latter would explicitly
 * progresses a single collective operation request.
 *
 * @param [in]  coll        Collective operation handle.
 *
 * @return A valid function pointer.
 */
ucg_collective_progress_t ucg_request_get_progress(ucg_coll_h coll);


/**
 * @ingroup UCG_COLLECTIVE
 * @brief Cancel an outstanding collective request.
 *
 * @param [in]  coll        Collective operation handle.
 *
 * This routine tries to cancels an outstanding collective request.  After
 * calling this routine, the @a request will be in completed or canceled (but
 * not both) state regardless of the status of the target endpoint associated
 * with the collective request. If the request is completed successfully,
 * the @ref ucg_collective_callback_t completion callback will be
 * called with the @a status argument of the callback set to UCS_OK, and in a
 * case it is canceled the @a status argument is set to UCS_ERR_CANCELED.
 */
void ucg_request_cancel(ucg_coll_h coll);


/**
 * @ingroup UCG_COLLECTIVE
 * @brief Destroys a collective operation handle.
 *
 * This is only required for persistent collectives, where the flag
 * UCG_GROUP_COLLECTIVE_MODIFIER_PERSISTENT is passed when calling
 * @ref ucg_collective_create. Otherwise, the handle is
 * destroyed when the collective operation is completed.
 *
 * @param [in]  coll         Collective operation handle.
 *
 * @return Error code as defined by @ref ucs_status_t
 */
void ucg_collective_destroy(ucg_coll_h coll);


/**
 * @ingroup UCG_CONTEXT
 * @brief Read UCG configuration descriptor
 *
 * The routine fetches the information about UCG library configuration from
 * the run-time environment. Then, the fetched descriptor is used for
 * UCG library @ref ucg_init "initialization". The Application can print out the
 * descriptor using @ref ucg_config_print "print" routine. In addition
 * the application is responsible for @ref ucg_config_release "releasing" the
 * descriptor back to the UCG library.
 *
 * @param [in]  env_prefix    If non-NULL, the routine searches for the
 *                            environment variables that start with
 *                            @e \<env_prefix\>_UCX_ prefix.
 *                            Otherwise, the routine searches for the
 *                            environment variables that start with
 *                            @e UCX_ prefix.
 * @param [in]  filename      If non-NULL, read configuration from the file
 *                            defined by @e filename. If the file does not
 *                            exist, it will be ignored and no error reported
 *                            to the application.
 * @param [out] config_p      Pointer to configuration descriptor as defined by
 *                            @ref ucg_config_t "ucg_config_t".
 *
 * @return Error code as defined by @ref ucs_status_t
 */
ucs_status_t ucg_config_read(const char *env_prefix, const char *filename,
                             ucg_config_t **config_p);


/**
 * @ingroup UCG_CONTEXT
 * @brief Release configuration descriptor
 *
 * The routine releases the configuration descriptor that was allocated through
 * @ref ucg_config_read "ucg_config_read()" routine.
 *
 * @param [out] config        Configuration descriptor as defined by
 *                            @ref ucg_config_t "ucg_config_t".
 */
void ucg_config_release(ucg_config_t *config);


/**
 * @ingroup UCG_CONTEXT
 * @brief Modify context configuration.
 *
 * The routine changes one configuration setting stored in @ref ucg_config_t
 * "configuration" descriptor.
 *
 * @param [in]  config        Configuration to modify.
 * @param [in]  name          Configuration variable name.
 * @param [in]  value         Value to set.
 *
 * @return Error code.
 */
ucs_status_t ucg_config_modify(ucg_config_t *config, const char *name,
                               const char *value);


/**
 * @ingroup UCG_CONTEXT
 * @brief Print configuration information
 *
 * The routine prints the configuration information that is stored in
 * @ref ucg_config_t "configuration" descriptor.
 *
 * @todo Expose ucs_config_print_flags_t
 *
 * @param [in]  config        @ref ucg_config_t "Configuration descriptor"
 *                            to print.
 * @param [in]  stream        Output stream to print the configuration to.
 * @param [in]  title         Configuration title to print.
 * @param [in]  print_flags   Flags that control various printing options.
 */
void ucg_config_print(const ucg_config_t *config, FILE *stream,
                      const char *title, ucs_config_print_flags_t print_flags);


/** @cond PRIVATE_INTERFACE */
/**
 * @ingroup UCG_CONTEXT
 * @brief UCG context initialization with particular API version.
 *
 * This is an internal routine used to check compatibility with a particular
 * API version. @ref ucg_init should be used to create UCG context.
 */
ucs_status_t ucg_init_version(unsigned ucg_api_major_version,
                              unsigned ucg_api_minor_version,
                              unsigned ucp_api_major_version,
                              unsigned ucp_api_minor_version,
                              const ucg_params_t *params,
                              const ucg_config_t *config,
                              ucg_context_h *context_p);
/** @endcond */


/**
 * @ingroup UCG_CONTEXT
 * @brief UCG context initialization.
 *
 * This routine creates and initializes a @ref ucg_context_h
 * "UCG application context".
 *
 * @warning This routine must be called before any other UCG function
 * call in the application.
 *
 * This routine checks API version compatibility, then discovers the available
 * network interfaces, and initializes the network resources required for
 * discovering of the network and memory related devices.
 *  This routine is responsible for initialization all information required for
 * a particular application scope, for example, MPI application, OpenSHMEM
 * application, etc.
 *
 * @param [in]  config        UCG configuration descriptor allocated through
 *                            @ref ucg_config_read "ucg_config_read()" routine.
 * @param [in]  params        User defined @ref ucg_params_t configurations for the
 *                            @ref ucg_context_h "UCG application context".
 * @param [out] context_p     Initialized @ref ucg_context_h
 *                            "UCG application context".
 *
 * @return Error code as defined by @ref ucs_status_t
 */
ucs_status_t ucg_init(const ucg_params_t *params,
                      const ucg_config_t *config,
                      ucg_context_h *context_p);


/**
 * @ingroup UCG_CONTEXT
 * @brief Release UCG application context.
 *
 * This routine finalizes and releases the resources associated with a
 * @ref ucg_context_h "UCG application context".
 *
 * @warning An application cannot call any UCG routine
 * once the UCG application context released.
 *
 * The cleanup process releases and shuts down all resources associated with
 * the application context. After calling this routine, calling any UCG
 * routine without calling @ref ucg_init "UCG initialization routine" is invalid.
 *
 * @param [in] context_p   Handle to @ref ucg_context_h
 *                         "UCG application context".
 */
void ucg_cleanup(ucg_context_h context_p);


/**
 * @ingroup UCG_CONTEXT
 * @brief Print context information.
 *
 * This routine prints information about the context configuration: including
 * memory domains, transport resources, and other useful information associated
 * with the context.
 *
 * @param [in] context      Print this context object's configuration.
 * @param [in] stream       Output stream on which to print the information.
 */
void ucg_context_print_info(const ucg_context_h context, FILE *stream);


/**
 * @ingroup UCG_CONTEXT
 * @brief Gain access to the internal UCG context within a UCG context.
 *
 * This routine allows applications using UCG to call UCG APIs as well.
 *
 * @param [in] context Handle to @ref ucg_context_h "UCG application context".
 *
 * @return Handle to an internal @ref ucp_context_h "UCG application context".
 */
ucp_context_h ucg_context_get_ucp(ucg_context_h context);


/**
 * @ingroup UCG_CONTEXT
 * @brief Get UCG library version.
 *
 * This routine returns the UCG library version.
 *
 * @param [out] major_version       Filled with library major version.
 * @param [out] minor_version       Filled with library minor version.
 * @param [out] release_number      Filled with library release number.
 */
void ucg_get_version(unsigned *major_version, unsigned *minor_version,
                     unsigned *release_number);


/**
 * @ingroup UCG_CONTEXT
 * @brief Get UCG library version as a string.
 *
 * This routine returns the UCG library version as a string which consists of:
 * "major.minor.release".
 */
const char *ucg_get_version_string(void);


END_C_DECLS

#endif
