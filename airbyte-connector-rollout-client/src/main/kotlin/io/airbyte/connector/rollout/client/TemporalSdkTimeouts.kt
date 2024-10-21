/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.connector.rollout.client

import io.micronaut.context.annotation.ConfigurationProperties
import io.temporal.serviceclient.ServiceStubsOptions
import io.temporal.serviceclient.WorkflowServiceStubsOptions

/**
 * Micronaut configured properties for the Temporal SDK RPC timeout values. <br></br>
 * <br></br>
 * **N.B.**: If the properties are not present in the configuration, the default values provided
 * by the Temporal SDK are used.
 *
 */
@ConfigurationProperties("temporal.sdk.timeouts")
class TemporalSdkTimeouts {
  var rpcTimeout = ServiceStubsOptions.DEFAULT_RPC_TIMEOUT
    /**
     * Sets the RPC timeout value, if not `null`. Otherwise, the default value defined in
     * [WorkflowServiceStubsOptions.DEFAULT_RPC_TIMEOUT] is used.
     *
     * @param rpcTimeout The RPC timeout value.
     */
    set(rpcTimeout) {
      if (rpcTimeout != null) {
        field = rpcTimeout
      }
    }
  var rpcLongPollTimeout = WorkflowServiceStubsOptions.DEFAULT_POLL_RPC_TIMEOUT
    /**
     * Sets the RPC long poll timeout value, if not `null`. Otherwise, the default value defined
     * in [WorkflowServiceStubsOptions.DEFAULT_POLL_RPC_TIMEOUT] is used.
     *
     * @param rpcLongPollTimeout The RPC timeout value.
     */
    set(rpcLongPollTimeout) {
      if (rpcLongPollTimeout != null) {
        field = rpcLongPollTimeout
      }
    }
  var rpcQueryTimeout = WorkflowServiceStubsOptions.DEFAULT_QUERY_RPC_TIMEOUT
    /**
     * Sets the RPC query timeout value, if not `null`. Otherwise, the default value defined in
     * [WorkflowServiceStubsOptions.DEFAULT_QUERY_RPC_TIMEOUT] is used.
     *
     * @param rpcQueryTimeout The RPC timeout value.
     */
    set(rpcQueryTimeout) {
      if (rpcQueryTimeout != null) {
        field = rpcQueryTimeout
      }
    }
}
