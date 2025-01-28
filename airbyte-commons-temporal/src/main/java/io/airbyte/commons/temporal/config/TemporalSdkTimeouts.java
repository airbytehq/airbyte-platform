/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.config;

import static io.temporal.serviceclient.WorkflowServiceStubsOptions.DEFAULT_POLL_RPC_TIMEOUT;
import static io.temporal.serviceclient.WorkflowServiceStubsOptions.DEFAULT_QUERY_RPC_TIMEOUT;
import static io.temporal.serviceclient.WorkflowServiceStubsOptions.DEFAULT_RPC_TIMEOUT;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import java.time.Duration;

/**
 * Micronaut configured properties for the Temporal SDK RPC timeout values. <br />
 * <br />
 * <b>N.B.</b>: If the properties are not present in the configuration, the default values provided
 * by the Temporal SDK are used.
 *
 */
@ConfigurationProperties("temporal.sdk.timeouts")
public class TemporalSdkTimeouts {

  private Duration rpcTimeout = DEFAULT_RPC_TIMEOUT;
  private Duration rpcLongPollTimeout = DEFAULT_POLL_RPC_TIMEOUT;
  private Duration rpcQueryTimeout = DEFAULT_QUERY_RPC_TIMEOUT;

  public Duration getRpcTimeout() {
    return rpcTimeout;
  }

  /**
   * Sets the RPC timeout value, if not {@code null}. Otherwise, the default value defined in
   * {@link WorkflowServiceStubsOptions#DEFAULT_RPC_TIMEOUT} is used.
   *
   * @param rpcTimeout The RPC timeout value.
   */
  public void setRpcTimeout(final Duration rpcTimeout) {
    if (rpcTimeout != null) {
      this.rpcTimeout = rpcTimeout;
    }
  }

  public Duration getRpcLongPollTimeout() {
    return rpcLongPollTimeout;
  }

  /**
   * Sets the RPC long poll timeout value, if not {@code null}. Otherwise, the default value defined
   * in {@link WorkflowServiceStubsOptions#DEFAULT_POLL_RPC_TIMEOUT} is used.
   *
   * @param rpcLongPollTimeout The RPC timeout value.
   */
  public void setRpcLongPollTimeout(final Duration rpcLongPollTimeout) {
    if (rpcLongPollTimeout != null) {
      this.rpcLongPollTimeout = rpcLongPollTimeout;
    }
  }

  public Duration getRpcQueryTimeout() {
    return rpcQueryTimeout;
  }

  /**
   * Sets the RPC query timeout value, if not {@code null}. Otherwise, the default value defined in
   * {@link WorkflowServiceStubsOptions#DEFAULT_QUERY_RPC_TIMEOUT} is used.
   *
   * @param rpcQueryTimeout The RPC timeout value.
   */
  public void setRpcQueryTimeout(final Duration rpcQueryTimeout) {
    if (rpcQueryTimeout != null) {
      this.rpcQueryTimeout = rpcQueryTimeout;
    }
  }

}
