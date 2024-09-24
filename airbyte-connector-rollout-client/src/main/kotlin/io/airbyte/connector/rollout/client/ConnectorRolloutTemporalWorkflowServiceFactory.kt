/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.connector.rollout.client

import io.airbyte.commons.temporal.factories.TemporalCloudConfig
import io.airbyte.commons.temporal.factories.TemporalSelfHostedConfig
import io.airbyte.commons.temporal.factories.WorkflowServiceStubsFactory
import io.airbyte.commons.temporal.factories.WorkflowServiceStubsTimeouts
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Value
import io.temporal.serviceclient.WorkflowServiceStubs
import jakarta.inject.Singleton
import lombok.extern.slf4j.Slf4j
import java.time.Duration
import java.util.Objects

@Slf4j
@Singleton
class ConnectorRolloutTemporalWorkflowServiceFactory(
  @Property(name = "temporal.cloud.client.cert") temporalCloudClientCert: String?,
  @Property(name = "temporal.cloud.client.key") temporalCloudClientKey: String?,
  @Property(name = "temporal.cloud.enabled", defaultValue = "false") temporalCloudEnabled: Boolean,
  @Value("\${temporal.cloud.host}") temporalCloudHost: String?,
  @Value("\${temporal.cloud.connectorRollout.namespace}") temporalCloudNamespace: String?,
  @Value("\${temporal.host}") temporalHost: String?,
) {
  private val temporalCloudConfig: TemporalCloudConfig
  private val temporalCloudEnabled: Boolean
  private val workflowServiceStubsFactory: WorkflowServiceStubsFactory

  init {
    this.temporalCloudEnabled = Objects.requireNonNullElse(temporalCloudEnabled, false)
    temporalCloudConfig = TemporalCloudConfig(temporalCloudClientCert, temporalCloudClientKey, temporalCloudHost, temporalCloudNamespace)
    workflowServiceStubsFactory =
      WorkflowServiceStubsFactory(
        temporalCloudConfig,
        TemporalSelfHostedConfig(temporalHost, if (this.temporalCloudEnabled) temporalCloudNamespace else DEFAULT_NAMESPACE),
        this.temporalCloudEnabled,
      )
  }

  /**
   * Create WorkflowServiceStubs without making a connection to the Temporal server.
   *
   * Does not initialize any namespace.
   *
   */
  fun createTemporalWorkflowServiceLazily(temporalSdkTimeouts: TemporalSdkTimeouts): WorkflowServiceStubs {
    val timeouts =
      WorkflowServiceStubsTimeouts(
        temporalSdkTimeouts.rpcTimeout,
        temporalSdkTimeouts.rpcLongPollTimeout,
        temporalSdkTimeouts.rpcQueryTimeout,
        MAX_TIME_TO_CONNECT,
      )
    val options = workflowServiceStubsFactory.createWorkflowServiceStubsOptions(timeouts)
    return WorkflowServiceStubs.newServiceStubs(options)
  }

  companion object {
    private val MAX_TIME_TO_CONNECT = Duration.ofMinutes(2)
    const val DEFAULT_NAMESPACE = "default"
  }
}
