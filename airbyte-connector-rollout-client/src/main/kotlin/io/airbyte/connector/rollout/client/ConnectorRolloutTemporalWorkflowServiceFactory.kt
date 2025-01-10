/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.connector.rollout.client

import io.airbyte.commons.temporal.factories.TemporalCloudConfig
import io.airbyte.commons.temporal.factories.TemporalSelfHostedConfig
import io.airbyte.commons.temporal.factories.WorkflowServiceStubsFactory
import io.airbyte.commons.temporal.factories.WorkflowServiceStubsTimeouts
import io.airbyte.connector.rollout.shared.Constants
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Value
import io.temporal.serviceclient.WorkflowServiceStubs
import jakarta.inject.Singleton
import java.time.Duration

@Singleton
class ConnectorRolloutTemporalWorkflowServiceFactory(
  @Property(name = "temporal.cloud.client.cert") temporalCloudClientCert: String?,
  @Property(name = "temporal.cloud.client.key") temporalCloudClientKey: String?,
  @Property(name = "temporal.cloud.enabled", defaultValue = "false") temporalCloudEnabled: Boolean,
  @Value("\${temporal.cloud.connector-rollout.host}") temporalCloudHost: String?,
  @Value("\${temporal.cloud.connector-rollout.namespace}") temporalCloudNamespace: String?,
  @Value("\${temporal.host}") temporalHost: String?,
) {
  private val temporalCloudConfig: TemporalCloudConfig =
    TemporalCloudConfig(temporalCloudClientCert, temporalCloudClientKey, temporalCloudHost, temporalCloudNamespace)

  private val workflowServiceStubsFactory: WorkflowServiceStubsFactory =
    WorkflowServiceStubsFactory(
      temporalCloudConfig,
      TemporalSelfHostedConfig(temporalHost, if (temporalCloudEnabled) temporalCloudNamespace else Constants.DEFAULT_NAMESPACE),
      temporalCloudEnabled,
    )

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
  }
}
