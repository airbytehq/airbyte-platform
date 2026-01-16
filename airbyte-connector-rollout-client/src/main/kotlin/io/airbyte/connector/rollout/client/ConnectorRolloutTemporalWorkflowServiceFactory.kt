/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.client

import io.airbyte.commons.temporal.factories.TemporalCloudConfig
import io.airbyte.commons.temporal.factories.TemporalSelfHostedConfig
import io.airbyte.commons.temporal.factories.WorkflowServiceStubsFactory
import io.airbyte.commons.temporal.factories.WorkflowServiceStubsTimeouts
import io.airbyte.connector.rollout.shared.Constants
import io.airbyte.micronaut.runtime.AirbyteTemporalConfig
import io.micrometer.core.instrument.MeterRegistry
import io.temporal.serviceclient.WorkflowServiceStubs
import jakarta.inject.Singleton
import java.time.Duration

@Singleton
class ConnectorRolloutTemporalWorkflowServiceFactory(
  airbyteTemporalConfig: AirbyteTemporalConfig,
  meterRegistry: MeterRegistry?,
) {
  private val temporalCloudConfig: TemporalCloudConfig =
    TemporalCloudConfig(
      airbyteTemporalConfig.cloud.client.cert,
      airbyteTemporalConfig.cloud.client.key,
      airbyteTemporalConfig.cloud.connectorRollout.host,
      airbyteTemporalConfig.cloud.connectorRollout.namespace,
    )

  private val workflowServiceStubsFactory: WorkflowServiceStubsFactory =
    WorkflowServiceStubsFactory(
      temporalCloudConfig,
      TemporalSelfHostedConfig(
        airbyteTemporalConfig.host,
        if (airbyteTemporalConfig.cloud.enabled) airbyteTemporalConfig.cloud.connectorRollout.namespace else Constants.DEFAULT_NAMESPACE,
      ),
      airbyteTemporalConfig.cloud.enabled,
      meterRegistry,
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
