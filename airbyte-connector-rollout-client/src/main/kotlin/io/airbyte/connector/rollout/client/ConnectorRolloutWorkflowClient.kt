/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.client

import io.airbyte.commons.temporal.factories.WorkflowClientFactory
import io.airbyte.connector.rollout.shared.Constants
import io.airbyte.micronaut.runtime.AirbyteTemporalConfig
import io.micronaut.context.annotation.Factory
import io.temporal.client.WorkflowClient
import io.temporal.common.converter.DataConverter
import jakarta.inject.Singleton

class WorkflowClientWrapper(
  private val workflowClient: WorkflowClient,
) {
  fun getClient(): WorkflowClient = workflowClient
}

@Factory
class ConnectorRolloutWorkflowClient {
  @Singleton
  fun workflowClient(
    temporalWorkflowServiceFactory: ConnectorRolloutTemporalWorkflowServiceFactory,
    temporalSdkTimeouts: TemporalSdkTimeouts,
    airbyteTemporalConfig: AirbyteTemporalConfig,
    dataConverter: DataConverter,
  ): WorkflowClientWrapper {
    val temporalWorkflowService = temporalWorkflowServiceFactory.createTemporalWorkflowServiceLazily(temporalSdkTimeouts)
    return WorkflowClientWrapper(
      WorkflowClientFactory().createWorkflowClient(
        temporalWorkflowService,
        if (airbyteTemporalConfig.cloud.enabled) airbyteTemporalConfig.cloud.connectorRollout.namespace else Constants.DEFAULT_NAMESPACE,
        dataConverter,
      ),
    )
  }
}
