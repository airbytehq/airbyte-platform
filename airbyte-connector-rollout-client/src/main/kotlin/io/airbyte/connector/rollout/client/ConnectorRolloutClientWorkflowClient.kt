/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.client

import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import io.temporal.client.WorkflowClient
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.serviceclient.WorkflowServiceStubsOptions
import jakarta.inject.Named
import jakarta.inject.Singleton

@Factory
class ConnectorRolloutClientWorkflowClient {
  @Singleton
  @Named("connectorRolloutClientWorkflowClient")
  @Requires(property = "temporal.host")
  fun connectorRolloutWorkflowClient(
    @Value("\${temporal.host}") temporalServer: String,
  ): WorkflowClient {
    val options =
      WorkflowServiceStubsOptions.newBuilder()
        .setTarget(temporalServer)
        .build()
    val service = WorkflowServiceStubs.newInstance(options)
    return WorkflowClient.newInstance(service)
  }
}
