/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import io.mockk.mockk
import io.temporal.client.WorkflowClient
import jakarta.inject.Named
import jakarta.inject.Singleton

@Factory
class TemporalShadowing {
  // This is so that the micronaut test do not try to instantiate a connection to a local temporal instance that doesn't exist.
  @Named("workerWorkflowClient")
  @Singleton
  @Replaces(WorkflowClient::class)
  fun workflowClient(): WorkflowClient = mockk()
}
