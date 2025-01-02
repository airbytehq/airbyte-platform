/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.workers.config

import io.micronaut.context.annotation.Factory
import io.temporal.client.WorkflowClient
import io.temporal.worker.WorkerFactory
import io.temporal.worker.WorkerFactoryOptions
import jakarta.inject.Singleton

/**
 * Micronaut bean factory for Temporal-related singletons.
 */
@Factory
class TemporalBeanFactory {
  @Singleton
  fun workerFactory(workflowClient: WorkflowClient): WorkerFactory {
    val workerFactoryOptions =
      WorkerFactoryOptions.newBuilder()
        .build()
    return WorkerFactory.newInstance(workflowClient, workerFactoryOptions)
  }
}
