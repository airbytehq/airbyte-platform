/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config;

import io.micronaut.context.annotation.Factory;
import io.temporal.client.WorkflowClient;
import io.temporal.worker.WorkerFactory;
import jakarta.inject.Singleton;

/**
 * Micronaut bean factory for Temporal-related singletons.
 */
@Factory
public class TemporalBeanFactory {

  @Singleton
  public WorkerFactory workerFactory(final WorkflowClient workflowClient) {
    return WorkerFactory.newInstance(workflowClient);
  }

}
