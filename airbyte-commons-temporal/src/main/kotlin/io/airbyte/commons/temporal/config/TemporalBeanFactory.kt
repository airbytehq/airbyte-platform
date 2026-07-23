/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.config

import io.airbyte.commons.temporal.TemporalUtils
import io.airbyte.commons.temporal.WorkflowClientWrapped
import io.airbyte.commons.temporal.WorkflowServiceStubsWrapped
import io.airbyte.commons.temporal.factories.WorkflowClientFactory
import io.airbyte.metrics.MetricClient
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.micronaut.context.annotation.Factory
import io.temporal.client.WorkflowClient
import io.temporal.common.converter.DataConverter
import io.temporal.serviceclient.WorkflowServiceStubs
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.nio.file.Path

/**
 * Micronaut bean factory for Temporal-related singletons.
 */
@Factory
class TemporalBeanFactory {
  /**
   * WorkflowServiceStubs shouldn't be used directly, use WorkflowServiceStubsWrapped instead.
   */
  @Singleton
  fun temporalService(temporalUtils: TemporalUtils): WorkflowServiceStubs = temporalUtils.createTemporalService()

  @Singleton
  fun temporalServiceWrapped(
    workflowServiceStubs: WorkflowServiceStubs,
    metricClient: MetricClient,
  ): WorkflowServiceStubsWrapped = WorkflowServiceStubsWrapped(workflowServiceStubs, metricClient)

  /**
   * WorkflowClient shouldn't be used directly, use WorkflowClientWrapped instead.
   */
  @Singleton
  @Named("workerWorkflowClient")
  fun workflowClient(
    temporalUtils: TemporalUtils,
    temporalService: WorkflowServiceStubs,
    dataConverter: DataConverter,
  ): WorkflowClient = WorkflowClientFactory().createWorkflowClient(temporalService, temporalUtils.getNamespace(), dataConverter)

  @Singleton
  fun workflowClientWrapped(
    @Named("workerWorkflowClient") workflowClient: WorkflowClient,
    metricClient: MetricClient,
  ): WorkflowClientWrapped = WorkflowClientWrapped(workflowClient, metricClient)

  @Singleton
  @Named("workspaceRootTemporal")
  fun workspaceRoot(airbyteConfig: AirbyteConfig): Path = Path.of(airbyteConfig.workspaceRoot)
}
