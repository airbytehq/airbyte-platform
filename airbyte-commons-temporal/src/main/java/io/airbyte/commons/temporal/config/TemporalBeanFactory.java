/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.config;

import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.commons.temporal.WorkflowClientWrapped;
import io.airbyte.commons.temporal.WorkflowServiceStubsWrapped;
import io.airbyte.commons.temporal.factories.WorkflowClientFactory;
import io.airbyte.metrics.lib.MetricClient;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import io.temporal.client.WorkflowClient;
import io.temporal.common.converter.DataConverter;
import io.temporal.serviceclient.WorkflowServiceStubs;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.file.Path;

/**
 * Micronaut bean factory for Temporal-related singletons.
 */
@Factory
public class TemporalBeanFactory {

  /**
   * WorkflowServiceStubs shouldn't be used directly, use WorkflowServiceStubsWrapped instead.
   */
  @Singleton
  WorkflowServiceStubs temporalService(final TemporalUtils temporalUtils, final TemporalSdkTimeouts temporalSdkTimeouts) {
    return temporalUtils.createTemporalService(temporalSdkTimeouts);
  }

  @Singleton
  public WorkflowServiceStubsWrapped temporalServiceWrapped(final WorkflowServiceStubs workflowServiceStubs, final MetricClient metricClient) {
    return new WorkflowServiceStubsWrapped(workflowServiceStubs, metricClient);
  }

  /**
   * WorkflowClient shouldn't be used directly, use WorkflowClientWrapped instead.
   */
  @Singleton
  @Named("workerWorkflowClient")
  WorkflowClient workflowClient(
                                final TemporalUtils temporalUtils,
                                final WorkflowServiceStubs temporalService,
                                final DataConverter dataConverter) {
    return new WorkflowClientFactory().createWorkflowClient(temporalService, temporalUtils.getNamespace(), dataConverter);
  }

  @Singleton
  public WorkflowClientWrapped workflowClientWrapped(@Named("workerWorkflowClient") final WorkflowClient workflowClient,
                                                     final MetricClient metricClient) {
    return new WorkflowClientWrapped(workflowClient, metricClient);
  }

  @Singleton
  @Named("workspaceRootTemporal")
  public Path workspaceRoot(@Value("${airbyte.workspace.root}") final String workspaceRoot) {
    return Path.of(workspaceRoot);
  }

}
