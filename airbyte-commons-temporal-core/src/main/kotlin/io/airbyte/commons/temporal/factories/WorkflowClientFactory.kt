/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.factories

import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowClientOptions
import io.temporal.common.converter.DataConverter
import io.temporal.serviceclient.WorkflowServiceStubs

/**
 * Factory class to build WorkflowClient.
 */
class WorkflowClientFactory {
  fun createWorkflowClient(
    workflowServiceStubs: WorkflowServiceStubs,
    namespace: String,
    dataConverter: DataConverter,
  ): WorkflowClient {
    return WorkflowClient.newInstance(
      workflowServiceStubs,
      WorkflowClientOptions.newBuilder()
        .setDataConverter(dataConverter)
        .setNamespace(namespace).build(),
    )
  }

  fun createWorkflowClient(
    workflowServiceStubs: WorkflowServiceStubs,
    options: WorkflowClientOptions,
  ): WorkflowClient {
    return WorkflowClient.newInstance(
      workflowServiceStubs,
      options,
    )
  }
}
