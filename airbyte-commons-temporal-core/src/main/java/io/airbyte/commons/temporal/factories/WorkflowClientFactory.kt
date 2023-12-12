package io.airbyte.commons.temporal.factories

import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowClientOptions
import io.temporal.serviceclient.WorkflowServiceStubs

/**
 * Factory class to build WorkflowClient.
 */
class WorkflowClientFactory {
  fun createWorkflowClient(
    workflowServiceStubs: WorkflowServiceStubs,
    namespace: String,
  ): WorkflowClient {
    return WorkflowClient.newInstance(
      workflowServiceStubs,
      WorkflowClientOptions.newBuilder().setNamespace(namespace).build(),
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
