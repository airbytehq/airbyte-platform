/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal;

import io.airbyte.commons.temporal.stubs.TestWorkflow;
import io.airbyte.commons.temporal.stubs.TestWorkflow.TestActivityImplTest;
import io.airbyte.commons.temporal.stubs.TestWorkflow.TestWorkflowImpl;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CancellationHandlerTest {

  @Test
  void testCancellationHandler() {

    final TestWorkflowEnvironment testEnv = TestWorkflowEnvironment.newInstance();

    final Worker worker = testEnv.newWorker("task-queue");

    worker.registerWorkflowImplementationTypes(TestWorkflowImpl.class);
    final WorkflowClient client = testEnv.getWorkflowClient();

    worker.registerActivitiesImplementations(new TestActivityImplTest(() -> {
      final ActivityExecutionContext context = Activity.getExecutionContext();
      new CancellationHandler.TemporalCancellationHandler(context).checkAndHandleCancellation(() -> {});
    }));

    testEnv.start();

    final TestWorkflow testWorkflow = client.newWorkflowStub(
        TestWorkflow.class,
        WorkflowOptions.newBuilder()
            .setTaskQueue("task-queue")
            .build());

    Assertions.assertDoesNotThrow(testWorkflow::execute);

  }

}
