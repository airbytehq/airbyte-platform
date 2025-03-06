/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.stubs;

import io.airbyte.commons.temporal.TemporalConstants;
import io.temporal.activity.ActivityCancellationType;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.time.Duration;

/**
 * Workflow used for testing cancellations and heartbeats.
 */

@WorkflowInterface
public interface TestWorkflow {

  @WorkflowMethod
  void execute();

  class TestWorkflowImpl implements TestWorkflow {

    private final ActivityOptions options = ActivityOptions.newBuilder()
        .setScheduleToCloseTimeout(Duration.ofDays(1))
        .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
        .setRetryOptions(TemporalConstants.NO_RETRY)
        .build();

    private final TestHeartbeatActivity testHeartbeatActivity = Workflow.newActivityStub(TestHeartbeatActivity.class, options);

    @Override
    public void execute() {
      testHeartbeatActivity.heartbeat();
    }

  }

  @ActivityInterface
  interface TestHeartbeatActivity {

    @ActivityMethod
    void heartbeat();

  }

  class TestActivityImplTest implements TestHeartbeatActivity {

    private final Runnable runnable;

    public TestActivityImplTest(final Runnable runnable) {
      this.runnable = runnable;
    }

    @Override
    public void heartbeat() {
      runnable.run();
    }

  }

}
