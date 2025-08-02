/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.stubs

import io.airbyte.commons.temporal.TemporalConstants
import io.temporal.activity.ActivityCancellationType
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.activity.ActivityOptions
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import java.time.Duration

/**
 * Workflow used for testing cancellations and heartbeats.
 */
@WorkflowInterface
interface TestWorkflow1 {
  @WorkflowMethod
  fun execute()

  class TestWorkflowImpl : TestWorkflow1 {
    private val options: ActivityOptions =
      ActivityOptions
        .newBuilder()
        .setScheduleToCloseTimeout(Duration.ofDays(1))
        .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
        .setRetryOptions(TemporalConstants.NO_RETRY)
        .build()

    private val testHeartbeatActivity: TestHeartbeatActivity =
      Workflow.newActivityStub(TestHeartbeatActivity::class.java, options)

    override fun execute() {
      testHeartbeatActivity.heartbeat()
    }
  }

  @ActivityInterface
  interface TestHeartbeatActivity {
    @ActivityMethod
    fun heartbeat()
  }

  class TestActivityImplTest(
    private val runnable: Runnable,
  ) : TestHeartbeatActivity {
    override fun heartbeat() {
      runnable.run()
    }
  }
}
