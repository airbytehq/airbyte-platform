/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.workflows

import io.airbyte.commons.temporal.annotations.TemporalActivityStub
import io.airbyte.workers.temporal.jobpostprocessing.JobPostProcessingInput
import io.airbyte.workers.temporal.jobpostprocessing.JobPostProcessingWorkflow
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod

@ActivityInterface
interface ValidateJobPostProcessingWorkflowStartedActivity {
  @ActivityMethod
  fun wasStarted()
}

/**
 * This is a test workflow to validate we are starting the post-processing workflow as expected.
 * Because the actual post-processing workflow may have sleep and evolve independently of the ConnectionManagerWorkflow,
 * this should help keeping the tests more stable.
 */
open class JobPostProcessingWorkflowStub : JobPostProcessingWorkflow {
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private lateinit var validateJobPostProcessingWorkflowStartedActivity: ValidateJobPostProcessingWorkflowStartedActivity

  override fun run(input: JobPostProcessingInput) {
    validateJobPostProcessingWorkflowStartedActivity.wasStarted()
  }
}
