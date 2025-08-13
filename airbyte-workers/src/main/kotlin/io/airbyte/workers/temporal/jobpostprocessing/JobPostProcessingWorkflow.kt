/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.jobpostprocessing

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import io.airbyte.commons.temporal.annotations.TemporalActivityStub
import io.airbyte.config.JobStatus
import io.airbyte.workers.temporal.scheduling.activities.EvaluateOutlierInput
import io.airbyte.workers.temporal.scheduling.activities.FinalizeJobStatsInput
import io.airbyte.workers.temporal.scheduling.activities.JobPostProcessingActivity
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import java.time.Duration
import java.util.UUID

@JsonDeserialize(builder = JobPostProcessingInput.Builder::class)
data class JobPostProcessingInput(
  val jobId: Long,
  val connectionId: UUID,
  val jobStatus: JobStatus?,
) {
  class Builder
    @JvmOverloads
    constructor(
      var jobId: Long? = null,
      var connectionId: UUID? = null,
      var jobStatus: JobStatus? = null,
    ) {
      fun jobId(jobId: Long) = apply { this.jobId = jobId }

      fun connectionId(connectionId: UUID) = apply { this.connectionId = connectionId }

      fun jobStatus(jobStatus: JobStatus) = apply { this.jobStatus = jobStatus }

      fun build(): JobPostProcessingInput =
        JobPostProcessingInput(
          jobId = jobId ?: throw IllegalStateException("jobId must be specified"),
          connectionId = connectionId ?: throw IllegalStateException("connectionId must be specified"),
          jobStatus = jobStatus,
        )
    }
}

@WorkflowInterface
interface JobPostProcessingWorkflow {
  @WorkflowMethod
  fun run(input: JobPostProcessingInput)
}

open class JobPostProcessingWorkflowImpl : JobPostProcessingWorkflow {
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private lateinit var jobPostProcessingActivity: JobPostProcessingActivity

  override fun run(input: JobPostProcessingInput) {
    if (input.jobStatus != JobStatus.SUCCEEDED) {
      // Delay to account for stats delay for non-successful jobs
      Workflow.sleep(Duration.ofMinutes(10))
    }

    jobPostProcessingActivity.finalizeJobStats(FinalizeJobStatsInput(input.jobId, input.connectionId))

    jobPostProcessingActivity.evaluateOutlier(EvaluateOutlierInput(input.jobId, input.connectionId))
  }
}
