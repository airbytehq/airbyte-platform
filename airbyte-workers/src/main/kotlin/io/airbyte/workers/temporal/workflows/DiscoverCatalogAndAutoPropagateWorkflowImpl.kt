package io.airbyte.workers.temporal.workflows

import com.google.common.annotations.VisibleForTesting
import datadog.trace.api.Trace
import io.airbyte.commons.temporal.annotations.TemporalActivityStub
import io.airbyte.commons.temporal.scheduling.DiscoverCatalogAndAutoPropagateWorkflow
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.metrics.lib.ApmTraceConstants
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.PostprocessCatalogInput
import io.airbyte.workers.models.RefreshSchemaActivityOutput
import io.airbyte.workers.temporal.discover.catalog.DiscoverCatalogActivity

open class DiscoverCatalogAndAutoPropagateWorkflowImpl : DiscoverCatalogAndAutoPropagateWorkflow {
  @VisibleForTesting
  constructor(activity: DiscoverCatalogActivity) {
    this.activity = activity
  }

  constructor() {}

  @TemporalActivityStub(activityOptionsBeanName = "discoveryActivityOptions")
  private lateinit var activity: DiscoverCatalogActivity

  @Trace(operationName = ApmTraceConstants.WORKFLOW_TRACE_OPERATION_NAME)
  override fun run(
    jobRunConfig: JobRunConfig,
    launcherConfig: IntegrationLauncherConfig,
    config: StandardDiscoverCatalogInput,
  ): RefreshSchemaActivityOutput {
    ApmTraceUtils.addTagsToTrace(
      mapOf(
        ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY to jobRunConfig.attemptId,
        ApmTraceConstants.Tags.JOB_ID_KEY to jobRunConfig.jobId,
        ApmTraceConstants.Tags.DOCKER_IMAGE_KEY to launcherConfig.dockerImage,
      ),
    )
    val workloadResult =
      try {
        activity.runWithWorkload(
          DiscoverCatalogInput(
            jobRunConfig,
            launcherConfig,
            config,
          ),
        )
      } catch (e: WorkerException) {
        activity.reportFailure(true)
        throw RuntimeException(e)
      }

    if (workloadResult.discoverCatalogId == null) {
      return failure()
    }

    val postprocessResult = activity.postprocess(PostprocessCatalogInput(workloadResult.discoverCatalogId, launcherConfig.connectionId))
    if (postprocessResult.isFailure) {
      return failure()
    }

    activity.reportSuccess(true)
    return RefreshSchemaActivityOutput(postprocessResult.diff)
  }

  fun failure(): RefreshSchemaActivityOutput {
    activity.reportFailure(true)
    return RefreshSchemaActivityOutput(null)
  }
}
