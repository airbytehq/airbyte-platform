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
import java.util.Map

open class DiscoverCatalogAndAutoPropagateWorkflowImpl : DiscoverCatalogAndAutoPropagateWorkflow {
  @VisibleForTesting
  constructor(activity: DiscoverCatalogActivity) {
    this.activity = activity
  }

  constructor() {}

  init {
  }

  @TemporalActivityStub(activityOptionsBeanName = "discoveryActivityOptions")
  private lateinit var activity: DiscoverCatalogActivity

  @Trace(operationName = ApmTraceConstants.WORKFLOW_TRACE_OPERATION_NAME)
  override fun run(
    jobRunConfig: JobRunConfig,
    launcherConfig: IntegrationLauncherConfig,
    config: StandardDiscoverCatalogInput,
  ): RefreshSchemaActivityOutput {
    ApmTraceUtils.addTagsToTrace(
      Map.of<String, Any>(
        ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY,
        jobRunConfig.attemptId,
        ApmTraceConstants.Tags.JOB_ID_KEY,
        jobRunConfig.jobId,
        ApmTraceConstants.Tags.DOCKER_IMAGE_KEY,
        launcherConfig.dockerImage,
      ),
    )
    val result =
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

    if (result.discoverCatalogId != null) {
      val result = activity.postprocess(PostprocessCatalogInput(result.discoverCatalogId, launcherConfig.connectionId, launcherConfig.workspaceId))
      if (result.isSuccess) {
        activity.reportSuccess(true)
        return RefreshSchemaActivityOutput(result.diff!!)
      } else {
        return failure()
      }
    } else {
      return failure()
    }
  }

  fun failure(): RefreshSchemaActivityOutput {
    activity.reportFailure(true)
    return RefreshSchemaActivityOutput(null)
  }
}
