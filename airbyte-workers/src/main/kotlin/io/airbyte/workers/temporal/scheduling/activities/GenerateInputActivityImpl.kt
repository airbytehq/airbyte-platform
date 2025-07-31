/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import datadog.trace.api.Trace
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.CheckInput
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.micronaut.EnvConstants
import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.commons.temporal.utils.PayloadChecker
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.DESTINATION_DOCKER_IMAGE_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.SOURCE_DOCKER_IMAGE_KEY
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.ApmTraceUtils.formatTag
import io.airbyte.metrics.lib.MetricTags.ATTEMPT_NUMBER
import io.airbyte.metrics.lib.MetricTags.CONNECTION_ID
import io.airbyte.metrics.lib.MetricTags.JOB_ID
import io.airbyte.workers.models.JobInput
import io.airbyte.workers.models.SyncJobCheckConnectionInputs
import io.airbyte.workers.temporal.scheduling.activities.GenerateInputActivity.SyncInputWithAttemptNumber
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpStatus
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import java.io.IOException
import java.util.Map

/**
 * Generate input for a workflow.
 */
@Singleton
@Requires(env = [EnvConstants.CONTROL_PLANE])
open class GenerateInputActivityImpl(
  private val airbyteApiClient: AirbyteApiClient,
  private val payloadChecker: PayloadChecker,
) : GenerateInputActivity {
  override fun getCheckConnectionInputs(input: SyncInputWithAttemptNumber): SyncJobCheckConnectionInputs {
    try {
      val checkInput =
        airbyteApiClient.jobsApi
          .getCheckInput(CheckInput(input.jobId, input.attemptNumber))
      return payloadChecker.validatePayloadSize<SyncJobCheckConnectionInputs>(
        Jsons.convertValue<SyncJobCheckConnectionInputs>(checkInput, SyncJobCheckConnectionInputs::class.java),
      )
    } catch (e: ClientException) {
      if (e.statusCode == HttpStatus.NOT_FOUND.getCode()) {
        throw e
      }
      throw RetryableException(e)
    } catch (e: IOException) {
      throw RetryableException(e)
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Throws(IOException::class)
  override fun getSyncWorkflowInput(input: GenerateInputActivity.SyncInput): JobInput {
    val jobInputResult =
      airbyteApiClient.jobsApi.getJobInput(
        io.airbyte.api.client.model.generated
          .SyncInput(input.jobId, input.attemptId),
      )
    val jobInput: JobInput = Jsons.convertValue(jobInputResult, JobInput::class.java)

    var attrs: MutableList<MetricAttribute> = mutableListOf()
    try {
      attrs =
        mutableListOf<MetricAttribute>(
          MetricAttribute(formatTag(CONNECTION_ID), jobInput.destinationLauncherConfig!!.getConnectionId().toString()),
          MetricAttribute(formatTag(JOB_ID), jobInput.jobRunConfig!!.getJobId()),
          MetricAttribute(formatTag(ATTEMPT_NUMBER), jobInput.jobRunConfig!!.getAttemptId().toString()),
          MetricAttribute(formatTag(DESTINATION_DOCKER_IMAGE_KEY), jobInput.destinationLauncherConfig!!.getDockerImage()),
          MetricAttribute(formatTag(SOURCE_DOCKER_IMAGE_KEY), jobInput.sourceLauncherConfig!!.getDockerImage()),
        )
    } catch (e: NullPointerException) {
      // This shouldn't happen, but for good measure we're catching, because I don't want to introduce an
      // NPE in the critical path.
    }

    return payloadChecker.validatePayloadSize<JobInput?>(jobInput, attrs.toTypedArray())!!
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Throws(IOException::class)
  override fun getSyncWorkflowInputWithAttemptNumber(input: SyncInputWithAttemptNumber): JobInput {
    ApmTraceUtils.addTagsToTrace(Map.of<String?, Long?>(JOB_ID_KEY, input.jobId))
    return getSyncWorkflowInput(
      GenerateInputActivity.SyncInput(
        input.attemptNumber,
        input.jobId,
      ),
    )
  }
}
