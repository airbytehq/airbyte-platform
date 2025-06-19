/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import com.google.common.annotations.VisibleForTesting
import datadog.trace.api.Trace
import io.airbyte.commons.json.Jsons
import io.airbyte.config.FailureReason
import io.airbyte.config.ReplicationAttemptSummary
import io.airbyte.config.ReplicationOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.ApmTraceConstants
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.helper.FailureHelper.destinationFailure
import io.airbyte.workers.helper.FailureHelper.platformFailure
import io.airbyte.workers.helper.FailureHelper.sourceFailure
import io.airbyte.workers.internal.exception.DestinationException
import io.airbyte.workers.internal.exception.SourceException
import io.airbyte.workers.workload.WorkloadOutputWriter
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.client.model.generated.WorkloadCancelRequest
import io.airbyte.workload.api.client.model.generated.WorkloadFailureRequest
import io.airbyte.workload.api.client.model.generated.WorkloadSuccessRequest
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.IOException
import java.nio.file.Path
import java.time.Duration
import java.util.Optional
import java.util.function.ToDoubleFunction

private val logger = KotlinLogging.logger {}

@VisibleForTesting
internal val BYTES_TO_GB: Double = (1024 * 1024 * 1024).toDouble()

@VisibleForTesting
internal const val STATUS_ATTRIBUTE: String = "status"

@VisibleForTesting
internal const val FAILED_STATUS: String = "failed"

@VisibleForTesting
internal const val SUCCESS_STATUS: String = "success"

private val BYTES_GAUGE_FUNCTION =
  ToDoubleFunction { replicationAttemptSummary: ReplicationAttemptSummary -> (replicationAttemptSummary.bytesSynced ?: 0) / BYTES_TO_GB }

private val DURATION_GAUGE_FUNCTION =
  ToDoubleFunction { replicationAttemptSummary: ReplicationAttemptSummary ->
    Duration.ofMillis(replicationAttemptSummary.endTime - replicationAttemptSummary.startTime).toSeconds().toDouble()
  }

/**
 * Runs replication worker.
 */
@Singleton
class ReplicationJobOrchestrator(
  private val replicationInput: ReplicationInput,
  @Named("workloadId") private val workloadId: String,
  @Named("jobRoot") private val jobRoot: Path,
  private val jobRunConfig: JobRunConfig,
  private val replicationWorker: ReplicationWorker,
  private val workloadApiClient: WorkloadApiClient,
  private val outputWriter: WorkloadOutputWriter,
  private val metricClient: MetricClient,
) {
  @Trace(operationName = ApmTraceConstants.JOB_ORCHESTRATOR_OPERATION_NAME)
  @Throws(Exception::class)
  fun runJob(): Optional<String> {
    val sourceLauncherConfig = replicationInput.sourceLauncherConfig
    val destinationLauncherConfig = replicationInput.destinationLauncherConfig

    ApmTraceUtils.addTagsToTrace(
      mutableMapOf<String, Any>(
        ApmTraceConstants.Tags.IS_RESET_KEY to replicationInput.isReset,
        ApmTraceConstants.Tags.JOB_ID_KEY to jobRunConfig.jobId,
        ApmTraceConstants.Tags.DESTINATION_DOCKER_IMAGE_KEY to destinationLauncherConfig.dockerImage,
        ApmTraceConstants.Tags.SOURCE_DOCKER_IMAGE_KEY to sourceLauncherConfig.dockerImage,
      ),
    )

    logger.info { "Running replication worker..." }

    val replicationOutput =
      run(replicationWorker, replicationInput, jobRoot, workloadId)

    outputWriter.writeSyncOutput(workloadId, replicationOutput)
    updateStatusInWorkloadApi(replicationOutput, workloadId)

    val attributes =
      buildMetricAttributes(
        replicationInput,
        jobRunConfig.jobId.toLong(),
        jobRunConfig.attemptId.toInt(),
      )
    metricClient.gauge<ReplicationAttemptSummary>(
      OssMetricsRegistry.SYNC_DURATION,
      replicationOutput.replicationAttemptSummary,
      DURATION_GAUGE_FUNCTION,
      *attributes.toTypedArray<MetricAttribute>(),
    )
    metricClient.gauge<ReplicationAttemptSummary>(
      OssMetricsRegistry.SYNC_GB_MOVED,
      replicationOutput.replicationAttemptSummary,
      BYTES_GAUGE_FUNCTION,
      *attributes.toTypedArray<MetricAttribute>(),
    )

    logger.info { "Returning output..." }
    return Optional.of(Jsons.serialize(replicationOutput))
  }

  private fun run(
    replicationWorker: ReplicationWorker,
    replicationInput: ReplicationInput,
    jobRoot: Path,
    workloadId: String,
  ): ReplicationOutput {
    val jobId = jobRunConfig.jobId.toLong()
    val attemptNumber = Math.toIntExact(jobRunConfig.attemptId)
    val attributes = buildMetricAttributes(replicationInput, jobId, attemptNumber)
    try {
      return replicationWorker.runReplicationBlocking(jobRoot)
    } catch (e: DestinationException) {
      failWorkload(workloadId = workloadId, failureReason = destinationFailure(e, jobId, attemptNumber), originalException = e)
      attributes.add(MetricAttribute(STATUS_ATTRIBUTE, FAILED_STATUS))
      throw e
    } catch (e: SourceException) {
      failWorkload(workloadId = workloadId, failureReason = sourceFailure(e, jobId, attemptNumber), originalException = e)
      attributes.add(MetricAttribute(STATUS_ATTRIBUTE, FAILED_STATUS))
      throw e
    } catch (e: WorkerException) {
      failWorkload(workloadId = workloadId, failureReason = platformFailure(e, jobId, attemptNumber), originalException = e)
      attributes.add(MetricAttribute(STATUS_ATTRIBUTE, FAILED_STATUS))
      throw RuntimeException(e)
    } catch (e: Exception) {
      failWorkload(workloadId = workloadId, failureReason = platformFailure(e, jobId, attemptNumber), originalException = e)
      throw RuntimeException(e)
    } finally {
      if (attributes.stream().noneMatch { a: MetricAttribute? -> STATUS_ATTRIBUTE.equals(a!!.key, ignoreCase = true) }) {
        attributes.add(MetricAttribute(STATUS_ATTRIBUTE, SUCCESS_STATUS))
      }
      metricClient.count(metric = OssMetricsRegistry.SYNC_STATUS, attributes = attributes.toTypedArray<MetricAttribute>())
    }
  }

  @VisibleForTesting
  @Throws(IOException::class)
  fun updateStatusInWorkloadApi(
    replicationOutput: ReplicationOutput?,
    workloadId: String,
  ) {
    if (replicationOutput == null || replicationOutput.replicationAttemptSummary == null) {
      logger.warn { "The replication output is null, skipping updating the workload status via API" }
      return
    }
    when (replicationOutput.replicationAttemptSummary.status) {
      StandardSyncSummary.ReplicationStatus.FAILED -> failWorkload(workloadId, replicationOutput.failures.first())
      StandardSyncSummary.ReplicationStatus.CANCELLED -> cancelWorkload(workloadId)
      StandardSyncSummary.ReplicationStatus.COMPLETED -> succeedWorkload(workloadId)
      else -> throw RuntimeException("Unknown status ${replicationOutput.replicationAttemptSummary.status}.")
    }
  }

  @Throws(IOException::class)
  private fun cancelWorkload(workloadId: String) {
    workloadApiClient.workloadApi.workloadCancel(WorkloadCancelRequest(workloadId, "Replication job has been cancelled", "orchestrator"))
  }

  private fun failWorkload(
    workloadId: String,
    failureReason: FailureReason?,
    originalException: Exception,
  ) {
    try {
      failWorkload(workloadId, failureReason)
    } catch (ioe: IOException) {
      originalException.addSuppressed(ioe)
    }
  }

  @Throws(IOException::class)
  private fun failWorkload(
    workloadId: String,
    failureReason: FailureReason?,
  ) {
    if (failureReason != null) {
      workloadApiClient.workloadApi.workloadFailure(
        WorkloadFailureRequest(
          workloadId,
          failureReason.failureOrigin.value(),
          failureReason.externalMessage,
        ),
      )
    } else {
      workloadApiClient.workloadApi.workloadFailure(WorkloadFailureRequest(workloadId, null, null))
    }
  }

  @Throws(IOException::class)
  private fun succeedWorkload(workloadId: String) {
    workloadApiClient.workloadApi.workloadSuccess(WorkloadSuccessRequest(workloadId))
  }

  @VisibleForTesting
  fun buildMetricAttributes(
    replicationInput: ReplicationInput,
    jobId: Long,
    attemptNumber: Int,
  ) = mutableListOf(
    MetricAttribute("attempt_count", (attemptNumber + 1).toString()), // Normalize to make it human understandable
    MetricAttribute("connection_id", replicationInput.connectionId.toString()),
    MetricAttribute("workspace_id", replicationInput.workspaceId.toString()),
    MetricAttribute("job_id", jobId.toString()),
    MetricAttribute("destination_connector_id", replicationInput.destinationId.toString()),
    MetricAttribute("source_connector_id", replicationInput.sourceId.toString()),
  )
}
