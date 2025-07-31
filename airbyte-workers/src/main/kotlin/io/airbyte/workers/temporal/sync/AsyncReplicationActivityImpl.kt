/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync

import com.google.common.annotations.VisibleForTesting
import datadog.trace.api.Trace
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.commons.logging.LogSource
import io.airbyte.commons.logging.MdcScope
import io.airbyte.commons.temporal.TemporalUtils
import io.airbyte.commons.temporal.utils.PayloadChecker
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ReplicationAttemptSummary
import io.airbyte.config.ReplicationOutput
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.DESTINATION_DOCKER_IMAGE_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.REPLICATION_BYTES_SYNCED_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.REPLICATION_RECORDS_SYNCED_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.REPLICATION_STATUS_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.SOURCE_DOCKER_IMAGE_KEY
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.ApmTraceUtils.addActualRootCauseToTrace
import io.airbyte.metrics.lib.ApmTraceUtils.formatTag
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.input.ReplicationInputMapper
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.storage.activities.OutputStorageClient
import io.airbyte.workers.sync.WorkloadApiWorker
import io.airbyte.workers.sync.WorkloadClient
import io.airbyte.workers.workload.DataplaneGroupResolver
import io.airbyte.workers.workload.WorkloadIdGenerator
import io.airbyte.workers.workload.WorkloadOutputWriter
import io.airbyte.workload.api.client.WorkloadApiClient
import io.temporal.activity.Activity
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.util.UUID

/**
 * Replication temporal activity impl.
 */
@Singleton
class AsyncReplicationActivityImpl : AsyncReplicationActivity {
  private val replicationInputMapper: ReplicationInputMapper
  private val workspaceRoot: Path
  private val workloadApiClient: WorkloadApiClient
  private val workloadClient: WorkloadClient
  private val workloadOutputWriter: WorkloadOutputWriter
  private val workloadIdGenerator: WorkloadIdGenerator
  private val metricClient: MetricClient
  private val featureFlagClient: FeatureFlagClient
  private val payloadChecker: PayloadChecker
  private val catalogStorageClient: OutputStorageClient<ConfiguredAirbyteCatalog>
  private val logClientManager: LogClientManager
  private val dataplaneGroupResolver: DataplaneGroupResolver

  constructor(
    @Named("workspaceRoot") workspaceRoot: Path,
    workloadOutputWriter: WorkloadOutputWriter,
    workloadApiClient: WorkloadApiClient,
    workloadClient: WorkloadClient,
    workloadIdGenerator: WorkloadIdGenerator,
    metricClient: MetricClient,
    featureFlagClient: FeatureFlagClient,
    payloadChecker: PayloadChecker,
    @Named("outputCatalogClient") catalogStorageClient: OutputStorageClient<ConfiguredAirbyteCatalog>,
    logClientManager: LogClientManager,
    dataplaneGroupResolver: DataplaneGroupResolver,
  ) {
    this.replicationInputMapper = ReplicationInputMapper()
    this.workspaceRoot = workspaceRoot
    this.workloadOutputWriter = workloadOutputWriter
    this.workloadApiClient = workloadApiClient
    this.workloadClient = workloadClient
    this.workloadIdGenerator = workloadIdGenerator
    this.metricClient = metricClient
    this.featureFlagClient = featureFlagClient
    this.payloadChecker = payloadChecker
    this.catalogStorageClient = catalogStorageClient
    this.logClientManager = logClientManager
    this.dataplaneGroupResolver = dataplaneGroupResolver
  }

  @VisibleForTesting
  internal constructor(
    replicationInputMapper: ReplicationInputMapper,
    @Named("workspaceRoot") workspaceRoot: Path,
    workloadOutputWriter: WorkloadOutputWriter,
    workloadApiClient: WorkloadApiClient,
    workloadClient: WorkloadClient,
    workloadIdGenerator: WorkloadIdGenerator,
    metricClient: MetricClient,
    featureFlagClient: FeatureFlagClient,
    payloadChecker: PayloadChecker,
    @Named("outputCatalogClient") catalogStorageClient: OutputStorageClient<ConfiguredAirbyteCatalog>,
    logClientManager: LogClientManager,
    dataplaneGroupResolver: DataplaneGroupResolver,
  ) {
    this.replicationInputMapper = replicationInputMapper
    this.workspaceRoot = workspaceRoot
    this.workloadOutputWriter = workloadOutputWriter
    this.workloadApiClient = workloadApiClient
    this.workloadClient = workloadClient
    this.workloadIdGenerator = workloadIdGenerator
    this.metricClient = metricClient
    this.featureFlagClient = featureFlagClient
    this.payloadChecker = payloadChecker
    this.catalogStorageClient = catalogStorageClient
    this.logClientManager = logClientManager
    this.dataplaneGroupResolver = dataplaneGroupResolver
  }

  @JvmRecord
  internal data class TracingContext(
    val connectionId: UUID?,
    val jobId: String?,
    val attemptNumber: Long?,
    val traceAttributes: MutableMap<String?, Any?>?,
  )

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun startReplication(replicationActivityInput: ReplicationActivityInput): String {
    val tracingContext = buildTracingContext(replicationActivityInput)
    val jobRoot = TemporalUtils.getJobRoot(workspaceRoot, tracingContext.jobId!!, tracingContext.attemptNumber!!)

    try {
      MdcScope
        .Builder()
        .setExtraMdcEntries(LogSource.PLATFORM.toMdc().toMap())
        .build()
        .use { mdcScope ->
          logClientManager.setJobMdc(jobRoot)
          metricClient.count(OssMetricsRegistry.ACTIVITY_REPLICATION)

          ApmTraceUtils.addTagsToTrace(tracingContext.traceAttributes!!)

          if (replicationActivityInput.isReset != null && replicationActivityInput.isReset!!) {
            metricClient.count(OssMetricsRegistry.RESET_REQUEST)
          }

          LOGGER.info("Starting async replication")

          val workerAndReplicationInput = getWorkerAndReplicationInput(replicationActivityInput)
          val worker = workerAndReplicationInput.worker

          LOGGER.debug("connection {}, input: {}", tracingContext.connectionId, workerAndReplicationInput.replicationInput)
          return worker.createWorkload(workerAndReplicationInput.replicationInput!!, jobRoot)
        }
    } catch (e: Exception) {
      addActualRootCauseToTrace(e)
      throw Activity.wrap(e)
    } finally {
      logClientManager.setJobMdc(null)
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun cancel(
    replicationActivityInput: ReplicationActivityInput,
    workloadId: String,
  ) {
    val tracingContext = buildTracingContext(replicationActivityInput)
    val jobRoot = TemporalUtils.getJobRoot(workspaceRoot, tracingContext.jobId!!, tracingContext.attemptNumber!!)

    MdcScope.Builder().setExtraMdcEntries(LogSource.PLATFORM.toMdc().toMap()).build().use { ignored ->
      logClientManager.setJobMdc(jobRoot)
      LOGGER.info("Canceling workload {}", workloadId)

      val workerAndReplicationInput = getWorkerAndReplicationInput(replicationActivityInput)
      val worker = workerAndReplicationInput.worker
      try {
        worker.cancelWorkload(workloadId)
      } catch (e: Exception) {
        throw RuntimeException(e)
      }
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun getReplicationOutput(
    replicationActivityInput: ReplicationActivityInput,
    workloadId: String,
  ): StandardSyncOutput {
    val tracingContext = buildTracingContext(replicationActivityInput)
    val jobRoot = TemporalUtils.getJobRoot(workspaceRoot, tracingContext.jobId!!, tracingContext.attemptNumber!!)

    try {
      MdcScope.Builder().setExtraMdcEntries(LogSource.PLATFORM.toMdc().toMap()).build().use { mdcScope ->
        logClientManager.setJobMdc(jobRoot)
        val workerAndReplicationInput = getWorkerAndReplicationInput(replicationActivityInput)
        val worker = workerAndReplicationInput.worker

        val output = worker.getOutput(workloadId)
        return finalizeOutput(replicationActivityInput, output)
      }
    } catch (e: Exception) {
      addActualRootCauseToTrace(e)
      throw Activity.wrap(e)
    } finally {
      logClientManager.setJobMdc(null)
    }
  }

  private fun finalizeOutput(
    replicationActivityInput: ReplicationActivityInput,
    attemptOutput: ReplicationOutput,
  ): StandardSyncOutput {
    val tracingContext = buildTracingContext(replicationActivityInput)
    ApmTraceUtils.addTagsToTrace(tracingContext.traceAttributes!!)

    val metricAttributes: Array<MetricAttribute> =
      tracingContext.traceAttributes.entries
        .mapNotNull { (key, value) -> MetricAttribute(formatTag(key), value.toString()) }
        .toSet()
        .toTypedArray()

    val standardSyncOutput = reduceReplicationOutput(attemptOutput, metricAttributes)

    val standardSyncOutputString = standardSyncOutput.toString()
    LOGGER.debug("sync summary: {}", standardSyncOutputString)
    if (standardSyncOutputString.length > MAX_TEMPORAL_MESSAGE_SIZE) {
      LOGGER.error(
        "Sync output exceeds the max temporal message size of {}, actual is {}.",
        MAX_TEMPORAL_MESSAGE_SIZE,
        standardSyncOutputString.length,
      )
    } else {
      LOGGER.debug("Sync summary length: {}", standardSyncOutputString.length)
    }

    val uri =
      catalogStorageClient.persist(
        attemptOutput.getOutputCatalog(),
        tracingContext.connectionId!!,
        tracingContext.jobId!!.toLong(),
        tracingContext.attemptNumber!!.toInt(),
        metricAttributes,
      )

    standardSyncOutput.setCatalogUri(uri)

    payloadChecker.validatePayloadSize<StandardSyncOutput?>(standardSyncOutput, metricAttributes)

    return standardSyncOutput
  }

  @JvmRecord
  data class WorkerAndReplicationInput(
    val worker: WorkloadApiWorker,
    val replicationInput: ReplicationInput?,
  )

  @VisibleForTesting
  fun getWorkerAndReplicationInput(replicationActivityInput: ReplicationActivityInput): WorkerAndReplicationInput {
    val replicationInput: ReplicationInput
    val worker: WorkloadApiWorker

    replicationInput = replicationInputMapper.toReplicationInput(replicationActivityInput)
    worker =
      WorkloadApiWorker(
        workloadOutputWriter,
        workloadApiClient,
        workloadClient,
        workloadIdGenerator,
        replicationActivityInput,
        featureFlagClient,
        logClientManager,
        dataplaneGroupResolver,
      )

    return WorkerAndReplicationInput(worker, replicationInput)
  }

  private fun reduceReplicationOutput(
    output: ReplicationOutput,
    metricAttributes: Array<MetricAttribute>,
  ): StandardSyncOutput {
    val standardSyncOutput = StandardSyncOutput()
    val syncSummary = StandardSyncSummary()
    val replicationSummary = output.getReplicationAttemptSummary()

    traceReplicationSummary(replicationSummary, metricAttributes)

    syncSummary.setBytesSynced(replicationSummary.getBytesSynced())
    syncSummary.setRecordsSynced(replicationSummary.getRecordsSynced())
    syncSummary.setStartTime(replicationSummary.getStartTime())
    syncSummary.setEndTime(replicationSummary.getEndTime())
    syncSummary.setStatus(replicationSummary.getStatus())
    syncSummary.setTotalStats(replicationSummary.getTotalStats())
    syncSummary.setStreamStats(replicationSummary.getStreamStats())
    syncSummary.setPerformanceMetrics(output.getReplicationAttemptSummary().getPerformanceMetrics())
    syncSummary.setStreamCount(
      output
        .getOutputCatalog()
        .streams.size
        .toLong(),
    )

    standardSyncOutput.setStandardSyncSummary(syncSummary)
    standardSyncOutput.setFailures(output.getFailures())

    return standardSyncOutput
  }

  private fun traceReplicationSummary(
    replicationSummary: ReplicationAttemptSummary?,
    metricAttributes: Array<MetricAttribute>,
  ) {
    if (replicationSummary == null) {
      return
    }

    val tags: MutableMap<String?, Any?> = HashMap<String?, Any?>()
    if (replicationSummary.getBytesSynced() != null) {
      tags.put(REPLICATION_BYTES_SYNCED_KEY, replicationSummary.getBytesSynced())
      metricClient.count(OssMetricsRegistry.REPLICATION_BYTES_SYNCED, replicationSummary.getBytesSynced(), *metricAttributes)
    }
    if (replicationSummary.getRecordsSynced() != null) {
      tags.put(REPLICATION_RECORDS_SYNCED_KEY, replicationSummary.getRecordsSynced())
      metricClient.count(OssMetricsRegistry.REPLICATION_RECORDS_SYNCED, replicationSummary.getRecordsSynced(), *metricAttributes)
    }
    if (replicationSummary.getStatus() != null) {
      tags.put(REPLICATION_STATUS_KEY, replicationSummary.getStatus().value())
    }
    if (replicationSummary.getStartTime() != null && replicationSummary.getEndTime() != null && replicationSummary.getBytesSynced() != null) {
      val elapsedMs = replicationSummary.getEndTime() - replicationSummary.getStartTime()
      if (elapsedMs > 0) {
        val elapsedSeconds = elapsedMs / 1000
        val throughput = replicationSummary.getBytesSynced() / elapsedSeconds
        metricClient.count(OssMetricsRegistry.REPLICATION_THROUGHPUT_BPS, throughput, *metricAttributes)
      }
    }
    if (!tags.isEmpty()) {
      ApmTraceUtils.addTagsToTrace(tags)
    }
  }

  private fun buildTracingContext(replicationActivityInput: ReplicationActivityInput): TracingContext {
    val connectionId = replicationActivityInput.connectionId
    val jobId = replicationActivityInput.jobRunConfig!!.getJobId()
    val attemptNumber = replicationActivityInput.jobRunConfig!!.getAttemptId()

    val traceAttributes =
      mutableMapOf<String?, Any?>(
        CONNECTION_ID_KEY to connectionId,
        JOB_ID_KEY to jobId,
        ATTEMPT_NUMBER_KEY to attemptNumber,
        DESTINATION_DOCKER_IMAGE_KEY to replicationActivityInput.destinationLauncherConfig!!.getDockerImage(),
        SOURCE_DOCKER_IMAGE_KEY to replicationActivityInput.sourceLauncherConfig!!.getDockerImage(),
      )

    return TracingContext(connectionId, jobId, attemptNumber, traceAttributes)
  }

  companion object {
    private val LOGGER: Logger = LoggerFactory.getLogger(AsyncReplicationActivityImpl::class.java)
    private val MAX_TEMPORAL_MESSAGE_SIZE = 2 * 1024 * 1024
  }
}
