/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.sync

import dev.failsafe.Failsafe
import dev.failsafe.RetryPolicy
import dev.failsafe.function.CheckedSupplier
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.config.ReplicationOutput
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.WorkloadType
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.Destination
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Source
import io.airbyte.featureflag.WorkloadHeartbeatTimeout
import io.airbyte.featureflag.WorkloadPollingInterval
import io.airbyte.featureflag.Workspace
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.exception.WorkloadLauncherException
import io.airbyte.workers.exception.WorkloadMonitorException
import io.airbyte.workers.internal.exception.DestinationException
import io.airbyte.workers.internal.exception.SourceException
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.pod.Metadata
import io.airbyte.workers.workload.DataplaneGroupResolver
import io.airbyte.workers.workload.WorkloadConstants
import io.airbyte.workers.workload.WorkloadIdGenerator
import io.airbyte.workers.workload.WorkloadOutputWriter
import io.airbyte.workers.workload.exception.DocStoreAccessException
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadCancelRequest
import io.airbyte.workload.api.domain.WorkloadCreateRequest
import io.airbyte.workload.api.domain.WorkloadLabel
import io.airbyte.workload.api.domain.WorkloadStatus
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.HttpStatus
import org.openapitools.client.infrastructure.ServerException
import java.io.IOException
import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CancellationException
import kotlin.jvm.optionals.getOrNull
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

private val HTTP_CONFLICT_CODE = HttpStatus.CONFLICT.getCode()
private const val DESTINATION = "destination"
private const val SOURCE = "source"
private const val WORKLOAD_LAUNCHER = "workload-launcher"

private val WORKLOAD_MONITOR = setOf("workload-monitor-start", "workload-monitor-claim", "workload-monitor-heartbeat")

private val log = KotlinLogging.logger {}
private val TERMINAL_STATUSES = setOf(WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS)

class WorkloadApiWorker(
  private val workloadOutputWriter: WorkloadOutputWriter,
  private val workloadApiClient: WorkloadApiClient,
  private val workloadClient: WorkloadClient,
  private val workloadIdGenerator: WorkloadIdGenerator,
  private val input: ReplicationActivityInput,
  private val featureFlagClient: FeatureFlagClient,
  private val logClientManager: LogClientManager,
  private val dataplaneGroupResolver: DataplaneGroupResolver,
) {
  private var workloadId: String? = null

  // TODO Migrate test before deleting to ensure we do not lose coverage
  @Deprecated("")
  fun run(
    replicationInput: ReplicationInput,
    jobRoot: Path,
  ): ReplicationOutput {
    val workloadId = createWorkload(replicationInput, jobRoot)
    waitForWorkload(workloadId)
    return getOutput(workloadId)
  }

  @Throws(WorkerException::class)
  fun createWorkload(
    replicationInput: ReplicationInput,
    jobRoot: Path,
  ): String {
    val serializedInput = Jsons.serialize(input)
    workloadId =
      workloadIdGenerator.generateSyncWorkloadId(
        replicationInput.connectionId,
        replicationInput.jobRunConfig.jobId.toLong(),
        replicationInput.jobRunConfig.attemptId.toInt(),
      )

    val context = replicationInput.connectionContext
    val organizationId: UUID = context?.organizationId!!
    val workspaceId = context.workspaceId
    val connectionId = context.connectionId

    val dataplaneGroup =
      dataplaneGroupResolver.resolveForSync(
        organizationId,
        workspaceId,
        connectionId,
      )

    log.info { "Creating workload $workloadId" }

    val workloadCreateRequest =
      WorkloadCreateRequest(
        workloadId = workloadId!!,
        // This list copied from KubeProcess#getLabels() without docker image labels which we populate from the launcher
        labels =
          listOf<WorkloadLabel>(
            WorkloadLabel(Metadata.CONNECTION_ID_LABEL_KEY, replicationInput.connectionId.toString()),
            WorkloadLabel(Metadata.JOB_LABEL_KEY, replicationInput.jobRunConfig.jobId),
            WorkloadLabel(Metadata.ATTEMPT_LABEL_KEY, replicationInput.jobRunConfig.attemptId.toString()),
            WorkloadLabel(Metadata.WORKSPACE_LABEL_KEY, replicationInput.workspaceId.toString()),
            WorkloadLabel(Metadata.WORKER_POD_LABEL_KEY, Metadata.WORKER_POD_LABEL_VALUE),
          ),
        workloadInput = serializedInput,
        logPath = logClientManager.fullLogPath(jobRoot),
        type = WorkloadType.SYNC,
        priority = WorkloadPriority.DEFAULT,
        workspaceId = workspaceId,
        organizationId = organizationId,
        mutexKey = replicationInput.connectionId.toString(),
        deadline = null,
        signalInput = replicationInput.signalInput,
        dataplaneGroup = dataplaneGroup,
      )

    // Create the workload
    try {
      workloadClient.createWorkload(workloadCreateRequest)
    } catch (e: ServerException) {
      if (e.statusCode != HTTP_CONFLICT_CODE) {
        throw e
      } else {
        log.info { "Workload $workloadId has already been created, reconnecting..." }
      }
    }
    return workloadId!!
  }

  fun isWorkloadTerminal(workloadId: String): Boolean {
    // TODO handle error
    val workload = getWorkload(workloadId)
    return workload.status != null && TERMINAL_STATUSES.contains(workload.status)
  }

  fun waitForWorkload(workloadId: String) {
    // Wait until workload reaches a terminal status
    // TODO merge this with WorkloadApiHelper.waitForWorkload. The only difference currently is the progress log.
    var i = 0
    val sleepInterval = Duration.ofSeconds(featureFlagClient.intVariation(WorkloadPollingInterval, getFeatureFlagContext()).toLong())
    var workload: Workload
    while (true) {
      workload = getWorkload(workloadId)

      if (workload.status != null) {
        if (TERMINAL_STATUSES.contains(workload.status)) {
          log.info { "Workload $workloadId has returned a terminal status of ${workload.status}.  Fetching output..." }
          break
        }

        if (i % 5 == 0) {
          i++
          // Since syncs are mostly in a running state this can spam logs while providing no actionable information
          if (workload.status != WorkloadStatus.RUNNING) {
            log.info { "Workload $workloadId is ${workload.status}" }
          }
        }
        i++
      }
      sleep(sleepInterval.toMillis())
    }

    if (workload.status == WorkloadStatus.CANCELLED) {
      throw CancellationException("Replication cancelled by " + workload.terminationSource)
    }
  }

  @Throws(IOException::class)
  fun cancelWorkload(workloadId: String) {
    callWithRetry<Boolean> {
      workloadApiClient
        .workloadCancel(
          WorkloadCancelRequest(
            workloadId,
            WorkloadConstants.WORKLOAD_CANCELLED_BY_USER_REASON,
            "WorkloadApiWorker",
          ),
        )
      true
    }
  }

  @Throws(WorkerException::class)
  fun getOutput(workloadId: String): ReplicationOutput {
    val workload = getWorkload(workloadId)
    val output: ReplicationOutput? =
      try {
        getReplicationOutput(workloadId)
      } catch (e: Exception) {
        throwFallbackError(workload, e)
        throw WorkerException("Failed to read replication output", e)
      }

    if (output == null) {
      // If we fail to read the output, fallback to throwing an exception based on the status of the workload
      throwFallbackError(workload, null)
      throw WorkerException("Replication output is empty")
    }

    return output
  }

  @Throws(WorkerException::class)
  private fun throwFallbackError(
    workload: Workload,
    e: Exception?,
  ) {
    if (workload.status == WorkloadStatus.FAILURE) {
      when (workload.terminationSource) {
        SOURCE -> throw SourceException(workload.terminationReason, e)
        DESTINATION -> throw DestinationException(workload.terminationReason, e)
        WORKLOAD_LAUNCHER -> throw WorkloadLauncherException(workload.terminationReason)
        in WORKLOAD_MONITOR -> throw WorkloadMonitorException(workload.terminationReason)
        else -> throw WorkerException(workload.terminationReason, e)
      }
    }
  }

  fun cancel() {
    try {
      workloadId?.let { cancelWorkload(it) }
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
  }

  private fun getReplicationOutput(workloadId: String): ReplicationOutput? {
    val output =
      fetchReplicationOutput(
        workloadId,
        { location ->
          try {
            workloadOutputWriter.readSyncOutput(location).getOrNull()
          } catch (e: DocStoreAccessException) {
            throw RuntimeException(e)
          }
        },
      )

    log.debug { "Replication output for workload $workloadId : $output" }
    return output
  }

  private fun fetchReplicationOutput(
    location: String,
    replicationFetcher: (String) -> ReplicationOutput?,
  ): ReplicationOutput? = replicationFetcher(location)

  private fun getFeatureFlagContext(): Context =
    Multi(
      listOf(
        Workspace(input.workspaceId!!),
        Connection(input.connectionId!!),
        Source(input.sourceId!!),
        Destination(input.destinationId!!),
      ),
    )

  private fun getWorkload(workloadId: String): Workload = callWithRetry<Workload> { workloadApiClient.workloadGet(workloadId) }

  /**
   * This method is aiming to mimic the behavior of the heartbeat which only fails after its timeout
   * is reach. This allows to be more resilient to a workloadApi downtime
   *
   * @param workloadApiCall A supplier calling the API
   * @return the result of the API call
   */
  private fun <T> callWithRetry(workloadApiCall: CheckedSupplier<T>): T {
    val timeoutDuration = featureFlagClient.intVariation(WorkloadHeartbeatTimeout, getFeatureFlagContext()).minutes
    return Failsafe
      .with(
        RetryPolicy
          .builder<T>()
          .withDelay(30.seconds.toJavaDuration())
          .withMaxDuration(timeoutDuration.toJavaDuration())
          .build(),
      ).get<T>(workloadApiCall)
  }

  private fun sleep(millis: Long) {
    try {
      Thread.sleep(millis)
    } catch (e: InterruptedException) {
      Thread.currentThread().interrupt()
      throw RuntimeException(e)
    }
  }
}
