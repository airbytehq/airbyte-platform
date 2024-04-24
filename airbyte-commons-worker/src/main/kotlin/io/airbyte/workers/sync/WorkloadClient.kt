package io.airbyte.workers.sync

import io.airbyte.api.client.WorkloadApiClient
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.workers.workload.JobOutputDocStore
import io.airbyte.workload.api.client.model.generated.Workload
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest
import io.airbyte.workload.api.client.model.generated.WorkloadStatus
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.HttpStatus
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import java.io.IOException
import kotlin.jvm.optionals.getOrElse
import kotlin.time.Duration.Companion.seconds

private val logger = KotlinLogging.logger { }

/**
 * WorkloadClient that abstracts common interactions with the workload-api.
 * This client should be preferred over direct usage of the WorkloadApiClient.
 */
@Singleton
class WorkloadClient(private val workloadApiClient: WorkloadApiClient, private val jobOutputDocStore: JobOutputDocStore) {
  companion object {
    val TERMINAL_STATUSES = setOf(WorkloadStatus.SUCCESS, WorkloadStatus.FAILURE, WorkloadStatus.CANCELLED)
  }

  fun createWorkload(workloadCreateRequest: WorkloadCreateRequest) {
    try {
      workloadApiClient.workloadApi.workloadCreate(workloadCreateRequest)
    } catch (e: ClientException) {
      /*
       * The Workload API returns a 409 response when the request to execute the workload has already been
       * created. That response is handled in the form of a ClientException by the generated OpenAPI
       * client. We do not want to cause the Temporal workflow to retry, so catch it and log the
       * information so that the workflow will continue.
       */
      if (e.statusCode == HttpStatus.CONFLICT.code) {
        logger.warn { "Workload ${workloadCreateRequest.workloadId} already created and in progress.  Continuing..." }
      } else {
        throw RuntimeException(e)
      }
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
  }

  fun waitForWorkload(
    workloadId: String,
    pollingFrequencyInSeconds: Int,
  ) {
    try {
      var workload = workloadApiClient.workloadApi.workloadGet(workloadId)
      while (!isWorkloadTerminal(workload)) {
        Thread.sleep(pollingFrequencyInSeconds.seconds.inWholeMilliseconds)
        workload = workloadApiClient.workloadApi.workloadGet(workloadId)
      }
    } catch (e: IOException) {
      throw RuntimeException(e)
    } catch (e: InterruptedException) {
      throw RuntimeException(e)
    }
  }

  fun getConnectorJobOutput(
    workloadId: String,
    onFailure: (FailureReason) -> ConnectorJobOutput,
  ): ConnectorJobOutput {
    return Result.runCatching {
      jobOutputDocStore.read(workloadId).orElseThrow()
    }.fold(
      onFailure = { t -> onFailure(handleMissingConnectorJobOutput(workloadId, t)) },
      onSuccess = { x -> x },
    )
  }

  private fun handleMissingConnectorJobOutput(
    workloadId: String,
    t: Throwable?,
  ): FailureReason {
    return Result.runCatching {
      val workload = workloadApiClient.workloadApi.workloadGet(workloadId)

      return when (workload.status) {
        // This is pretty bad, the workload succeeded, but we failed to read the output
        WorkloadStatus.SUCCESS ->
          FailureReason()
            .withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
            .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
            .withExternalMessage("Failed to read the output")
            .withInternalMessage("Failed to read the output of a successful workload $workloadId")
            .withStacktrace(t?.stackTraceToString())

        // do some classification from workload.terminationSource
        WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE ->
          FailureReason()
            .withFailureOrigin(
              when (workload.terminationSource) {
                "source" -> FailureReason.FailureOrigin.SOURCE
                "destination" -> FailureReason.FailureOrigin.DESTINATION
                else -> FailureReason.FailureOrigin.AIRBYTE_PLATFORM
              },
            )
            .withExternalMessage("Workload terminated by ${workload.terminationSource}")
            .withInternalMessage(workload.terminationReason)

        // We should never be in this situation, workload is still running not having an output is expected,
        // we should not be trying to read the output of a non-terminal workload.
        else ->
          FailureReason()
            .withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
            .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
            .withExternalMessage("Expected error in the platform")
            .withInternalMessage("$workloadId isn't in a terminal state, no output available")
      }
    }.getOrElse {
      FailureReason()
        .withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
        .withFailureType(FailureReason.FailureType.TRANSIENT_ERROR)
        .withExternalMessage("Platform failure")
        .withInternalMessage("Unable to reach the workload-api")
        .withStacktrace(it.stackTraceToString())
    }
  }

  private fun isWorkloadTerminal(workload: Workload): Boolean = workload.status in TERMINAL_STATUSES
}
