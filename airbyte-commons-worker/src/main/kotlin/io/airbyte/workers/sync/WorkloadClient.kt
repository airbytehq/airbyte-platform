package io.airbyte.workers.sync

import io.airbyte.api.client.WorkloadApiClient
import io.airbyte.workload.api.client.model.generated.Workload
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest
import io.airbyte.workload.api.client.model.generated.WorkloadStatus
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.HttpStatus
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import java.io.IOException
import kotlin.time.Duration.Companion.seconds

private val logger = KotlinLogging.logger { }

/**
 * WorkloadClient that abstracts common interactions with the workload-api.
 * This client should be preferred over direct usage of the WorkloadApiClient.
 */
@Singleton
class WorkloadClient(private val workloadApiClient: WorkloadApiClient) {
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

  private fun isWorkloadTerminal(workload: Workload): Boolean = workload.status in TERMINAL_STATUSES
}
