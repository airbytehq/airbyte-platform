package io.airbyte.initContainer

import io.airbyte.config.FailureReason.FailureOrigin
import io.airbyte.initContainer.input.ReplicationHydrationProcessor
import io.airbyte.initContainer.system.SystemClient
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.client.model.generated.WorkloadFailureRequest
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
class InputFetcher(
  private val workloadApiClient: WorkloadApiClient,
  private val replInputProcessor: ReplicationHydrationProcessor,
  private val systemClient: SystemClient,
) {
  fun fetch(workloadId: String) {
    val rawPayload =
      try {
        val workload = workloadApiClient.workloadApi.workloadGet(workloadId)
        workload.inputPayload
      } catch (e: Exception) {
        return failWorkloadAndExit(workloadId, "fetching workload", e)
      }

    try {
      replInputProcessor.process(rawPayload)
    } catch (e: Exception) {
      return failWorkloadAndExit(workloadId, "processing workload", e)
    }
  }

  private fun failWorkloadAndExit(
    id: String,
    stepPhrase: String,
    e: Exception,
  ) {
    val msg = "Init container error encountered while $stepPhrase for id: $id."

    logger.error(e) { "$msg Attempting to fail workload..." }

    try {
      workloadApiClient.workloadApi.workloadFailure(
        WorkloadFailureRequest(
          id,
          FailureOrigin.AIRBYTE_PLATFORM.toString(),
          "$msg Encountered exception of type: ${e.javaClass}. Exception message: ${e.message}.",
        ),
      )
    } catch (e: Exception) {
      logger.error(e) { "Error encountered failing workload for id: $id. Ignoring..." }
    } finally {
      systemClient.exitProcess(1)
    }
  }
}
