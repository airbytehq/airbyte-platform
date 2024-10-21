package io.airbyte.initContainer

import io.airbyte.config.FailureReason.FailureOrigin
import io.airbyte.initContainer.input.InputHydrationProcessor
import io.airbyte.initContainer.system.SystemClient
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.client.model.generated.WorkloadFailureRequest
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import org.apache.commons.lang3.time.StopWatch

private val logger = KotlinLogging.logger {}

@Singleton
class InputFetcher(
  private val workloadApiClient: WorkloadApiClient,
  private val hydrationProcessor: InputHydrationProcessor,
  private val systemClient: SystemClient,
) {
  fun fetch(
    workloadId: String,
    stopWatch: StopWatch,
  ) {
    val workload =
      try {
        workloadApiClient.workloadApi.workloadGet(workloadId)
      } catch (e: Exception) {
        return failWorkloadAndExit(workloadId, "fetching workload", e)
      }
    logger.info { "Workload fetched from the DB at: ${stopWatch.time}" }
    try {
      hydrationProcessor.process(workload)
    } catch (e: Exception) {
      return failWorkloadAndExit(workloadId, "processing workload", e)
    }
    logger.info { "Workload hydrated at: ${stopWatch.time}" }
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
