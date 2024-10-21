package io.airbyte.initContainer

import io.airbyte.config.FailureReason.FailureOrigin
import io.airbyte.initContainer.input.InputHydrationProcessor
import io.airbyte.initContainer.system.SystemClient
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.client.model.generated.WorkloadFailureRequest
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Context
import io.micronaut.context.annotation.Value
import javax.annotation.PostConstruct

private val logger = KotlinLogging.logger {}

@Context
class InputFetcher(
  private val workloadApiClient: WorkloadApiClient,
  private val hydrationProcessor: InputHydrationProcessor,
  private val systemClient: SystemClient,
  @Value("\${airbyte.workload-id}") private val workloadId: String,
) {
  @PostConstruct
  fun fetch() {
    logger.info { "Fetching workload..." }

    val workload =
      try {
        workloadApiClient.workloadApi.workloadGet(workloadId)
      } catch (e: Exception) {
        return failWorkloadAndExit(workloadId, "fetching workload", e)
      }

    logger.info { "Workload ${workload.id} fetched." }

    logger.info { "Processing workload..." }
    try {
      hydrationProcessor.process(workload)
    } catch (e: Exception) {
      return failWorkloadAndExit(workloadId, "processing workload", e)
    }
    logger.info { "Workload processed." }
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
