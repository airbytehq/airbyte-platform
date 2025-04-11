/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.initContainer

import io.airbyte.config.FailureReason.FailureOrigin
import io.airbyte.initContainer.input.InputHydrationProcessor
import io.airbyte.initContainer.system.SystemClient
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.workers.models.InitContainerConstants
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.client.model.generated.WorkloadFailureRequest
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Context
import io.micronaut.context.annotation.Value
import secrets.persistence.SecretCoordinateException

private val logger = KotlinLogging.logger {}

@Context
class InputFetcher(
  private val workloadApiClient: WorkloadApiClient,
  private val hydrationProcessor: InputHydrationProcessor,
  private val systemClient: SystemClient,
  private val metricClient: MetricClient,
  @Value("\${airbyte.workload-id}") private val workloadId: String,
) {
  fun fetch() {
    logger.info { "Fetching workload..." }

    val workload =
      try {
        workloadApiClient.workloadApi.workloadGet(workloadId)
      } catch (e: Exception) {
        metricClient.count(metric = OssMetricsRegistry.WORKLOAD_HYDRATION_FETCH_FAILURE)
        return failWorkloadAndExit(workloadId, "fetching workload", e, InitContainerConstants.WORKLOAD_API_ERROR_EXIT_CODE)
      }

    logger.info { "Workload ${workload.id} fetched." }

    logger.info { "Processing workload..." }
    try {
      hydrationProcessor.process(workload)
    } catch (e: SecretCoordinateException) {
      return failWorkloadAndExit(workloadId, "hydrating secrets", e, InitContainerConstants.SECRET_HYDRATION_ERROR_EXIT_CODE)
    } catch (e: Exception) {
      return failWorkloadAndExit(workloadId, "processing workload", e, InitContainerConstants.UNEXPECTED_ERROR_EXIT_CODE)
    }
    logger.info { "Workload processed." }
  }

  private fun failWorkloadAndExit(
    id: String,
    stepPhrase: String,
    e: Exception,
    exitCodeOnError: Int,
  ) {
    val msg = "Init container error encountered while $stepPhrase for id: $id."

    logger.error(e) { "$msg Attempting to fail workload..." }

    var actualExitCodeToUse = exitCodeOnError
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
      actualExitCodeToUse = InitContainerConstants.WORKLOAD_API_ERROR_EXIT_CODE
    } finally {
      systemClient.exitProcess(actualExitCodeToUse)
    }
  }
}
