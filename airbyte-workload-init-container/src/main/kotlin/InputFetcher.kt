/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.initContainer

import io.airbyte.config.FailureReason.FailureOrigin
import io.airbyte.config.WorkloadType
import io.airbyte.initContainer.input.InputHydrationProcessor
import io.airbyte.initContainer.serde.ObjectSerializer
import io.airbyte.initContainer.system.FileClient
import io.airbyte.initContainer.system.SystemClient
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.micronaut.runtime.AirbyteContextConfig
import io.airbyte.workers.models.InitContainerConstants
import io.airbyte.workers.pod.FileConstants
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.LogDeliveryMode
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadFailureRequest
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Context
import secrets.persistence.SecretCoordinateException

private val logger = KotlinLogging.logger {}

@Context
class InputFetcher(
  private val workloadApiClient: WorkloadApiClient,
  private val hydrationProcessor: InputHydrationProcessor,
  private val systemClient: SystemClient,
  private val metricClient: MetricClient,
  private val airbyteContextConfig: AirbyteContextConfig,
  private val serializer: ObjectSerializer,
  private val fileClient: FileClient,
) {
  fun fetch() {
    logger.info { "Fetching workload..." }

    val workload =
      try {
        workloadApiClient.workloadGet(airbyteContextConfig.workloadId)
      } catch (e: Exception) {
        metricClient.count(metric = OssMetricsRegistry.WORKLOAD_HYDRATION_FETCH_FAILURE)
        return failWorkloadAndExit(airbyteContextConfig.workloadId, "fetching workload", e, InitContainerConstants.WORKLOAD_API_ERROR_EXIT_CODE)
      }

    logger.info { "Workload ${workload.id} fetched." }
    prepareSyncLogDelivery(workload)
    logger.info { "Processing workload..." }

    try {
      hydrationProcessor.process(workload)
    } catch (e: SecretCoordinateException) {
      return failWorkloadAndExit(airbyteContextConfig.workloadId, "hydrating secrets", e, InitContainerConstants.SECRET_HYDRATION_ERROR_EXIT_CODE)
    } catch (e: Exception) {
      return failWorkloadAndExit(airbyteContextConfig.workloadId, "processing workload", e, InitContainerConstants.UNEXPECTED_ERROR_EXIT_CODE)
    }
    logger.info { "Workload processed." }
  }

  private fun prepareSyncLogDelivery(workload: Workload) {
    if (workload.type != WorkloadType.SYNC) {
      return
    }

    val serializedMode =
      try {
        serializer.serialize(workload.logDeliveryMode)
      } catch (_: Exception) {
        reportLogDeliveryHandoffFailure("mode-serialization")
        return
      }

    try {
      fileClient.writeInputFile(FileConstants.LOG_DELIVERY_MODE_FILE, serializedMode)
    } catch (_: Exception) {
      reportLogDeliveryHandoffFailure("mode-file-write")
      return
    }

    if (workload.logDeliveryMode != LogDeliveryMode.FLEX) {
      return
    }

    val authorization =
      try {
        workloadApiClient.workloadLogUploadAuthorization(workload.id)
      } catch (_: Exception) {
        reportLogDeliveryHandoffFailure("authorization-fetch")
        return
      } ?: return

    val serializedAuthorization =
      try {
        serializer.serialize(authorization)
      } catch (_: Exception) {
        reportLogDeliveryHandoffFailure("authorization-serialization")
        return
      }

    try {
      fileClient.writeInputFileAtomically(FileConstants.LOG_UPLOAD_AUTHORIZATION_FILE, serializedAuthorization)
    } catch (_: Exception) {
      reportLogDeliveryHandoffFailure("authorization-file-write")
    }
  }

  private fun reportLogDeliveryHandoffFailure(step: String) {
    logger.warn { "Unable to complete the SYNC log delivery handoff during $step. Continuing workload hydration." }
    metricClient.count(
      metric = OssMetricsRegistry.WORKLOAD_LOG_DELIVERY_HANDOFF_FAILURE,
      attributes = arrayOf(MetricAttribute("step", step)),
    )
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
      workloadApiClient
        .workloadFailure(
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
