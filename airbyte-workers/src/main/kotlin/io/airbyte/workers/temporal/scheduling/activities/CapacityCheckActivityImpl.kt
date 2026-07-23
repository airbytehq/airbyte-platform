/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.CheckDataWorkerCapacityRequest
import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.workers.temporal.scheduling.activities.CapacityCheckActivity.CapacityCheckInput
import io.airbyte.workers.temporal.scheduling.activities.CapacityCheckActivity.CapacityCheckOutput
import io.micronaut.http.HttpStatus
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import java.io.IOException

/**
 * API-backed implementation of CapacityCheckActivity.
 */
@Singleton
class CapacityCheckActivityImpl(
  private val airbyteApiClient: AirbyteApiClient,
) : CapacityCheckActivity {
  override fun checkCapacity(input: CapacityCheckInput): CapacityCheckOutput {
    val jobId = input.jobId
    val connectionId = input.connectionId
    val organizationId = input.organizationId

    if (!input.enforcementEnabled || jobId == null || connectionId == null || organizationId == null) {
      return CapacityCheckOutput(
        capacityAvailable = true,
        useOnDemandCapacity = false,
        enforcementEnabled = input.enforcementEnabled,
      )
    }

    try {
      val response =
        airbyteApiClient.jobsApi.checkDataWorkerCapacity(
          CheckDataWorkerCapacityRequest(
            jobId,
            connectionId,
            organizationId,
          ),
        )

      return CapacityCheckOutput(
        capacityAvailable = response.capacityAvailable,
        useOnDemandCapacity = response.useOnDemandCapacity,
        enforcementEnabled = true,
      )
    } catch (e: ClientException) {
      if (e.statusCode == HttpStatus.NOT_FOUND.getCode()) {
        throw e
      }
      throw RetryableException(e)
    } catch (e: IOException) {
      throw RetryableException(e)
    }
  }
}
