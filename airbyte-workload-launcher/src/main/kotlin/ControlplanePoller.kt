/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.DataplaneInitRequestBody
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.PlaneName
import io.airbyte.featureflag.WorkloadLauncherUseDataPlaneAuthNFlow
import io.airbyte.workload.launcher.config.DataplaneCredentials
import io.airbyte.workload.launcher.model.DataplaneConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Property
import io.micronaut.context.event.ApplicationEventPublisher
import jakarta.annotation.PostConstruct
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
class ControlplanePoller(
  @Property(name = "airbyte.data-plane-name") private val dataplaneName: String,
  private val dataplaneCredentials: DataplaneCredentials,
  private val airbyteApiClient: AirbyteApiClient,
  private val featureFlagClient: FeatureFlagClient,
  private val eventPublisher: ApplicationEventPublisher<DataplaneConfig>,
) {
  var dataplaneConfig: DataplaneConfig? = null
    private set

  @PostConstruct
  fun initialize() {
    if (useDataplaneAuthNFlow()) {
      try {
        val initResponse =
          airbyteApiClient.dataplaneApi.initializeDataplane(
            DataplaneInitRequestBody(clientId = dataplaneCredentials.clientId),
          )
        dataplaneConfig =
          DataplaneConfig(
            dataplaneId = initResponse.dataplaneId,
            dataplaneName = initResponse.dataplaneName,
            dataplaneEnabled = initResponse.dataplaneEnabled,
            dataplaneGroupId = initResponse.dataplaneGroupId,
            dataplaneGroupName = initResponse.dataplaneGroupName,
          )

        dataplaneConfig?.let {
          logger.info { "Running as ${it.dataplaneId} (${it.dataplaneName}) for ${it.dataplaneGroupName}" }
          eventPublisher.publishEvent(it)
        }
      } catch (e: Exception) {
        throw RuntimeException("Failed to initialize data-plane", e)
      }
    }
  }

  private fun useDataplaneAuthNFlow(): Boolean = featureFlagClient.boolVariation(WorkloadLauncherUseDataPlaneAuthNFlow, PlaneName(dataplaneName))
}
