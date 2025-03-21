/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.authn

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.DataplaneHeartbeatRequestBody
import io.airbyte.api.client.model.generated.DataplaneInitRequestBody
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.PlaneName
import io.airbyte.featureflag.UseWorkloadTemporalConsumer
import io.airbyte.featureflag.WorkloadLauncherUseDataPlaneAuthNFlow
import io.airbyte.workload.launcher.config.DataplaneCredentials
import io.airbyte.workload.launcher.model.DataplaneConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Property
import io.micronaut.context.event.ApplicationEventPublisher
import io.micronaut.http.HttpStatus
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException

private val logger = KotlinLogging.logger {}

/**
 * Resolves dataplane name, id and other identity related data.
 *
 * Polls control plane for updates at a cadence and publishes results for downstream consumer services.
 *
 * Designed to be able to transition between dynamic (new auth n flow)
 * and static micronaut property / env var driven configuration.
 */
@Singleton
class DataplaneIdentityService(
  @Property(name = "airbyte.data-plane-id") private val dataplaneIdProperty: String,
  @Property(name = "airbyte.data-plane-name") private val dataplaneNameProperty: String,
  private val dataplaneCredentials: DataplaneCredentials,
  private val airbyteApiClient: AirbyteApiClient,
  private val featureFlagClient: FeatureFlagClient,
  private val eventPublisher: ApplicationEventPublisher<DataplaneConfig>,
) {
  var authNDrivenDataplaneConfig: DataplaneConfig? = null
    private set

  fun initialize() {
    if (useDataplaneAuthNFlow()) {
      try {
        val initResponse =
          airbyteApiClient.dataplaneApi.initializeDataplane(
            DataplaneInitRequestBody(clientId = dataplaneCredentials.clientId),
          )
        val config =
          DataplaneConfig(
            dataplaneId = initResponse.dataplaneId,
            dataplaneName = initResponse.dataplaneName,
            dataplaneEnabled = initResponse.dataplaneEnabled,
            dataplaneGroupId = initResponse.dataplaneGroupId,
            dataplaneGroupName = initResponse.dataplaneGroupName,
            temporalConsumerEnabled = featureFlagClient.boolVariation(UseWorkloadTemporalConsumer, PlaneName(dataplaneNameProperty)),
          )

        logger.info { "Running as ${config.dataplaneId} (${config.dataplaneName}) for ${config.dataplaneGroupName}" }
        publishConfigChange(config)
      } catch (e: Exception) {
        throw RuntimeException("Failed to initialize data-plane", e)
      }
    }
  }

  fun getDataplaneId(): String =
    if (useDataplaneAuthNFlow()) {
      authNDrivenDataplaneConfig!!.dataplaneId.toString()
    } else {
      dataplaneIdProperty
    }

  fun getDataplaneName(): String =
    if (useDataplaneAuthNFlow()) {
      authNDrivenDataplaneConfig!!.dataplaneName
    } else {
      dataplaneNameProperty
    }

  @Scheduled(fixedRate = "\${airbyte.workload-launcher.heartbeat-rate}")
  fun heartbeat() {
    if (useDataplaneAuthNFlow()) {
      try {
        val heartbeatResponse =
          airbyteApiClient.dataplaneApi.heartbeatDataplane(
            DataplaneHeartbeatRequestBody(clientId = dataplaneCredentials.clientId),
          )
        publishConfigChange(
          DataplaneConfig(
            dataplaneId = heartbeatResponse.dataplaneId,
            dataplaneName = heartbeatResponse.dataplaneName,
            dataplaneEnabled = heartbeatResponse.dataplaneEnabled,
            dataplaneGroupId = heartbeatResponse.dataplaneGroupId,
            dataplaneGroupName = heartbeatResponse.dataplaneGroupName,
            temporalConsumerEnabled = featureFlagClient.boolVariation(UseWorkloadTemporalConsumer, PlaneName(dataplaneNameProperty)),
          ),
        )
      } catch (e: ClientException) {
        if (e.statusCode == HttpStatus.UNAUTHORIZED.code || e.statusCode == HttpStatus.FORBIDDEN.code) {
          logger.warn { "Failed to authenticate, stopping." }
          authNDrivenDataplaneConfig?.let {
            publishConfigChange(it.copy(dataplaneEnabled = false))
          }
        } else {
          logger.warn(e) { "Failed to heartbeat" }
        }
      } catch (e: Exception) {
        logger.warn(e) { "Failed to heartbeat" }
      }
    }
  }

  private fun publishConfigChange(config: DataplaneConfig) {
    if (config != authNDrivenDataplaneConfig) {
      authNDrivenDataplaneConfig = config
      eventPublisher.publishEvent(config)
    }
  }

  private fun useDataplaneAuthNFlow(): Boolean =
    featureFlagClient.boolVariation(
      WorkloadLauncherUseDataPlaneAuthNFlow,
      PlaneName(dataplaneNameProperty),
    )
}
