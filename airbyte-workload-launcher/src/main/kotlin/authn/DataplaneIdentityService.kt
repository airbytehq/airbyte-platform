/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.authn

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.DataplaneHeartbeatRequestBody
import io.airbyte.api.client.model.generated.DataplaneInitRequestBody
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
  @Property(name = "airbyte.internal-api.auth.client-id") private val dataplaneClientId: String,
  private val airbyteApiClient: AirbyteApiClient,
  private val eventPublisher: ApplicationEventPublisher<DataplaneConfig>,
) {
  var authNDrivenDataplaneConfig: DataplaneConfig? = null
    private set

  fun initialize() {
    try {
      val initResponse =
        airbyteApiClient.dataplaneApi.initializeDataplane(
          DataplaneInitRequestBody(clientId = dataplaneClientId),
        )
      val config =
        DataplaneConfig(
          dataplaneId = initResponse.dataplaneId,
          dataplaneName = initResponse.dataplaneName,
          dataplaneEnabled = initResponse.dataplaneEnabled,
          dataplaneGroupId = initResponse.dataplaneGroupId,
          dataplaneGroupName = initResponse.dataplaneGroupName,
        )

      logger.info { "Running as ${config.dataplaneId} (${config.dataplaneName}) for ${config.dataplaneGroupName}" }
      publishConfigChange(config)
    } catch (e: Exception) {
      throw RuntimeException("Failed to initialize data-plane", e)
    }
  }

  fun getDataplaneId(): String = authNDrivenDataplaneConfig!!.dataplaneId.toString()

  fun getDataplaneName(): String = authNDrivenDataplaneConfig!!.dataplaneName

  @Scheduled(fixedRate = "\${airbyte.workload-launcher.heartbeat-rate}")
  fun heartbeat() {
    try {
      val heartbeatResponse =
        airbyteApiClient.dataplaneApi.heartbeatDataplane(
          DataplaneHeartbeatRequestBody(clientId = dataplaneClientId),
        )
      publishConfigChange(
        DataplaneConfig(
          dataplaneId = heartbeatResponse.dataplaneId,
          dataplaneName = heartbeatResponse.dataplaneName,
          dataplaneEnabled = heartbeatResponse.dataplaneEnabled,
          dataplaneGroupId = heartbeatResponse.dataplaneGroupId,
          dataplaneGroupName = heartbeatResponse.dataplaneGroupName,
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

  private fun publishConfigChange(config: DataplaneConfig) {
    if (config != authNDrivenDataplaneConfig) {
      authNDrivenDataplaneConfig = config
      eventPublisher.publishEvent(config)
    }
  }
}
