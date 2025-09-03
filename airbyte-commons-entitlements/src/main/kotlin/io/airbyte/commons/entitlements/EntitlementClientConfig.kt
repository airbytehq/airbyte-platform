/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.license.ActiveAirbyteLicense
import io.airbyte.config.Configs
import io.airbyte.data.services.OrganizationService
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Factory
import io.stigg.sidecar.proto.v1.ApiConfig
import io.stigg.sidecar.sdk.Stigg
import io.stigg.sidecar.sdk.StiggConfig
import io.stigg.sidecar.sdk.offline.CustomerEntitlements
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@ConfigurationProperties("airbyte.stigg")
data class AirbyteStiggConfig(
  val enabled: Boolean = false,
  val apiKey: String? = null,
  val sidecarHost: String? = null,
  val sidecarPort: Int? = null,
)

object MissingStiggConfig : Exception("Can't create an entitlements client because the Stigg config is null")

object MissingStiggApiKey : Exception("Can't create an entitlements client because the Stigg API key is null or blank")

object MissingStiggSidecarHost : Exception("Can't create an entitlements client because the sidecar host is null or blank")

object MissingStiggSidecarPort : Exception("Can't create an entitlements client because the sidecar port is null or blank")

object MissingOrganizationService : Exception("Can't create an entitlements client because organizationService is null")

@Factory
internal class EntitlementClientFactory(
  private val airbyteEdition: Configs.AirbyteEdition,
  private val stiggConfig: AirbyteStiggConfig = AirbyteStiggConfig(),
  private val activeLicense: ActiveAirbyteLicense? = null,
  private val organizationService: OrganizationService? = null,
) {
  @Singleton
  fun entitlementClient(): EntitlementClient =
    when (airbyteEdition) {
      Configs.AirbyteEdition.COMMUNITY -> {
        logger.info { "Creating NoEntitlementClient" }
        NoEntitlementClient()
      }
      Configs.AirbyteEdition.ENTERPRISE -> createStiggEnterpriseClient()
      Configs.AirbyteEdition.CLOUD -> createStiggCloudClient()
    }

  private fun createStiggCloudClient(): EntitlementClient {
    if (!stiggConfig.enabled) {
      logger.info { "Stigg cloud client is not enabled. Falling back to NoEntitlementClient" }
      return NoEntitlementClient()
    }
    logger.info { "Creating Stigg Cloud client" }

    if (stiggConfig == null) {
      throw MissingStiggConfig
    }
    if (stiggConfig.apiKey.isNullOrBlank()) {
      throw MissingStiggApiKey
    }
    if (stiggConfig.sidecarHost.isNullOrBlank()) {
      throw MissingStiggSidecarHost
    }
    if (stiggConfig.sidecarPort == null || stiggConfig.sidecarPort <= 0) {
      throw MissingStiggSidecarPort
    }
    if (organizationService == null) {
      throw MissingOrganizationService
    }

    return StiggCloudEntitlementClient(
      StiggWrapper(
        Stigg.init(
          StiggConfig
            .builder()
            .apiConfig(ApiConfig.newBuilder().setApiKey(stiggConfig.apiKey).build())
            .remoteSidecarHost(stiggConfig.sidecarHost)
            .remoteSidecarPort(stiggConfig.sidecarPort)
            .build(),
        ),
      ),
      organizationService,
    )
  }

  private fun createStiggEnterpriseClient(): EntitlementClient {
    logger.info { "Creating Stigg Enterprise client" }

    val license = activeLicense?.license
    if (license == null) {
      logger.info { "License key is not set. Falling back to NoEntitlementsClient" }
      return NoEntitlementClient()
    }

    val rawEntitlements = license.stiggEntitlements
    if (rawEntitlements.isNullOrEmpty()) {
      logger.info { "Stigg entitlements from license are not set. Falling back to NoEntitlementsClient" }
      return NoEntitlementClient()
    }

    val entitlements = Jsons.deserialize(rawEntitlements, CustomerEntitlements::class.java)
    logger.debug { "Found entitlements docs: $entitlements" }
    return StiggEnterpriseEntitlementClient(entitlements)
  }
}
