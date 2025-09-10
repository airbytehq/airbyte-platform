/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.license.ActiveAirbyteLicense
import io.airbyte.config.Configs
import io.airbyte.data.services.OrganizationService
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteEntitlementConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import io.stigg.sidecar.proto.v1.ApiConfig
import io.stigg.sidecar.sdk.Stigg
import io.stigg.sidecar.sdk.StiggConfig
import io.stigg.sidecar.sdk.offline.CustomerEntitlements
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

object MissingStiggApiKey : Exception("Can't create an entitlements client because the Stigg API key is null or blank")

object MissingStiggSidecarHost : Exception("Can't create an entitlements client because the sidecar host is null or blank")

object MissingStiggSidecarPort : Exception("Can't create an entitlements client because the sidecar port is null or blank")

object MissingOrganizationService : Exception("Can't create an entitlements client because organizationService is null")

@Factory
internal class EntitlementClientFactory(
  private val airbyteConfig: AirbyteConfig,
  private val airbyteEntitlementConfig: AirbyteEntitlementConfig,
  private val activeLicense: ActiveAirbyteLicense? = null,
  private val organizationService: OrganizationService? = null,
) {
  @Singleton
  fun entitlementClient(): EntitlementClient =
    when (airbyteConfig.edition) {
      Configs.AirbyteEdition.COMMUNITY -> {
        logger.info { "Creating NoEntitlementClient" }
        NoEntitlementClient()
      }
      Configs.AirbyteEdition.ENTERPRISE -> createStiggEnterpriseClient()
      Configs.AirbyteEdition.CLOUD -> createStiggCloudClient()
    }

  private fun createStiggCloudClient(): EntitlementClient {
    if (!airbyteEntitlementConfig.stigg.enabled) {
      logger.info { "Stigg cloud client is not enabled. Falling back to NoEntitlementClient" }
      return NoEntitlementClient()
    }
    logger.info { "Creating Stigg Cloud client" }

    if (airbyteEntitlementConfig.stigg.apiKey.isBlank()) {
      throw MissingStiggApiKey
    }
    if (airbyteEntitlementConfig.stigg.sidecarHost.isBlank()) {
      throw MissingStiggSidecarHost
    }
    if (airbyteEntitlementConfig.stigg.sidecarPort <= 0) {
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
            .apiConfig(ApiConfig.newBuilder().setApiKey(airbyteEntitlementConfig.stigg.apiKey).build())
            .remoteSidecarHost(airbyteEntitlementConfig.stigg.sidecarHost)
            .remoteSidecarPort(airbyteEntitlementConfig.stigg.sidecarPort)
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
