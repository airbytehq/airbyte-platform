/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.airbyte.commons.entitlements.models.Entitlements
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.license.ActiveAirbyteLicense
import io.airbyte.config.Configs
import io.airbyte.data.services.OrganizationService
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.MetricClient
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteStiggClientConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import io.stigg.sidecar.proto.v1.ApiConfig
import io.stigg.sidecar.sdk.Stigg
import io.stigg.sidecar.sdk.StiggConfig
import io.stigg.sidecar.sdk.offline.CustomerEntitlements
import jakarta.inject.Singleton
import java.io.File
import java.io.IOException

private val logger = KotlinLogging.logger {}

private val yamlMapper = ObjectMapper(YAMLFactory()).registerKotlinModule()

private data class StaticEntitlementsFile(
  val entitlements: Map<String, Boolean> = emptyMap(),
)

/**
 * Loads the set of statically granted entitlement feature ids from a YAML file of the form:
 *
 * ```yaml
 * entitlements:
 *   <feature-id>: true
 *   <another-feature-id>: false
 * ```
 *
 * A blank path returns an empty set silently (deny everything, the default behavior).
 * A missing, unreadable, empty, or unparseable file logs a warning and returns an empty set.
 * Unknown feature ids are skipped with a warning to catch typos and stale ids after renames.
 */
private fun loadGrantedFeatureIds(path: String): Set<String> {
  if (path.isBlank()) {
    return emptySet()
  }
  val file = File(path)
  if (!file.isFile || file.length() == 0L) {
    logger.warn { "Static entitlements file '$path' is missing or empty. No entitlements will be granted." }
    return emptySet()
  }
  val parsed =
    try {
      yamlMapper.readValue(file, StaticEntitlementsFile::class.java)
    } catch (e: IOException) {
      logger.warn(e) { "Failed to parse static entitlements file '$path'. No entitlements will be granted." }
      return emptySet()
    }
  val knownFeatureIds = Entitlements.all.map { it.featureId }.toSet()
  return parsed.entitlements
    .filterValues { it }
    .keys
    .filter { featureId ->
      val known = featureId in knownFeatureIds
      if (!known) {
        logger.warn { "Static entitlements file '$path' contains unknown entitlement id '$featureId'. Skipping." }
      }
      known
    }.toSet()
}

object MissingStiggApiKey : Exception("Can't create an entitlements client because the Stigg API key is null or blank")

object MissingStiggSidecarHost : Exception("Can't create an entitlements client because the sidecar host is null or blank")

object MissingStiggSidecarPort : Exception("Can't create an entitlements client because the sidecar port is null or blank")

object MissingOrganizationService : Exception("Can't create an entitlements client because organizationService is null")

@Factory
internal class EntitlementClientFactory(
  private val airbyteConfig: AirbyteConfig,
  private val airbyteStiggClientConfig: AirbyteStiggClientConfig,
  private val activeLicense: ActiveAirbyteLicense? = null,
  private val organizationService: OrganizationService? = null,
  private val metricClient: MetricClient? = null,
  private val featureFlagClient: FeatureFlagClient? = null,
) {
  @Singleton
  fun entitlementClient(): EntitlementClient =
    when (airbyteConfig.edition) {
      Configs.AirbyteEdition.COMMUNITY -> {
        logger.info { "Creating StaticEntitlementClient" }
        StaticEntitlementClient()
      }
      Configs.AirbyteEdition.ENTERPRISE -> createStiggEnterpriseClient()
      Configs.AirbyteEdition.CLOUD -> createStiggCloudClient()
    }

  private fun createStiggCloudClient(): EntitlementClient {
    if (!airbyteStiggClientConfig.enabled) {
      val entitlementsFile = airbyteStiggClientConfig.entitlementsFile
      val grantedFeatureIds = loadGrantedFeatureIds(entitlementsFile)
      logger.info {
        "Stigg cloud client is not enabled. Falling back to StaticEntitlementClient with ${grantedFeatureIds.size} " +
          "statically granted entitlement id(s) (entitlements file: ${entitlementsFile.ifBlank { "unset" }})"
      }
      return StaticEntitlementClient(grantedFeatureIds)
    }
    logger.info { "Creating Stigg Cloud client" }

    if (airbyteStiggClientConfig.apiKey.isBlank()) {
      throw MissingStiggApiKey
    }
    if (airbyteStiggClientConfig.sidecarHost.isBlank()) {
      throw MissingStiggSidecarHost
    }
    if (airbyteStiggClientConfig.sidecarPort <= 0) {
      throw MissingStiggSidecarPort
    }
    if (organizationService == null) {
      throw MissingOrganizationService
    }

    return StiggCloudEntitlementClient(
      StiggWrapper(
        stigg =
          Stigg.init(
            StiggConfig
              .builder()
              .apiConfig(ApiConfig.newBuilder().setApiKey(airbyteStiggClientConfig.apiKey).build())
              .remoteSidecarHost(airbyteStiggClientConfig.sidecarHost)
              .remoteSidecarPort(airbyteStiggClientConfig.sidecarPort)
              .build(),
          ),
        metricClient = metricClient,
        featureFlagClient = featureFlagClient,
      ),
      organizationService,
    )
  }

  private fun createStiggEnterpriseClient(): EntitlementClient {
    logger.info { "Creating Stigg Enterprise client" }

    val license = activeLicense?.license
    if (license == null) {
      logger.info { "License key is not set. Falling back to StaticEntitlementClient" }
      return StaticEntitlementClient()
    }

    val rawEntitlements = license.stiggEntitlements
    if (rawEntitlements.isNullOrEmpty()) {
      logger.info { "Stigg entitlements from license are not set. Falling back to StaticEntitlementClient" }
      return StaticEntitlementClient()
    }

    val entitlements = Jsons.deserialize(rawEntitlements, CustomerEntitlements::class.java)
    logger.debug { "Found entitlements docs: $entitlements" }
    return StiggEnterpriseEntitlementClient(entitlements)
  }
}
