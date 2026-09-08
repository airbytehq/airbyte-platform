/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.airbyte.commons.entitlements.models.Entitlements
import io.airbyte.commons.entitlements.models.PlanNameEntitlement
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.license.ActiveAirbyteLicense
import io.airbyte.config.Configs
import io.airbyte.data.services.OrganizationService
import io.airbyte.domain.models.EntitlementPlan
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
  val entitlements: Map<String, Any> = emptyMap(),
)

/** Statically granted entitlements parsed from the entitlements YAML file. */
private data class StaticEntitlements(
  val grantedFeatureIds: Set<String> = emptySet(),
  val numericEntitlementValues: Map<String, Long> = emptyMap(),
  val plan: EntitlementPlan? = null,
)

/**
 * Loads statically granted entitlements from a YAML file of the form:
 *
 * ```yaml
 * entitlements:
 *   <feature-id>: true
 *   <numeric-feature-id>: 3
 *   feature-plan-name: plus
 * ```
 *
 * A `true` entry grants the entitlement; for numeric entitlements the grant is unlimited.
 * An integer entry grants a numeric entitlement with that finite value. A string entry for
 * [PlanNameEntitlement] sets the organization's plan (matched case-insensitively against an
 * [EntitlementPlan]'s id, enum name, or display name, e.g. `plan-airbyte-plus`, `plus`, or `Plus`)
 * and counts as a grant of that entitlement. `false` (and unknown feature ids, negative values,
 * and string values for other entitlements) are skipped, so they behave like omitted entries.
 *
 * A blank path returns empty grants silently (deny everything, the default behavior).
 * A missing, unreadable, empty, or unparseable file logs a warning and returns empty grants.
 */
private fun loadStaticEntitlements(path: String): StaticEntitlements {
  if (path.isBlank()) {
    return StaticEntitlements()
  }
  val file = File(path)
  if (!file.isFile || file.length() == 0L) {
    logger.warn { "Static entitlements file '$path' is missing or empty. No entitlements will be granted." }
    return StaticEntitlements()
  }
  val parsed =
    try {
      yamlMapper.readValue(file, StaticEntitlementsFile::class.java)
    } catch (e: IOException) {
      logger.warn(e) { "Failed to parse static entitlements file '$path'. No entitlements will be granted." }
      return StaticEntitlements()
    }

  val knownFeatureIds = Entitlements.all.map { it.featureId }.toSet()
  val grantedFeatureIds = mutableSetOf<String>()
  val numericEntitlementValues = mutableMapOf<String, Long>()
  var plan: EntitlementPlan? = null
  for ((featureId, value) in parsed.entitlements) {
    if (featureId !in knownFeatureIds) {
      logger.warn { "Static entitlements file '$path' contains unknown entitlement id '$featureId'. Skipping." }
      continue
    }
    when (value) {
      is Boolean -> if (value) grantedFeatureIds.add(featureId)
      is Number -> {
        val longValue = value.toLong()
        if (longValue >= 0) {
          numericEntitlementValues[featureId] = longValue
        } else {
          logger.warn { "Static entitlements file '$path' contains negative value '$longValue' for entitlement '$featureId'. Skipping." }
        }
      }
      is String -> {
        if (featureId == PlanNameEntitlement.featureId) {
          val resolved = resolvePlan(value)
          if (resolved == null) {
            logger.warn { "Static entitlements file '$path' contains unknown plan '$value'. Ignoring." }
          } else {
            grantedFeatureIds.add(featureId)
            plan = resolved
          }
        } else {
          logger.warn { "Static entitlements file '$path' contains unsupported string value '$value' for entitlement '$featureId'. Skipping." }
        }
      }
      else -> logger.warn { "Static entitlements file '$path' contains unsupported value '$value' for entitlement '$featureId'. Skipping." }
    }
  }

  return StaticEntitlements(
    grantedFeatureIds = grantedFeatureIds,
    numericEntitlementValues = numericEntitlementValues,
    plan = plan,
  )
}

private fun resolvePlan(planName: String): EntitlementPlan? =
  EntitlementPlan.entries.firstOrNull {
    it.id.equals(planName, ignoreCase = true) ||
      it.name.equals(planName, ignoreCase = true) ||
      it.displayName.equals(planName, ignoreCase = true)
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
      val staticEntitlements = loadStaticEntitlements(airbyteStiggClientConfig.entitlementsFile)
      logger.info {
        "Stigg cloud client is not enabled. Falling back to StaticEntitlementClient with " +
          "${staticEntitlements.grantedFeatureIds.size} statically granted entitlement id(s) and " +
          "${staticEntitlements.numericEntitlementValues.size} numeric entitlement value(s) " +
          "(entitlements file: ${airbyteStiggClientConfig.entitlementsFile.ifBlank { "unset" }})"
      }
      return StaticEntitlementClient(
        grantedFeatureIds = staticEntitlements.grantedFeatureIds,
        numericEntitlementValues = staticEntitlements.numericEntitlementValues,
        plan = staticEntitlements.plan,
      )
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
