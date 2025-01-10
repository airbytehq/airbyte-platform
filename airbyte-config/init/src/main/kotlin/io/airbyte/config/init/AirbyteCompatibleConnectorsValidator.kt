/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init

import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.config.CompatibilityRule
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import org.semver4j.Semver

private val logger = KotlinLogging.logger {}

interface AirbyteCompatibleConnectorsValidator {
  fun validate(
    connectorId: String,
    connectorVersion: String,
  ): ConnectorPlatformCompatibilityValidationResult

  fun validateDeclarativeManifest(connectorVersion: String): ConnectorPlatformCompatibilityValidationResult {
    return validate(DECLARATIVE_MANIFEST_DEFINITION_ID, connectorVersion)
  }

  companion object {
    const val DECLARATIVE_MANIFEST_DEFINITION_ID = "8dbab097-db1e-4555-87e8-52f5f94bfad4"
  }
}

@Singleton
@Requires(property = "airbyte.deployment-mode", value = "CLOUD")
class AlwaysValidAirbyteCompatibleConnectorsValidator : AirbyteCompatibleConnectorsValidator {
  init {
    logger.info { "Airbyte connector <> platform compatibility validation disabled.  All connector versions will be considered valid." }
  }

  override fun validate(
    connectorId: String,
    connectorVersion: String,
  ): ConnectorPlatformCompatibilityValidationResult {
    return ConnectorPlatformCompatibilityValidationResult(isValid = true, message = null)
  }
}

@Singleton
@Requires(property = "airbyte.deployment-mode", notEquals = "CLOUD")
class RealAirbyteCompatibleConnectorsValidator(
  private val airbyteCompatibleConnectorVersionsProvider: AirbyteCompatibleConnectorVersionsProvider,
  private val airbyteVersion: AirbyteVersion,
) : AirbyteCompatibleConnectorsValidator {
  override fun validate(
    connectorId: String,
    connectorVersion: String,
  ): ConnectorPlatformCompatibilityValidationResult {
    try {
      val connectorVersionToUpgradeTo = Semver(connectorVersion)
      val currentAirbyteVersion = Semver(airbyteVersion.serialize())
      val compatibleConnectors = airbyteCompatibleConnectorVersionsProvider.getCompatibleConnectorsMatrix()
      if (compatibleConnectors.containsKey(connectorId)) {
        val connectorCompatibilityMatrix = compatibleConnectors[connectorId]
        connectorCompatibilityMatrix?.let {
          for (version in it.compatibilityMatrix) {
            version?.let { v ->
              if (connectorVersionToUpgradeTo.satisfies(v.connectorVersion)) {
                return validateConnectorCompatibility(
                  connectorName = it.connectorName,
                  connectorVersion = connectorVersion,
                  compatibilityRule = v,
                  currentAirbyteVersion = currentAirbyteVersion,
                )
              }
            }
          }
        }
      }
      return ConnectorPlatformCompatibilityValidationResult(isValid = true, message = null)
    } catch (e: Exception) {
      logger.warn(e) { "Exception while trying to validate the connector '$connectorId:$connectorVersion', defaulting to valid." }
      return ConnectorPlatformCompatibilityValidationResult(isValid = true, message = null)
    }
  }

  private fun validateConnectorCompatibility(
    connectorName: String,
    connectorVersion: String,
    compatibilityRule: CompatibilityRule,
    currentAirbyteVersion: Semver,
  ): ConnectorPlatformCompatibilityValidationResult {
    return if (compatibilityRule.blocked) {
      ConnectorPlatformCompatibilityValidationResult(
        isValid = false,
        message =
          "Connector $connectorName with version $connectorVersion is temporarily blocked from upgrade.",
      )
    } else if (currentAirbyteVersion.satisfies(compatibilityRule.airbyteVersion)) {
      ConnectorPlatformCompatibilityValidationResult(isValid = true, message = null)
    } else {
      ConnectorPlatformCompatibilityValidationResult(
        isValid = false,
        message =
          "Current " +
            "Airbyte version $airbyteVersion doesn't support connector version " +
            "$connectorVersion for connector $connectorName." +
            " Compatible Airbyte Version(s): ${compatibilityRule.airbyteVersion}",
      )
    }
  }
}

data class ConnectorPlatformCompatibilityValidationResult(val isValid: Boolean, val message: String?)
