/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init

import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.config.CompatibilityRule
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import org.semver4j.Semver

private val logger = KotlinLogging.logger {}

@Singleton
class AirbyteCompatibleConnectorsValidator(
  private val airbyteCompatibleConnectorVersionsProvider: AirbyteCompatibleConnectorVersionsProvider,
  private val airbyteVersion: AirbyteVersion,
) {
  fun validate(
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
      logger.error(e) { "Exception while trying to validate the connector, defaulting to valid" }
      return ConnectorPlatformCompatibilityValidationResult(isValid = true, message = null)
    }
  }

  fun validateDeclarativeManifest(connectorVersion: String): ConnectorPlatformCompatibilityValidationResult {
    return validate(DECLARATIVE_MANIFEST_DEFINITION_ID, connectorVersion)
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

  companion object {
    const val DECLARATIVE_MANIFEST_DEFINITION_ID = "8dbab097-db1e-4555-87e8-52f5f94bfad4"
  }
}

data class ConnectorPlatformCompatibilityValidationResult(val isValid: Boolean, val message: String?)
