/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init

import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.config.AirbyteCompatibleConnectorVersionsMatrix
import io.airbyte.config.CompatibilityRule
import io.airbyte.config.ConnectorInfo
import io.airbyte.config.init.AirbyteCompatibleConnectorVersionsProvider.Companion.convertToMap
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.util.UUID

private const val CONNECTOR_NAME = "source-test"

internal class AirbyteCompatibleConnectorsValidatorTest {
  @ParameterizedTest
  @CsvSource(
    value = [
      "1.2.3;1.0.1;1.2.*;>1.0.0;true", // Rule exists for connector and Airbyte version is compatible
      "1.2.3;1.0.1;1.2.*;>=1.0.0;true", // Rule exists for connector and Airbyte version is compatible
      "1.2.3;1.0.1;1.2.*;>1.2.0;false", // Rule exists for connector but Airbyte version is incompatible
      "1.2.3;1.0.1;>1.2.4;>1.2.0;true", // Rule does not exist for the connector version
      "1.2.3;1.0.1;1.2.*;[1.0.0,1.0.2];true", // Rule exists for the connector and the Airbyte version is compatible (range)
      "1.2.3;1.0.3;1.2.*;[1.0.0,1.0.2];false", // Rule exists for the connector but the Airbyte version is incompatible (range)
      "1.2.3;1.0.3;>1.2.0;<0.0.0;false", // Rule exists for the connector but is blocked for all Airbyte versions
      "1.2.3;1.0.4;1.2.3;<1.0.0 || >1.0.3;true", // Rule exists for the connector and the Airbyte version is compatible (range)
      "1.2.3;0.0.9;1.2.3;<1.0.0 || >1.0.3;true", // Rule exists for the connector and the Airbyte version is compatible (range)
      "1.2.3;1.0.1;1.2.3;<1.0.0 || >1.0.3;false", // Rule exists for the connector but the Airbyte version is incompatible (range)
      "1.2.3;dev;1.2.3;<1.0.0 || >1.0.3;true", // Rule exists for the connector but the Airbyte version is dev
      "dev;1.0.1;1.2.3;<1.0.0 || >1.0.3;true", // Connector is dev but the Airbyte version is dev
    ],
    delimiterString = ";",
  )
  internal fun testCompatiblePlatformVersion(
    connectorVersionToUpgradeTo: String,
    currentAirbyteVersion: String,
    connectorVersionInfo: String,
    airbyteVersionInfo: String,
    expectedResult: Boolean,
  ) {
    val connectorId = UUID.randomUUID()
    val airbyteCompatibleConnectorVersionsProvider: AirbyteCompatibleConnectorVersionsProvider = mockk()
    val airbyteVersion = AirbyteVersion(currentAirbyteVersion)
    val validator =
      RealAirbyteCompatibleConnectorsValidator(
        airbyteCompatibleConnectorVersionsProvider = airbyteCompatibleConnectorVersionsProvider,
        airbyteVersion = airbyteVersion,
      )

    every {
      airbyteCompatibleConnectorVersionsProvider.getCompatibleConnectorsMatrix()
    } returns getCompatibilityMatrix(connectorId, connectorVersionInfo, airbyteVersionInfo).convertToMap()

    val result = validator.validate(connectorId = connectorId.toString(), connectorVersion = connectorVersionToUpgradeTo)

    assertEquals(expectedResult, result.isValid)
    if (!result.isValid) {
      assertEquals(
        "Current Airbyte version $airbyteVersion doesn't support connector version " +
          "$connectorVersionToUpgradeTo for connector ${CONNECTOR_NAME}. Compatible Airbyte Version(s): $airbyteVersionInfo",
        result.message,
      )
    }
  }

  @Test
  internal fun testBlockedConnector() {
    val connectorId = UUID.randomUUID()
    val connectorName = "source-test"
    val connectorVersion = "1.0.0"
    val airbyteCompatibleConnectorVersionsProvider: AirbyteCompatibleConnectorVersionsProvider = mockk()
    val airbyteVersion = AirbyteVersion("1.2.3")
    val compatibilityMatrix =
      AirbyteCompatibleConnectorVersionsMatrix()
        .withCompatibleConnectors(
          listOf(
            ConnectorInfo()
              .withConnectorType("source")
              .withConnectorName(connectorName)
              .withConnectorDefinitionId(connectorId)
              .withCompatibilityMatrix(
                listOf(
                  CompatibilityRule()
                    .withBlocked(true)
                    .withConnectorVersion(connectorVersion)
                    .withAirbyteVersion(airbyteVersion.serialize()),
                ),
              ),
          ),
        )
    val validator =
      RealAirbyteCompatibleConnectorsValidator(
        airbyteCompatibleConnectorVersionsProvider = airbyteCompatibleConnectorVersionsProvider,
        airbyteVersion = airbyteVersion,
      )

    every { airbyteCompatibleConnectorVersionsProvider.getCompatibleConnectorsMatrix() } returns
      compatibilityMatrix.convertToMap()

    val result = validator.validate(connectorId = connectorId.toString(), connectorVersion = connectorVersion)
    assertEquals(false, result.isValid)
    assertEquals("Connector $connectorName with version $connectorVersion is temporarily blocked from upgrade.", result.message)
  }

  @Test
  internal fun testNoRulesForConnector() {
    val connectorId = UUID.randomUUID().toString()
    val connectorVersion = "1.0.0"
    val airbyteCompatibleConnectorVersionsProvider: AirbyteCompatibleConnectorVersionsProvider = mockk()
    val airbyteVersion = AirbyteVersion("1.2.3")
    val validator =
      RealAirbyteCompatibleConnectorsValidator(
        airbyteCompatibleConnectorVersionsProvider = airbyteCompatibleConnectorVersionsProvider,
        airbyteVersion = airbyteVersion,
      )

    every { airbyteCompatibleConnectorVersionsProvider.getCompatibleConnectorsMatrix() } returns
      AirbyteCompatibleConnectorVersionsMatrix().convertToMap()
    val result = validator.validate(connectorId = connectorId, connectorVersion = connectorVersion)
    assertEquals(true, result.isValid)
  }

  @Test
  internal fun testAlwaysValidValidator() {
    val validator = AlwaysValidAirbyteCompatibleConnectorsValidator()
    assertEquals(true, validator.validate(connectorId = CONNECTOR_NAME, connectorVersion = "some version").isValid)
    assertEquals(true, validator.validate(connectorId = CONNECTOR_NAME, connectorVersion = "1.2.3").isValid)
    assertEquals(true, validator.validate(connectorId = CONNECTOR_NAME, connectorVersion = "dev").isValid)
  }

  private fun getCompatibilityMatrix(
    connectorId: UUID,
    connectorVersion: String,
    incompatibleAirbyteVersion: String,
  ): AirbyteCompatibleConnectorVersionsMatrix {
    return AirbyteCompatibleConnectorVersionsMatrix().withCompatibleConnectors(
      listOf(
        ConnectorInfo()
          .withConnectorName(CONNECTOR_NAME)
          .withConnectorType("type")
          .withConnectorDefinitionId(connectorId)
          .withCompatibilityMatrix(
            listOf(
              CompatibilityRule()
                .withConnectorVersion(connectorVersion)
                .withAirbyteVersion(incompatibleAirbyteVersion),
            ),
          ),
      ),
    )
  }
}
