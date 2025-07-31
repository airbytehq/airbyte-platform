/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.resources.Resources
import io.airbyte.config.AirbyteCompatibleConnectorVersionsMatrix
import io.airbyte.config.init.AirbyteCompatibleConnectorVersionsProvider
import io.airbyte.config.init.AirbyteCompatibleConnectorVersionsProvider.Companion.convertToMap
import io.airbyte.config.init.AirbyteCompatibleConnectorsValidator
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest(environments = [Environment.TEST])
@Property(name = "airbyte.version", value = "1.2.0")
@Property(name = "airbyte.workspace.root", value = "./build/tmp/workspace")
@Property(name = "airbyte.edition", value = "COMMUNITY")
@Property(name = "micronaut.http.services.workload-api.url", value = "http://localhost")
@Requires(env = ["internal"])
class AirbyteCompatibilityTest(
  private val airbyteCompatibilityValidator: AirbyteCompatibleConnectorsValidator,
) {
  @Test
  internal fun testPlatformCompatibility() {
    val connectorId = "8e6cf9b5-07da-4cae-b943-144c5bd73840"
    // Connector version is not compatible with current platform version
    assertEquals(false, airbyteCompatibilityValidator.validate(connectorId = connectorId, connectorVersion = "1.0.0").isValid)
    // Connector version is compatible with current platform version
    assertEquals(
      true,
      airbyteCompatibilityValidator.validate(connectorId = connectorId, connectorVersion = "1.1.0").isValid,
    )
    // Connector version is blocked from upgrade
    assertEquals(
      false,
      airbyteCompatibilityValidator.validate(connectorId = connectorId, connectorVersion = "1.1.3").isValid,
    )
    // Connector version is not compatible with current platform version due to <> range
    assertEquals(
      false,
      airbyteCompatibilityValidator.validate(connectorId = connectorId, connectorVersion = "1.1.5").isValid,
    )
    // Connector version is compatible with current platform version
    assertEquals(
      true,
      airbyteCompatibilityValidator.validate(connectorId = connectorId, connectorVersion = "1.2.0").isValid,
    )
    // Connector version is compatible with current platform version
    assertEquals(
      true,
      airbyteCompatibilityValidator.validate(connectorId = connectorId, connectorVersion = "1.2.1").isValid,
    )
    // Connector version is not present in matrix, so compatible by default
    assertEquals(
      true,
      airbyteCompatibilityValidator.validate(connectorId = connectorId, connectorVersion = "0.9.0").isValid,
    )
    // Connector is not present in matrix, so compatible by default
    assertEquals(
      true,
      airbyteCompatibilityValidator.validate(connectorId = UUID.randomUUID().toString(), connectorVersion = "1.0.0").isValid,
    )

    assertEquals("8dbab097-db1e-4555-87e8-52f5f94bfad4", AirbyteCompatibleConnectorsValidator.DECLARATIVE_MANIFEST_DEFINITION_ID)
  }

  /**
   * Mock [AirbyteCompatibleConnectorVersionsProvider] implementation that loads the compatibility matrix from a file on the
   * test classpath.
   */
  @MockBean(AirbyteCompatibleConnectorVersionsProvider::class)
  fun platformCompatibilityProvider(): AirbyteCompatibleConnectorVersionsProvider {
    val matrix =
      Jsons.deserialize(
        Resources.read("platform-compatibility.json"),
        AirbyteCompatibleConnectorVersionsMatrix::class.java,
      )
    val platformCompatibilityProvider: AirbyteCompatibleConnectorVersionsProvider = mockk()
    every { platformCompatibilityProvider.getCompatibleConnectorsMatrix() } returns matrix.convertToMap()
    return platformCompatibilityProvider
  }
}
