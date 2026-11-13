/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.config

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.nio.file.Path

class CheckConnectorConfigTest {
  @Test
  fun `the name and jar coordinates come from the image`() {
    assertEquals("source-postgres", connectorName("airbyte/source-postgres:3.8.10"))
    assertEquals("io.airbyte.connectors:source-postgres:3.8.10", CheckConnectorConfig().jarCoordinates("airbyte/source-postgres:3.8.10"))
  }

  @Test
  fun `another connector and group id give other coordinates`() {
    val config = CheckConnectorConfig(jarGroupId = "io.example.jars")

    assertEquals("io.example.jars:source-mysql:3.50.0", config.jarCoordinates("airbyte/source-mysql:3.50.0"))
  }

  @Test
  fun `a registry port is not taken for the name or the version`() {
    val image = "registry.example.com:5000/airbyte/source-postgres:3.8.10"

    assertEquals("source-postgres", connectorName(image))
    assertEquals("io.airbyte.connectors:source-postgres:3.8.10", CheckConnectorConfig().jarCoordinates(image))
  }

  @Test
  fun `the connector runs on the runner's own java unless another is set`() {
    assertEquals(Path.of(System.getProperty("java.home"), "bin", "java"), CheckConnectorConfig().java)
    assertEquals(Path.of("/opt/jdk/bin/java"), CheckConnectorConfig(javaExecutable = "/opt/jdk/bin/java").java)
  }
}
