/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.config

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.nio.file.Path

class CheckConnectorConfigTest {
  @Test
  fun `the name, launcher and JVM options env var come from the image`() {
    val config = CheckConnectorConfig(image = "airbyte/source-postgres:3.8.10")

    assertEquals("source-postgres", config.name)
    assertEquals(Path.of("/connectors/source-postgres/bin/source-postgres"), config.launcher)
    assertEquals("SOURCE_POSTGRES_OPTS", config.jvmOptsEnvVar)
  }

  @Test
  fun `another connector gets its own launcher and env var`() {
    val config = CheckConnectorConfig(image = "airbyte/source-mysql:3.50.0")

    assertEquals(Path.of("/connectors/source-mysql/bin/source-mysql"), config.launcher)
    assertEquals("SOURCE_MYSQL_OPTS", config.jvmOptsEnvVar)
  }

  @Test
  fun `a registry port is not taken for the name`() {
    val config = CheckConnectorConfig(image = "registry.example.com:5000/airbyte/source-postgres:3.8.10")

    assertEquals("source-postgres", config.name)
    assertEquals(Path.of("/connectors/source-postgres/bin/source-postgres"), config.launcher)
  }

  @Test
  fun `an explicit install dir is used as is`() {
    val config = CheckConnectorConfig(image = "airbyte/source-postgres:3.8.10", installDir = "/opt/pg")

    assertEquals(Path.of("/opt/pg/bin/source-postgres"), config.launcher)
  }
}
