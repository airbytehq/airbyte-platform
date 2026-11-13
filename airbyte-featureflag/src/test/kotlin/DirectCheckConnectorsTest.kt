/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.featureflag

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class DirectCheckConnectorsTest {
  @Test
  fun `connector names are split on commas, trimmed, and empty entries dropped`() {
    assertEquals(setOf("source-postgres", "source-mysql"), parseDirectCheckConnectors(" source-postgres , ,source-mysql, "))
    assertEquals(emptySet<String>(), parseDirectCheckConnectors(""))
  }

  @Test
  fun `any version of Airbyte's image of a listed connector matches`() {
    val connectors = setOf("source-postgres")

    listOf("airbyte/source-postgres:3.8.10", "airbyte/source-postgres:3.8.9", "airbyte/source-postgres:3.9.0-rc1").forEach { image ->
      assertTrue(isListedAirbyteConnector(image, connectors), image)
    }
  }

  @Test
  fun `another connector, another publisher's image, a registry prefix, a digest or a missing tag does not match`() {
    val connectors = setOf("source-postgres")

    listOf(
      "airbyte/source-mysql:3.50.0",
      "acme/source-postgres:3.8.10",
      "registry.example.com:5000/airbyte/source-postgres:3.8.10",
      "airbyte/source-postgres:3.8.10@sha256:0123456789abcdef",
      "airbyte/source-postgres",
      "airbyte/source-postgres:",
      "",
    ).forEach { image ->
      assertFalse(isListedAirbyteConnector(image, connectors), image)
    }
  }

  @Test
  fun `an image-style flag value matches nothing`() {
    assertFalse(isListedAirbyteConnector("airbyte/source-postgres:3.8.10", parseDirectCheckConnectors("airbyte/source-postgres:3.8.10")))
  }
}
