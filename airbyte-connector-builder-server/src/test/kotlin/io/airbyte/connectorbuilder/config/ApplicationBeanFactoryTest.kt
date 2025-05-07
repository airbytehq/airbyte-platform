/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.config

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class ApplicationBeanFactoryTest {
  @Test
  fun `python path`() {
    val path = this::class.java.getResource("/python-path")?.path ?: throw Exception("connectors directory not found")
    val pythonPath = pythonPath(path)
    assertEquals("/connectors/dst:/connectors/src", pythonPath)
  }

  @Test
  fun `python path with no sub-directories`() {
    val path = this::class.java.getResource("/python-path/src")?.path ?: throw Exception("connectors directory not found")
    val pythonPath = pythonPath(path)
    assertEquals("", pythonPath)
  }
}
