/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.charset.Charset

internal class OpenApiConfigHandlerTest {
  @Test
  fun testGetFile() {
    val lines: List<String> = OpenApiConfigHandler().file.readLines(Charset.defaultCharset())
    assertTrue(lines[0].contains("openapi"))
  }
}
