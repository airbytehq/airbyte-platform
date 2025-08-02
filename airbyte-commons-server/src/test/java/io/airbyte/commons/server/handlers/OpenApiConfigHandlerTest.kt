/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.google.common.io.Files
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.io.IOException
import java.nio.charset.Charset

internal class OpenApiConfigHandlerTest {
  @Test
  @Throws(IOException::class)
  fun testGetFile() {
    val lines: MutableList<String> = Files.readLines(OpenApiConfigHandler().file, Charset.defaultCharset())
    Assertions.assertTrue(lines.get(0).contains("openapi"))
  }
}
