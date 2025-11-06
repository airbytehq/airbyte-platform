/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.io

import io.airbyte.commons.io.IOs.readFile
import io.airbyte.commons.io.IOs.writeFile
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.nio.file.Files

internal class IOsTest {
  @Test
  fun testReadWrite() {
    val path = Files.createTempDirectory("tmp")
    writeFile(path.resolve(FILE), ABC)
    Assertions.assertEquals(ABC, readFile(path.resolve(FILE)))
  }

  companion object {
    private const val ABC = "abc"
    private const val FILE = "file"
  }
}
