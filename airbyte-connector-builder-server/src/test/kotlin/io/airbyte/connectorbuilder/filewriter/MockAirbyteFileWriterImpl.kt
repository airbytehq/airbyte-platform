/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.filewriter

import jakarta.inject.Singleton
import java.io.IOException

/**
 * Mocks writing & deleting files.
 */
@Singleton
class MockAirbyteFileWriterImpl : AirbyteFileWriter {
  var shouldThrow: Boolean = false

  /**
   * Mocks writing files.
   */
  @Throws(IOException::class)
  override fun write(
    name: String,
    contents: String,
  ): String {
    if (this.shouldThrow) {
      throw IOException()
    }
    return ""
  }

  override fun delete(filepath: String) {}
}
