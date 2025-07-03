/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.filewriter

import java.io.IOException

/**
 * Handle temporary files containing information required for sending requests.
 */
interface AirbyteFileWriter {
  /**
   * Writes `contents` to a temporary file prepended with `name`.
   */
  @Throws(IOException::class)
  fun write(
    name: String,
    contents: String,
  ): String

  /**
   * Deletes files at `filepath`.
   */
  fun delete(filepath: String)
}
