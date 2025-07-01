/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.filewriter

import java.io.IOException

/**
 * Exposes a way of constructing arguments for Airbyte protocol commands.
 */
class AirbyteArgument(
  private val writer: AirbyteFileWriter,
) {
  var filepath: String? = null

  @Throws(IOException::class)
  fun setUpArg(
    name: String,
    contents: String,
  ) {
    val filepath = writer.write(name, contents)
    this.filepath = filepath
  }
}
