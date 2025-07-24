/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.io

import java.io.BufferedReader
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

/**
 * Common code for handling IO.
 */
object IOs {
  /**
   * Write a string to a file.
   *
   * @param filePath path with file name to write to
   * @param contents string to write to file
   */
  @JvmStatic
  @Deprecated("If calling from kotlin, use kotlin primitive instead")
  fun writeFile(
    filePath: Path,
    contents: String,
  ) {
    try {
      Files.writeString(filePath, contents, StandardCharsets.UTF_8)
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
  }

  /**
   * Read file to a string.
   *
   * @param fullpath path of file with file name
   * @return string representation of the file
   */
  @Deprecated("If calling from kotlin, use kotlin primitive instead")
  @JvmStatic
  fun readFile(fullpath: Path): String {
    try {
      return Files.readString(fullpath, StandardCharsets.UTF_8)
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
  }

  /**
   * Create a [BufferedReader] from an [InputStream].
   *
   * @param inputStream input stream to decorate with a buffered reader
   * @return buffered reader that decorates provided input stream.
   */
  @JvmStatic
  fun newBufferedReader(inputStream: InputStream): BufferedReader = BufferedReader(InputStreamReader(inputStream, StandardCharsets.UTF_8))
}
