/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.io

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets

/**
 * Common code for handling IO.
 */
object IOs {
  /**
   * Create a [BufferedReader] from an [InputStream].
   *
   * @param inputStream input stream to decorate with a buffered reader
   * @return buffered reader that decorates provided input stream.
   */
  @JvmStatic
  fun newBufferedReader(inputStream: InputStream): BufferedReader = BufferedReader(InputStreamReader(inputStream, StandardCharsets.UTF_8))
}
