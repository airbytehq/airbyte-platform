/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.commandrunner

import java.io.InputStream
import java.io.OutputStream

/**
 * Mocks the Process class.
 */
class MockProcess(
  private var exitValue: Int,
  private var errorStream: InputStream,
  private var inputStream: InputStream,
  private var outputStream: OutputStream,
) : Process() {
  override fun getErrorStream(): InputStream = this.errorStream

  override fun getInputStream(): InputStream = this.inputStream

  override fun getOutputStream(): OutputStream = this.outputStream

  override fun exitValue(): Int = this.exitValue

  override fun waitFor(): Int = 0

  override fun destroy() {}

  override fun info(): ProcessHandle.Info? = null

  override fun isAlive(): Boolean = false
}
