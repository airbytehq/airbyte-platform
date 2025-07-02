/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.commandrunner

import io.airbyte.connectorbuilder.exceptions.AirbyteCdkInvalidInputException
import io.airbyte.connectorbuilder.filewriter.AirbyteFileWriter
import io.airbyte.workers.internal.AirbyteStreamFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream

/**
 * Mocks the creation of a synchronous python CDK command.
 */
class MockSynchronousPythonCdkCommandRunner(
  writer: AirbyteFileWriter,
  streamFactory: AirbyteStreamFactory,
  private var shouldThrow: Boolean,
  private var exitCode: Int,
  inputStream: InputStream? = null,
  errorStream: InputStream? = null,
  outputStream: OutputStream? = null,
) : SynchronousPythonCdkCommandRunner(writer, streamFactory, "", "", "", false) {
  private val inputStream = inputStream ?: ByteArrayInputStream(ByteArray(0))
  private val errorStream = errorStream ?: ByteArrayInputStream(ByteArray(0))
  private val outputStream = outputStream ?: ByteArrayOutputStream(1)

  @Throws(AirbyteCdkInvalidInputException::class, IOException::class)
  override fun start(
    cdkCommand: String,
    configFilepath: String,
    catalogFilepath: String,
    stateFilepath: String,
  ): AirbyteCdkProcess {
    if (this.shouldThrow) {
      throw IOException()
    }
    val process =
      MockProcess(
        this.exitCode,
        errorStream!!,
        inputStream!!,
        outputStream!!,
      )
    return MockAirbyteCdkProcess(process)
  }
}
