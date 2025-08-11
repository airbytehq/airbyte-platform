/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.commandrunner

import io.airbyte.commons.server.builder.exceptions.ConnectorBuilderException
import io.airbyte.connectorbuilder.filewriter.AirbyteArgument
import io.airbyte.connectorbuilder.filewriter.AirbyteFileWriter
import java.io.IOException

/**
 * Wrapper for the Python process launched for sending requests to the CDK connector builder
 * handler.
 *
 * Overrides `close` in order to remove the config and catalog files created in accordance with the
 * Airbyte protocol.
 */
open class AirbyteCdkPythonProcess(
  private val writer: AirbyteFileWriter,
  private val config: AirbyteArgument,
  private val catalog: AirbyteArgument,
  private val processBuilder: ProcessBuilder,
) : AirbyteCdkProcess {
  private var process: Process? = null

  /**
   * Create a python process to process the request.
   */
  @Throws(IOException::class)
  override fun start(): Process {
    if (this.process == null) {
      this.process = processBuilder.start()
      return this.process!!
    }
    throw ConnectorBuilderException("Python process already exists for request.")
  }

  /**
   * Retrieve the python process object.
   */
  override fun getProcess(): Process {
    if (this.process != null) {
      return process!!
    } else {
      throw ConnectorBuilderException("No python process exists for request.")
    }
  }

  /**
   * Stop the process and delete files created for use by the CDK.
   */
  override fun close() {
    if (this.process != null) {
      process!!.destroy()
    }
    writer.delete(config.filepath!!)
    writer.delete(catalog.filepath!!)
  }
}
