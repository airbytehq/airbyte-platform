/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.commandrunner

import datadog.trace.api.Trace
import io.airbyte.commons.envvar.EnvVar
import io.airbyte.connectorbuilder.TracingHelper
import io.airbyte.connectorbuilder.filewriter.AirbyteArgument
import io.airbyte.connectorbuilder.filewriter.AirbyteFileWriter
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.workers.internal.AirbyteStreamFactory
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.IOException

// TODO: Remove this and invoke directly

/**
 * Communicates with the CDK's Connector Builder handler by launching a Python process via an
 * Airbyte `read` command.
 */
open class SynchronousPythonCdkCommandRunner(
  private val writer: AirbyteFileWriter,
  private val streamFactory: AirbyteStreamFactory,
  private val python: String,
  private val cdkEntrypoint: String,
  // `:` separated path to the modules that will be imported by the Python executable
  // Custom components must be in one of these modules to be loaded
  private val pythonPath: String,
  private val enableUnsafeCodeGlobalOverride: Boolean,
) : SynchronousCdkCommandRunner {
  /**
   * Start the python process that will handle the command, and return the parsed AirbyteRecordMessage
   * returned by the CDK.
   */
  @Trace(operationName = TracingHelper.CONNECTOR_BUILDER_OPERATION_NAME)
  @Throws(IOException::class)
  override fun runCommand(
    cdkCommand: String,
    configContents: String,
    catalogContents: String,
    stateContents: String,
  ): AirbyteRecordMessage {
    start(
      cdkCommand,
      configContents,
      catalogContents,
      stateContents,
    ).use { cdkProcess ->
      return ProcessOutputParser().parse(cdkProcess.getProcess(), this.streamFactory, cdkCommand)
    }
  }

  /**
   * Start the python process. NOTE: This method should be called within a try-with-resources
   * statement, to ensure that the files are cleaned up after the process is done.
   */
  @Trace(operationName = TracingHelper.CONNECTOR_BUILDER_OPERATION_NAME)
  @Throws(IOException::class)
  open fun start(
    cdkCommand: String,
    configContents: String,
    catalogContents: String,
    stateContents: String,
  ): AirbyteCdkProcess {
    val catalog = this.write("catalog", catalogContents)
    val config = this.write("config", configContents)
    val state = this.write("state", stateContents)

    val command: List<String?> =
      listOf(
        this.python, // TODO: Remove this and invoke directly
        this.cdkEntrypoint,
        "read",
        "--config",
        config.filepath,
        "--catalog",
        catalog.filepath,
        "--state",
        state.filepath,
      )
    log.debug { "Preparing command for {}: $cdkCommand, ${command.joinToString(" ")}" }
    val processBuilder = ProcessBuilder(command)
    addPythonPathToSubprocessEnvironment(processBuilder)
    applyUnsafeCodeExecutionVariable(processBuilder)

    val cdkProcess =
      AirbyteCdkPythonProcess(
        writer,
        config,
        catalog,
        processBuilder,
      )
    cdkProcess.start()
    return cdkProcess
  }

  @Throws(IOException::class)
  private fun write(
    name: String,
    contents: String,
  ): AirbyteArgument {
    val arg = AirbyteArgument(this.writer)
    arg.setUpArg(name, contents)
    return arg
  }

  // TODO: Remove this and invoke directly
  private fun addPythonPathToSubprocessEnvironment(processBuilder: ProcessBuilder) {
    processBuilder.environment()["PYTHONPATH"] = pythonPath
  }

  /**
   * Enable unsafe code execution in the CDK. This method sets an environment variable to allow custom
   * code execution in the CDK. It should only be used in development environments or run in a secure
   * environment.
   *
   * @param processBuilder the ProcessBuilder instance to which the environment variable will be added
   */
  private fun applyUnsafeCodeExecutionVariable(processBuilder: ProcessBuilder) {
    processBuilder.environment()[EnvVar.AIRBYTE_ENABLE_UNSAFE_CODE.toString()] =
      enableUnsafeCodeGlobalOverride.toString()
  }

  companion object {
    private val log = KotlinLogging.logger {}
  }
}
