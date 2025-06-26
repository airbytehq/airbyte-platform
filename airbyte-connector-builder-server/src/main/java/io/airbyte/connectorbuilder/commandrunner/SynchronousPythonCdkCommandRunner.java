/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.commandrunner;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import datadog.trace.api.Trace;
import io.airbyte.commons.envvar.EnvVar;
import io.airbyte.connectorbuilder.TracingHelper;
import io.airbyte.connectorbuilder.filewriter.AirbyteArgument;
import io.airbyte.connectorbuilder.filewriter.AirbyteFileWriter;
import io.airbyte.protocol.models.v0.AirbyteRecordMessage;
import io.airbyte.workers.internal.AirbyteStreamFactory;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Communicates with the CDK's Connector Builder handler by launching a Python process via an
 * Airbyte `read` command.
 */
public class SynchronousPythonCdkCommandRunner implements SynchronousCdkCommandRunner {

  private final AirbyteFileWriter writer;
  private final AirbyteStreamFactory streamFactory;
  private final String python;
  private final String cdkEntrypoint;

  // `:` separated path to the modules that will be imported by the Python executable
  // Custom components must be in one of these modules to be loaded
  private final String pythonPath;

  private final Boolean enableUnsafeCodeGlobalOverride;

  private static final Logger LOGGER = LoggerFactory.getLogger(SynchronousPythonCdkCommandRunner.class);

  public SynchronousPythonCdkCommandRunner(
                                           final AirbyteFileWriter writer,
                                           final AirbyteStreamFactory streamFactory,
                                           final String python,
                                           final String cdkEntrypoint,
                                           final String pythonPath,
                                           final Boolean enableUnsafeCodeGlobalOverride) {
    this.writer = writer;
    this.streamFactory = streamFactory;
    this.python = python; // TODO: Remove this and invoke directly
    this.cdkEntrypoint = cdkEntrypoint;
    this.pythonPath = pythonPath; // TODO: Remove this and invoke directly
    this.enableUnsafeCodeGlobalOverride = enableUnsafeCodeGlobalOverride;
  }

  /**
   * Start the python process that will handle the command, and return the parsed AirbyteRecordMessage
   * returned by the CDK.
   */
  @Override
  @Trace(operationName = TracingHelper.CONNECTOR_BUILDER_OPERATION_NAME)
  public AirbyteRecordMessage runCommand(
                                         final String cdkCommand,
                                         final String configContents,
                                         final String catalogContents,
                                         final String stateContents)
      throws IOException {
    try (final AirbyteCdkProcess cdkProcess = this.start(cdkCommand, configContents, catalogContents,
        stateContents)) {
      return new ProcessOutputParser().parse(cdkProcess.getProcess(), this.streamFactory, cdkCommand);
    }
  }

  /**
   * Start the python process. NOTE: This method should be called within a try-with-resources
   * statement, to ensure that the files are cleaned up after the process is done.
   */
  @Trace(operationName = TracingHelper.CONNECTOR_BUILDER_OPERATION_NAME)
  AirbyteCdkProcess start(
                          final String cdkCommand,
                          final String configContents,
                          final String catalogContents,
                          final String stateContents)
      throws IOException {
    final AirbyteArgument catalog = this.write("catalog", catalogContents);
    final AirbyteArgument config = this.write("config", configContents);
    final AirbyteArgument state = this.write("state", stateContents);

    final List<String> command = Lists.newArrayList(
        this.python, // TODO: Remove this and invoke directly
        this.cdkEntrypoint,
        "read",
        "--config",
        config.getFilepath(),
        "--catalog",
        catalog.getFilepath(),
        "--state",
        state.getFilepath());
    LOGGER.debug("Preparing command for {}: {}", cdkCommand, Joiner.on(" ").join(command));
    final ProcessBuilder processBuilder = new ProcessBuilder(command);
    addPythonPathToSubprocessEnvironment(processBuilder);
    applyUnsafeCodeExecutionVariable(processBuilder);

    final AirbyteCdkPythonProcess cdkProcess = new AirbyteCdkPythonProcess(
        writer, config, catalog, processBuilder);
    cdkProcess.start();
    return cdkProcess;
  }

  private AirbyteArgument write(final String name, final String contents) throws IOException {
    final AirbyteArgument arg = new AirbyteArgument(this.writer);
    arg.setUpArg(name, contents);
    return arg;
  }

  // TODO: Remove this and invoke directly
  private void addPythonPathToSubprocessEnvironment(ProcessBuilder processBuilder) {
    processBuilder.environment().put("PYTHONPATH", this.pythonPath);
  }

  /**
   * Enable unsafe code execution in the CDK. This method sets an environment variable to allow custom
   * code execution in the CDK. It should only be used in development environments or run in a secure
   * environment.
   *
   * @param processBuilder the ProcessBuilder instance to which the environment variable will be added
   */
  private void applyUnsafeCodeExecutionVariable(ProcessBuilder processBuilder) {
    processBuilder.environment().put(EnvVar.AIRBYTE_ENABLE_UNSAFE_CODE.toString(), enableUnsafeCodeGlobalOverride.toString());
  }

}
