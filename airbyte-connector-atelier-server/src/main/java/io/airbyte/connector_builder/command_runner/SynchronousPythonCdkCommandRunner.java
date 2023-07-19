/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.command_runner;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import datadog.trace.api.Trace;
import io.airbyte.connector_builder.TracingHelper;
import io.airbyte.connector_builder.file_writer.AirbyteArgument;
import io.airbyte.connector_builder.file_writer.AirbyteFileWriter;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.workers.internal.AirbyteStreamFactory;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Communicates with the CDK's Connector Builder handler by launching a Python process via an
 * Airbyte `read` command.
 */
@Singleton
public class SynchronousPythonCdkCommandRunner implements SynchronousCdkCommandRunner {

  private final AirbyteFileWriter writer;
  private final AirbyteStreamFactory streamFactory;
  private final String python;
  private final String cdkEntrypoint;

  private static final Logger LOGGER = LoggerFactory.getLogger(SynchronousPythonCdkCommandRunner.class);

  @Inject
  public SynchronousPythonCdkCommandRunner(
                                           final AirbyteFileWriter writer,
                                           final AirbyteStreamFactory streamFactory,
                                           final String python,
                                           final String cdkEntrypoint) {
    this.writer = writer;
    this.streamFactory = streamFactory;
    this.python = python;
    this.cdkEntrypoint = cdkEntrypoint;
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
                                         final String catalogContents)
      throws IOException {
    try (final AirbyteCdkProcess cdkProcess = this.start(cdkCommand, configContents, catalogContents)) {
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
                          final String catalogContents)
      throws IOException {
    final AirbyteArgument catalog = this.write("catalog", catalogContents);
    final AirbyteArgument config = this.write("config", configContents);

    final List<String> command = Lists.newArrayList(
        this.python,
        this.cdkEntrypoint,
        "read",
        "--config",
        config.getFilepath(),
        "--catalog",
        catalog.getFilepath());
    LOGGER.debug("Preparing command for {}: {}", cdkCommand, Joiner.on(" ").join(command));
    final ProcessBuilder processBuilder = new ProcessBuilder(command);
    final AirbyteCdkPythonProcess cdkProcess = new AirbyteCdkPythonProcess(
        writer, config, catalog, processBuilder);
    cdkProcess.start();
    return cdkProcess;
  }

  AirbyteArgument write(final String name, final String contents) throws IOException {
    final AirbyteArgument arg = new AirbyteArgument(this.writer);
    arg.setUpArg(name, contents);
    return arg;
  }

}
