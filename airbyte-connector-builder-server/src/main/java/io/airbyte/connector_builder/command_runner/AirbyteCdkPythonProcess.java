/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.command_runner;

import io.airbyte.connector_builder.exceptions.ConnectorBuilderException;
import io.airbyte.connector_builder.file_writer.AirbyteArgument;
import io.airbyte.connector_builder.file_writer.AirbyteFileWriter;
import java.io.IOException;

/**
 * Wrapper for the Python process launched for sending requests to the CDK connector builder
 * handler.
 *
 * Overrides `close` in order to remove the config and catalog files created in accordance with the
 * Airbyte protocol.
 */
public class AirbyteCdkPythonProcess implements AirbyteCdkProcess {

  private final AirbyteFileWriter writer;
  private final AirbyteArgument config;
  private final AirbyteArgument catalog;
  private final ProcessBuilder processBuilder;
  private Process process;

  public AirbyteCdkPythonProcess(
                                 final AirbyteFileWriter writer,
                                 final AirbyteArgument config,
                                 final AirbyteArgument catalog,
                                 final ProcessBuilder processBuilder) {
    this.writer = writer;
    this.config = config;
    this.catalog = catalog;
    this.processBuilder = processBuilder;
  }

  /**
   * Create a python process to process the request.
   */
  @Override
  public Process start() throws IOException {
    if (this.process == null) {
      this.process = this.processBuilder.start();
      return this.process;
    }
    throw new ConnectorBuilderException("Python process already exists for request.");
  }

  /**
   * Retrieve the python process object.
   */
  @Override
  public Process getProcess() {
    if (this.process != null) {
      return this.process;
    } else {
      throw new ConnectorBuilderException("No python process exists for request.");
    }
  }

  /**
   * Stop the process and delete files created for use by the CDK.
   */
  @Override
  public void close() {
    if (this.process != null) {
      this.process.destroy();
    }
    this.writer.delete(this.config.getFilepath());
    this.writer.delete(this.catalog.getFilepath());
  }

}
