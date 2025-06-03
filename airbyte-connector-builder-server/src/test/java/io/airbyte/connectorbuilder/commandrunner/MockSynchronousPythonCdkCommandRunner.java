/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.commandrunner;

import io.airbyte.connectorbuilder.exceptions.AirbyteCdkInvalidInputException;
import io.airbyte.connectorbuilder.filewriter.AirbyteFileWriter;
import io.airbyte.workers.internal.AirbyteStreamFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Mocks the creation of a synchronous python CDK command.
 */
public class MockSynchronousPythonCdkCommandRunner extends SynchronousPythonCdkCommandRunner {

  public boolean shouldThrow;
  public int exitCode;
  InputStream inputStream;
  InputStream errorStream;
  OutputStream outputStream;

  public MockSynchronousPythonCdkCommandRunner(
                                               final AirbyteFileWriter writer,
                                               final AirbyteStreamFactory streamFactory) {
    super(writer, streamFactory, "", "", "", false);
  }

  public MockSynchronousPythonCdkCommandRunner(
                                               final AirbyteFileWriter writer,
                                               final AirbyteStreamFactory streamFactory,
                                               final boolean shouldThrow,
                                               final int exitCode,
                                               final InputStream inputStream,
                                               final InputStream errorStream,
                                               final OutputStream outputStream) {
    super(writer, streamFactory, "", "", "", false);
    this.shouldThrow = shouldThrow;
    this.exitCode = exitCode;
    this.inputStream = inputStream;
    this.errorStream = errorStream;
    this.outputStream = outputStream;
  }

  @Override
  AirbyteCdkProcess start(
                          final String cdkCommand,
                          final String configFilepath,
                          final String catalogFilepath,
                          final String stateFilepath)
      throws AirbyteCdkInvalidInputException, IOException {
    if (this.shouldThrow) {
      throw new IOException();
    }
    final MockProcess process = new MockProcess(
        this.exitCode,
        this.errorStream,
        this.inputStream,
        this.outputStream);
    return new MockAirbyteCdkProcess(process);
  }

}
