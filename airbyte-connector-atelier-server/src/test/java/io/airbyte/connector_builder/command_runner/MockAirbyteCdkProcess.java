/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.command_runner;

import java.io.IOException;

/**
 * Mock class for wrapping a process.
 */
public class MockAirbyteCdkProcess extends AirbyteCdkPythonProcess {

  private final Process process;

  public MockAirbyteCdkProcess(final Process process) {
    super(null, null, null, null);
    this.process = process;
  }

  @Override
  public Process start() throws IOException {
    return this.getProcess();
  }

  @Override
  public Process getProcess() {
    return this.process;
  }

  @Override
  public void close() {}

}
