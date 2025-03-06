/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.command_runner;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Mocks the Process class.
 */
public class MockProcess extends Process {

  int exitValue;
  InputStream inputStream;
  InputStream errorStream;
  OutputStream outputStream;

  public MockProcess(
                     final int exitValue,
                     final InputStream errorStream,
                     final InputStream inputStream,
                     final OutputStream outputStream) {
    this.exitValue = exitValue;
    this.errorStream = errorStream;
    this.inputStream = inputStream;
    this.outputStream = outputStream;
  }

  @Override
  public InputStream getErrorStream() {
    return this.errorStream;
  }

  @Override
  public InputStream getInputStream() {
    return this.inputStream;
  }

  @Override
  public OutputStream getOutputStream() {
    return this.outputStream;
  }

  @Override
  public int exitValue() {
    return this.exitValue;
  }

  @Override
  public int waitFor() {
    return 0;
  }

  @Override
  public void destroy() {}

  @Override
  public ProcessHandle.Info info() {
    return null;
  }

  @Override
  public boolean isAlive() {
    return false;
  }

}
