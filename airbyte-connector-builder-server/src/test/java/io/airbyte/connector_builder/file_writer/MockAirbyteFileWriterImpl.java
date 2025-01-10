/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.file_writer;

import jakarta.inject.Singleton;
import java.io.IOException;

/**
 * Mocks writing & deleting files.
 */
@Singleton
public class MockAirbyteFileWriterImpl implements AirbyteFileWriter {

  public boolean shouldThrow = false;

  /**
   * Mocks writing files.
   */
  @Override
  public String write(final String name, final String contents) throws IOException {
    if (this.shouldThrow) {
      throw new IOException();
    }
    return "";
  }

  @Override
  public void delete(final String filepath) {}

}
