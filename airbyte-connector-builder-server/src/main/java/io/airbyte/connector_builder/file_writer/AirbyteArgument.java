/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.file_writer;

import java.io.IOException;

/**
 * Exposes a way of constructing arguments for Airbyte protocol commands.
 */
public class AirbyteArgument {

  private String filepath;
  private final AirbyteFileWriter writer;

  public AirbyteArgument(final AirbyteFileWriter writer) {
    this.writer = writer;
  }

  public void setUpArg(final String name, final String contents) throws IOException {
    final String filepath = this.writer.write(name, contents);
    this.setFilepath(filepath);
  }

  public String getFilepath() {
    return this.filepath;
  }

  public void setFilepath(final String filepath) {
    this.filepath = filepath;
  }

}
