/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.file_writer;

import java.io.IOException;

/**
 * Handle temporary files containing information required for sending requests.
 */
public interface AirbyteFileWriter {

  /**
   * Writes `contents` to a temporary file prepended with `name`.
   */
  String write(final String name, final String contents) throws IOException;

  /**
   * Deletes files at `filepath`.
   */
  void delete(final String filepath);

}
