/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.file_writer;

import jakarta.inject.Singleton;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create and delete temporary files.
 */
@Singleton
public class AirbyteFileWriterImpl implements AirbyteFileWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(AirbyteFileWriterImpl.class);

  /**
   * Create a temporary file.
   */
  @Override
  public String write(final String name, final String contents) throws IOException {
    final File tempDir = new File(System.getProperty("java.io.tmpdir"));
    final File tempFile = File.createTempFile(name, ".tmp", tempDir);

    try (final FileOutputStream fos = new FileOutputStream(tempFile, false);
        final OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
        final BufferedWriter bw = new BufferedWriter(osw)) {
      bw.write(contents);
    }

    LOGGER.debug("{} file written to {}", name, tempFile.getAbsolutePath());
    return tempFile.getAbsolutePath();
  }

  /**
   * Delete file at `filepath`.
   */
  @Override
  public void delete(final String filepath) {
    final File file = new File(filepath);
    final boolean deleted = file.delete();

    if (deleted) {
      LOGGER.debug("Deleted file: {}", file.getName());
    } else {
      LOGGER.debug("Failed to delete file {}", file.getName());
    }
  }

}
