/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * Common code for handling IO.
 */
public class IOs {

  /**
   * Write a string to a file.
   *
   * @param filePath path with file name to write to
   * @param contents string to write to file
   */
  public static void writeFile(final Path filePath, final String contents) {
    try {
      Files.writeString(filePath, contents, StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Writes a file to a random directory in the /tmp folder. Useful as a staging group for test
   * resources.
   */
  public static String writeFileToRandomTmpDir(final String filename, final String contents) {
    final Path source = Paths.get("/tmp", UUID.randomUUID().toString());
    try {
      final Path tmpFile = source.resolve(filename);
      Files.deleteIfExists(tmpFile);
      Files.createDirectory(source);
      writeFile(tmpFile, contents);
      return tmpFile.toString();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Read file to a string.
   *
   * @param fullpath path of file with file name
   * @return string representation of the file
   */
  public static String readFile(final Path fullpath) {
    try {
      return Files.readString(fullpath, StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create a {@link BufferedReader} from an {@link InputStream}.
   *
   * @param inputStream input stream to decorate with a buffered reader
   * @return buffered reader that decorates provided input stream.
   */
  public static BufferedReader newBufferedReader(final InputStream inputStream) {
    return new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
  }

}
