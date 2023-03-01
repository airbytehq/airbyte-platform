/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.io;

import com.google.common.base.Charsets;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.input.ReversedLinesFileReader;

/**
 * Common code for handling IO.
 */
public class IOs {

  /**
   * Write a string to a file.
   *
   * @param path path to write to (without filename)
   * @param fileName name of file
   * @param contents string to write to file
   * @return path of file that was written to
   */
  public static Path writeFile(final Path path, final String fileName, final String contents) {
    final Path filePath = path.resolve(fileName);
    return writeFile(filePath, contents);
  }

  /**
   * Write a byte array to a file.
   *
   * @param filePath path with file name to write to
   * @param contents byte array to write to file
   * @return path of file that was written to
   */
  public static Path writeFile(final Path filePath, final byte[] contents) {
    try {
      Files.write(filePath, contents);
      return filePath;
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Write a string to a file.
   *
   * @param filePath path with file name to write to
   * @param contents string to write to file
   * @return path of file that was written to
   */
  public static Path writeFile(final Path filePath, final String contents) {
    try {
      Files.writeString(filePath, contents, StandardCharsets.UTF_8);
      return filePath;
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
   * @param path path of file
   * @param fileName name of file
   * @return string representation of the file
   */
  public static String readFile(final Path path, final String fileName) {
    return readFile(path.resolve(fileName));
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
   * Read last N lines from a file into a list of string. Each element is a separate line from the
   * file.
   *
   * @param numLines number of lines to read
   * @param path path of file with file name
   * @return list of strings where each element is a separate line from the file.
   * @throws IOException exception while reading the file
   */
  public static List<String> getTail(final int numLines, final Path path) throws IOException {
    if (path == null) {
      return Collections.emptyList();
    }

    final File file = path.toFile();
    if (!file.exists()) {
      return Collections.emptyList();
    }

    try (final ReversedLinesFileReader fileReader = new ReversedLinesFileReader(file, Charsets.UTF_8)) {
      final List<String> lines = new ArrayList<>();

      String line = fileReader.readLine();
      while (line != null && lines.size() < numLines) {
        lines.add(line);
        line = fileReader.readLine();
      }

      Collections.reverse(lines);

      return lines;
    }
  }

  /**
   * Create an {@link InputStream} from a filepath.
   *
   * @param path path to create input stream for
   * @return input stream of the path
   */
  public static InputStream inputStream(final Path path) {
    try {
      return Files.newInputStream(path);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Close a {@link Closeable} and throw any {@link IOException} as a {@link RuntimeException}.
   *
   * @param closeable closeable whose exception to convert
   */
  public static void silentClose(final Closeable closeable) {
    try {
      closeable.close();
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
