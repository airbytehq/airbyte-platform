/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.io;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class IOsTest {

  private static final String ABC = "abc";
  private static final String FILE = "file";

  @Test
  void testReadWrite() throws IOException {
    final Path path = Files.createTempDirectory("tmp");
    IOs.writeFile(path.resolve(FILE), ABC);
    assertEquals(ABC, IOs.readFile(path.resolve(FILE)));
  }

  @Test
  void testWriteFileToRandomDir() throws IOException {
    final String contents = "something to remember";
    final String tmpFilePath = IOs.writeFileToRandomTmpDir("file.txt", contents);
    assertEquals(contents, Files.readString(Path.of(tmpFilePath), StandardCharsets.UTF_8));
  }

}
