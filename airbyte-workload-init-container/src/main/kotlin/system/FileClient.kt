/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.initContainer.system

import io.airbyte.workers.pod.FileConstants
import io.airbyte.workers.pod.FileConstants.DEST_DIR
import io.airbyte.workers.pod.FileConstants.SOURCE_DIR
import io.airbyte.workers.pod.FileConstants.STDERR_PIPE_FILE
import io.airbyte.workers.pod.FileConstants.STDIN_PIPE_FILE
import io.airbyte.workers.pod.FileConstants.STDOUT_PIPE_FILE
import jakarta.inject.Singleton
import java.nio.charset.StandardCharsets
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermission
import java.util.EnumSet

/**
 * Wrapper around the Files API for encapsulation and testing purposes.
 */
@Singleton
class FileClient {
  /**
   * Writes the contents in UTF 8 to the designated filename in the config directory.
   */
  fun writeInputFile(
    fileName: String,
    fileContents: String,
    baseDir: String = FileConstants.CONFIG_DIR,
  ) {
    Files.writeString(
      Path.of(baseDir).resolve(fileName),
      fileContents,
      StandardCharsets.UTF_8,
    )
  }

  fun makeNamedPipes(
    sourceDir: String = SOURCE_DIR,
    destDir: String = DEST_DIR,
  ) {
    makeNamedPipe("$sourceDir/$STDOUT_PIPE_FILE")
    makeNamedPipe("$sourceDir/$STDERR_PIPE_FILE")
    makeNamedPipe("$destDir/$STDOUT_PIPE_FILE")
    makeNamedPipe("$destDir/$STDERR_PIPE_FILE")
    makeNamedPipe("$destDir/$STDIN_PIPE_FILE")
  }

  private fun makeNamedPipe(path: String) {
    val pb = ProcessBuilder("mkfifo", path)
    val process = pb.start()
    process.waitFor()

    setPermissions(path)
  }

  private fun setPermissions(filePath: String) {
    val path: Path = FileSystems.getDefault().getPath(filePath)
    Files.setPosixFilePermissions(path, pipePermissions)
  }

  companion object {
    val pipePermissions =
      EnumSet.of(
        PosixFilePermission.OWNER_READ,
        PosixFilePermission.OWNER_WRITE,
        PosixFilePermission.GROUP_READ,
        PosixFilePermission.GROUP_WRITE,
        PosixFilePermission.OTHERS_READ,
        PosixFilePermission.OTHERS_WRITE,
      )
  }
}
