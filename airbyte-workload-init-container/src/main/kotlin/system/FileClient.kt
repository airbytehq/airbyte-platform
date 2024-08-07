package io.airbyte.initContainer.system

import io.airbyte.workers.process.KubePodProcess
import jakarta.inject.Singleton
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

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
  ) {
    Files.writeString(
      Path.of(KubePodProcess.CONFIG_DIR).resolve(fileName),
      fileContents,
      StandardCharsets.UTF_8,
    )
  }
}
