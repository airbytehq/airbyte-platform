/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.filewriter

import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.io.BufferedWriter
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets

/**
 * Create and delete temporary files.
 */
@Singleton
class AirbyteFileWriterImpl : AirbyteFileWriter {
  /**
   * Create a temporary file.
   */
  @Throws(IOException::class)
  override fun write(
    name: String,
    contents: String,
  ): String {
    val tempDir = File(System.getProperty("java.io.tmpdir"))
    val tempFile = File.createTempFile(name, ".tmp", tempDir)

    FileOutputStream(tempFile, false).use { fos ->
      OutputStreamWriter(fos, StandardCharsets.UTF_8).use { osw ->
        BufferedWriter(osw).use { bw ->
          bw.write(contents)
        }
      }
    }
    log.debug { "{} file written to $name, tempFile.absolutePath" }
    return tempFile.absolutePath
  }

  /**
   * Delete file at `filepath`.
   */
  override fun delete(filepath: String) {
    val file = File(filepath)
    val deleted = file.delete()

    if (deleted) {
      log.debug { "Deleted file: $file.name" }
    } else {
      log.debug { "Failed to delete file $file.name" }
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}
  }
}
