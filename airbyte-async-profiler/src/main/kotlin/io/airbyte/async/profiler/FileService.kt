/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.async.profiler

import java.io.File

interface FileService {
  fun downloadFile(
    url: String,
    destination: File,
  )

  fun extractArchive(archiveFile: File)

  fun findProfilerScript(extractionDir: File): File?

  fun createTempDirectory(prefix: String): File

  fun fileExists(outputPath: String): Boolean
}
