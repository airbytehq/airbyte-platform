/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.initContainer.system

import io.airbyte.initContainer.system.FileClient.Companion.pipePermissions
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.workers.pod.FileConstants.STDERR_PIPE_FILE
import io.airbyte.workers.pod.FileConstants.STDIN_PIPE_FILE
import io.airbyte.workers.pod.FileConstants.STDOUT_PIPE_FILE
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.io.File
import java.nio.file.Path
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.createTempDirectory
import kotlin.io.path.deleteRecursively
import kotlin.io.path.exists
import kotlin.io.path.getPosixFilePermissions
import kotlin.io.path.listDirectoryEntries

@ExtendWith(MockKExtension::class)
internal class FileClientTest {
  @MockK(relaxed = true)
  lateinit var metricClient: MetricClient

  private lateinit var fileClient: FileClient

  @BeforeEach
  internal fun setUp() {
    fileClient = FileClient(metricClient)
  }

  @Test
  internal fun testWriteInputFile() {
    val inputFile = File.createTempFile("input", "txt")
    try {
      val contents = "This is a test"
      fileClient.writeInputFile(fileName = inputFile.absolutePath, fileContents = contents)
      assertEquals(contents, inputFile.readText())
    } finally {
      inputFile.delete()
    }
  }

  @OptIn(ExperimentalPathApi::class)
  @Test
  internal fun testMakeNamedPipes() {
    val sourceDir = createTempDirectory(prefix = "source")
    val destDir = createTempDirectory(prefix = "dest")
    try {
      fileClient.makeNamedPipes(sourceDir = sourceDir.toString(), destDir = destDir.toString())

      // Validate pipe counts per connector type
      assertEquals(2, sourceDir.listDirectoryEntries().size)
      assertEquals(3, destDir.listDirectoryEntries().size)

      // Source pipes
      assertEquals(true, Path.of(sourceDir.toString(), STDOUT_PIPE_FILE).exists())
      assertEquals(pipePermissions, Path.of(sourceDir.toString(), STDOUT_PIPE_FILE).getPosixFilePermissions())
      assertEquals(true, Path.of(sourceDir.toString(), STDERR_PIPE_FILE).exists())
      assertEquals(pipePermissions, Path.of(sourceDir.toString(), STDERR_PIPE_FILE).getPosixFilePermissions())
      assertEquals(false, Path.of(sourceDir.toString(), STDIN_PIPE_FILE).exists())

      // Destination pipes
      assertEquals(true, Path.of(destDir.toString(), STDOUT_PIPE_FILE).exists())
      assertEquals(pipePermissions, Path.of(destDir.toString(), STDOUT_PIPE_FILE).getPosixFilePermissions())
      assertEquals(true, Path.of(destDir.toString(), STDERR_PIPE_FILE).exists())
      assertEquals(pipePermissions, Path.of(destDir.toString(), STDERR_PIPE_FILE).getPosixFilePermissions())
      assertEquals(true, Path.of(destDir.toString(), STDIN_PIPE_FILE).exists())
      assertEquals(pipePermissions, Path.of(destDir.toString(), STDIN_PIPE_FILE).getPosixFilePermissions())
    } finally {
      sourceDir.deleteRecursively()
      destDir.deleteRecursively()
    }
  }
}
