/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.async.profiler

import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

class FileServiceImplTest {
  private val failSafeRetryPolicies: FailSafeRetryPolicies = DummyFailSafeRetryPolicies()
  private val fileService = FileServiceImpl(failSafeRetryPolicies)

  @Test
  fun `downloadFile should correctly download file from a URL`(
    @TempDir tempDir: Path,
  ) {
    // Create a source file with known content.
    val sourceFile = File(tempDir.toFile(), "source.txt")
    val content = "Hello, world!"
    sourceFile.writeText(content)

    // Use the file:// protocol so that URI.create(url).toURL().openStream() works.
    val sourceUrl = sourceFile.toURI().toString()

    // Destination file.
    val destinationFile = File(tempDir.toFile(), "downloaded.txt")

    fileService.downloadFile(sourceUrl, destinationFile)

    // Verify that the downloaded file has the same content.
    assertTrue(destinationFile.exists(), "Downloaded file should exist")
    assertEquals(content, destinationFile.readText(), "Content should match the source")
  }

  @Test
  fun `extractArchive should extract a zip archive correctly`(
    @TempDir tempDir: Path,
  ) {
    // Create a zip archive containing a single file "test.txt".
    val zipFile = File(tempDir.toFile(), "archive.zip")
    ZipOutputStream(FileOutputStream(zipFile)).use { zos ->
      val entryName = "test.txt"
      val entry = ZipEntry(entryName)
      zos.putNextEntry(entry)
      val data = "Zip content".toByteArray()
      zos.write(data)
      zos.closeEntry()
    }

    // Extraction is performed in the archive's parent directory.
    fileService.extractArchive(zipFile)

    // Verify that the file was extracted.
    val extractedFile = File(tempDir.toFile(), "test.txt")
    assertTrue(extractedFile.exists(), "Extracted file should exist")
    assertEquals("Zip content", extractedFile.readText(), "Extracted content should match")
  }

  @Test
  fun `extractArchive should extract a tar gz archive correctly`(
    @TempDir tempDir: Path,
  ) {
    // Create a tar.gz archive containing a single file "testTar.txt".
    val tarGzFile = File(tempDir.toFile(), "archive.tar.gz")
    FileOutputStream(tarGzFile).use { fos ->
      GzipCompressorOutputStream(fos).use { gzos ->
        TarArchiveOutputStream(gzos).use { taos ->
          val entryName = "testTar.txt"
          val data = "Tar content".toByteArray()
          val entry = TarArchiveEntry(entryName)
          entry.size = data.size.toLong()
          taos.putArchiveEntry(entry)
          taos.write(data)
          taos.closeArchiveEntry()
          taos.finish()
        }
      }
    }

    // Extraction is done in the archive's parent directory.
    fileService.extractArchive(tarGzFile)

    // Verify that the file was extracted.
    val extractedFile = File(tempDir.toFile(), "testTar.txt")
    assertTrue(extractedFile.exists(), "Extracted tar file should exist")
    assertEquals("Tar content", extractedFile.readText(), "Extracted content should match")
  }

  @Test
  fun `extractArchive should throw IllegalArgumentException for unsupported formats`(
    @TempDir tempDir: Path,
  ) {
    // Create a file with an unsupported extension.
    val unsupportedArchive = File(tempDir.toFile(), "archive.rar")
    unsupportedArchive.writeText("dummy content")

    // Expect an exception when trying to extract an unsupported archive.
    val exception =
      assertThrows(IllegalArgumentException::class.java) {
        fileService.extractArchive(unsupportedArchive)
      }
    assertTrue(exception.message!!.contains("Unsupported archive format"))
  }

  @Test
  fun `findProfilerScript should find profiler_sh in a nested directory structure`(
    @TempDir tempDir: Path,
  ) {
    // Create a directory structure.
    val rootDir = tempDir.toFile()
    val nestedDir = File(rootDir, "nested")
    assertTrue(nestedDir.mkdirs())
    // Create a file named "profiler.sh".
    val profilerScript = File(nestedDir, "profiler.sh")
    profilerScript.writeText("#!/bin/bash\necho 'Profiler script'")

    // Verify that findProfilerScript returns the correct file.
    val foundScript = fileService.findProfilerScript(rootDir)
    assertNotNull(foundScript, "Should find a profiler script")
    assertEquals("profiler.sh", foundScript!!.name)
  }

  @Test
  fun `findProfilerScript should find asprof in a directory`(
    @TempDir tempDir: Path,
  ) {
    // Create a directory with file "asprof".
    val rootDir = File(tempDir.toFile(), "root")
    assertTrue(rootDir.mkdirs())
    val asprofFile = File(rootDir, "asprof")
    asprofFile.writeText("binary content")

    val foundScript = fileService.findProfilerScript(rootDir)
    assertNotNull(foundScript, "Should find the asprof file")
    assertEquals("asprof", foundScript!!.name)
  }

  @Test
  fun `findProfilerScript should return null if no profiler script is found`(
    @TempDir tempDir: Path,
  ) {
    // Create an empty directory.
    val emptyDir = File(tempDir.toFile(), "empty")
    assertTrue(emptyDir.mkdirs())

    val foundScript = fileService.findProfilerScript(emptyDir)
    assertNull(foundScript, "Should return null if no profiler script is present")
  }

  @Test
  fun `createTempDirectory should create a temporary directory with the given prefix`() {
    val tempDir = fileService.createTempDirectory("testPrefix")
    try {
      assertTrue(tempDir.exists(), "Temporary directory should exist")
      assertTrue(tempDir.isDirectory, "Should be a directory")
      assertTrue(tempDir.name.startsWith("testPrefix"), "Directory name should start with prefix")
    } finally {
      // Cleanup the created temporary directory.
      tempDir.deleteRecursively()
    }
  }

  @Test
  fun `fileExists should return true for an existing file and false otherwise`(
    @TempDir tempDir: Path,
  ) {
    // Create an existing file.
    val existingFile = File(tempDir.toFile(), "exists.txt")
    existingFile.writeText("exists")
    assertTrue(fileService.fileExists(existingFile.absolutePath), "fileExists should return true for existing file")

    // Check for a file that does not exist.
    val nonExistingFile = File(tempDir.toFile(), "nonexistent.txt")
    assertFalse(fileService.fileExists(nonExistingFile.absolutePath), "fileExists should return false for non-existent file")
  }

  @Test
  fun `deleteRecursively should delete directories and files recursively`(
    @TempDir tempDir: Path,
  ) {
    // Create a directory structure with nested files.
    val rootToDelete = tempDir.resolve("toDelete")
    Files.createDirectory(rootToDelete)
    val subDir = rootToDelete.resolve("subdir")
    Files.createDirectory(subDir)
    val fileInSubDir = subDir.resolve("file1.txt")
    Files.write(fileInSubDir, "content".toByteArray())

    // Verify that the directory exists before deletion.
    assertTrue(Files.exists(rootToDelete), "Directory should exist before deletion")

    fileService.deleteRecursively(rootToDelete)

    // Verify that the directory structure is deleted.
    assertFalse(Files.exists(rootToDelete), "Directory should be deleted")
  }
}
