/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.async.profiler

import dev.failsafe.Failsafe
import dev.failsafe.function.CheckedRunnable
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Locale
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream

private val logger = KotlinLogging.logger {}

@Singleton
class FileServiceImpl(
  private val failSafeRetryPoliciesImpl: FailSafeRetryPolicies,
) : FileService {
  /**
   * Downloads a file from a URL to a local [dest].
   */
  override fun downloadFile(
    url: String,
    destination: File,
  ) {
    Failsafe.with(failSafeRetryPoliciesImpl.fileDownloadRetryPolicy(url)).run(
      CheckedRunnable {
        URI.create(url).toURL().openStream().use { input ->
          FileOutputStream(destination).use { fos ->
            val buffer = ByteArray(8192)
            var bytesRead: Int
            while (input.read(buffer).also { bytesRead = it } != -1) {
              fos.write(buffer, 0, bytesRead)
            }
          }
        }
      },
    )
    logger.info { "Downloaded: ${destination.absolutePath}" }
  }

  /**
   * Extracts a `.tar.gz` or `.zip` archive using pure-Java approaches (Commons Compress for tar.gz, builtin ZipInputStream for zip).
   */
  override fun extractArchive(archiveFile: File) {
    val fileNameLower = archiveFile.name.lowercase(Locale.getDefault())

    when {
      fileNameLower.endsWith(".tar.gz") -> extractTarGz(archiveFile)
      fileNameLower.endsWith(".zip") -> extractZip(archiveFile)
      else -> throw IllegalArgumentException("Unsupported archive format: ${archiveFile.name}")
    }
    logger.info { "Archive extracted." }
  }

  /**
   * Extract `.tar.gz` using Apache Commons Compress.
   */
  private fun extractTarGz(archive: File) {
    FileInputStream(archive).use { fis ->
      GzipCompressorInputStream(fis).use { gzis ->
        TarArchiveInputStream(gzis).use { tarIn ->
          var entry: TarArchiveEntry? = tarIn.nextEntry
          while (entry != null) {
            val outPath = File(archive.parentFile, entry.name)
            if (entry.isDirectory) {
              if (!outPath.exists() && !outPath.mkdirs()) {
                throw IOException("Failed to create directory: ${outPath.absolutePath}")
              }
            } else {
              outPath.parentFile?.let { parent ->
                if (!parent.exists() && !parent.mkdirs()) {
                  throw IOException("Failed to create directory: ${parent.absolutePath}")
                }
              }
              FileOutputStream(outPath).use { fos ->
                tarIn.copyTo(fos)
              }
            }
            entry = tarIn.nextEntry
          }
        }
      }
    }
  }

  /**
   * Extract `.zip` using built-in ZipInputStream.
   */
  private fun extractZip(archive: File) {
    FileInputStream(archive).use { fis ->
      ZipInputStream(fis).use { zipIn ->
        var entry: ZipEntry? = zipIn.nextEntry
        while (entry != null) {
          val outPath = File(archive.parentFile, entry.name)
          if (entry.isDirectory) {
            if (!outPath.exists() && !outPath.mkdirs()) {
              throw IOException("Failed to create directory: ${outPath.absolutePath}")
            }
          } else {
            outPath.parentFile?.let { parent ->
              if (!parent.exists() && !parent.mkdirs()) {
                throw IOException("Failed to create directory: ${parent.absolutePath}")
              }
            }
            FileOutputStream(outPath).use { fos ->
              zipIn.copyTo(fos)
            }
          }
          zipIn.closeEntry()
          entry = zipIn.nextEntry
        }
      }
    }
  }

  /**
   * Finds `profiler.sh` or `asprof` in the given directory tree (the extracted async-profiler files).
   */
  override fun findProfilerScript(extractionDir: File): File? {
    val files = extractionDir.listFiles() ?: return null
    for (f in files) {
      if (f.isDirectory) {
        findProfilerScript(f)?.let { return it }
      } else if (f.name == "profiler.sh" || f.name == "asprof") {
        return f
      }
    }
    return null
  }

  override fun createTempDirectory(prefix: String): File = Files.createTempDirectory(prefix).toFile()

  override fun fileExists(outputPath: String): Boolean = Files.exists(Paths.get(outputPath))

  /**
   * Recursively deletes the given [path].
   */
  fun deleteRecursively(path: Path) {
    if (Files.notExists(path)) return
    Files
      .walk(path)
      .sorted(Comparator.reverseOrder())
      .forEach {
        try {
          Files.deleteIfExists(it)
        } catch (e: IOException) {
          logger.warn(e) { "Failed to delete $it" }
        }
      }
  }
}
