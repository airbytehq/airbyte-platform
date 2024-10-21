/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.commons.storage.StorageType
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import jakarta.inject.Singleton
import java.util.regex.Pattern

private val logger = KotlinLogging.logger {}

private val LOG_LINE_PATTERN =
  Pattern.compile(
    """(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(.*(?:\R(?!\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*)*)""",
    Pattern.MULTILINE,
  )
private val TIMESTAMP_PATTERN = "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}).*".toPattern()

private fun MeterRegistry?.createCounter(
  metricName: String,
  logClientType: StorageType,
): Counter? =
  this?.let {
    Counter
      .builder(metricName)
      .tags(MetricTags.LOG_CLIENT_TYPE, logClientType.name.lowercase())
      .register(it)
  }

private fun <T : List<Any>> MeterRegistry?.createGauge(
  metricName: String,
  logClientType: StorageType,
  stateObject: T,
): T =
  this?.gauge(metricName, listOf(Tag.of(MetricTags.LOG_CLIENT_TYPE, logClientType.name.lowercase())), stateObject) { stateObject.size.toDouble() }
    ?: stateObject

private fun MeterRegistry?.createTimer(
  metricName: String,
  logClientType: StorageType,
): Timer? =
  this?.let {
    Timer
      .builder(metricName)
      .tags(MetricTags.LOG_CLIENT_TYPE, logClientType.name.lowercase())
      .register(it)
  }

/**
 * Client that retrieves operation job logs from storage.
 */
@Singleton
class LogClient(
  storageClientFactory: StorageClientFactory,
  private val meterRegistry: MeterRegistry?,
) {
  private val client = storageClientFactory.get(DocumentType.LOGS)

  fun deleteLogs(logPath: String) {
    logger.debug { "Deleting logs from path '$logPath' using ${client.storageType()} storage client..." }
    client.delete(id = logPath)
    logger.debug { "Log delete request complete." }
  }

  fun tailCloudLogs(
    logPath: String,
    numLines: Int,
  ): List<String> {
    logger.debug { "Tailing $numLines line(s) from logs from path '$logPath' using ${client.storageType()} storage client..." }
    val files = client.list(id = logPath)
    logger.debug { "Found ${files.size} files from path '$logPath' using ${client.storageType()} storage client." }

    val instrumentedFiles =
      meterRegistry.createGauge(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILES_RETRIEVED.metricName,
        logClientType = client.storageType(),
        stateObject = files,
      )
    val timer =
      meterRegistry.createTimer(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILES_RETRIEVAL_TIME_MS.metricName,
        logClientType = client.storageType(),
      )

    return if (timer != null) {
      timer.recordCallable { retrieveFiles(files = instrumentedFiles, numLines = numLines) } ?: emptyList()
    } else {
      retrieveFiles(files = instrumentedFiles, numLines = numLines)
    }
  }

  private fun retrieveFiles(
    files: List<String>,
    numLines: Int,
  ): List<String> {
    val lines = mutableListOf<String>()

    val lineCounter =
      meterRegistry.createCounter(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILE_LINE_COUNT_RETRIEVED.metricName,
        logClientType = client.storageType(),
      )
    val byteCounter =
      meterRegistry.createCounter(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILE_LINE_BYTES_RETRIEVED.metricName,
        logClientType = client.storageType(),
      )

    // run is necessary to allow the forEach calls to return early
    run {
      files.forEach { file ->
        val fileLines = extractLogLines(fileContents = client.read(id = file))
        fileLines.forEach { line ->
          lines.add(line)
          lineCounter?.increment()
          byteCounter?.increment(line.length.toDouble())

          if (lines.size >= numLines) {
            return@run
          }
        }
      }
    }

    return orderLogLines(lines = lines)
  }

  private fun extractLogLines(fileContents: String?): List<String> {
    val fileLines = mutableListOf<String>()
    val matcher = fileContents?.let { LOG_LINE_PATTERN.matcher(it) }
    if (matcher != null) {
      while (matcher.find()) {
        fileLines.add("${matcher.group(1)}${matcher.group(2)}")
      }
    }
    return fileLines
  }

  private fun orderLogLines(lines: List<String>): List<String> = lines.sortedBy { extractTimestamp(line = it) }

  private fun extractTimestamp(line: String): String {
    val matcher = TIMESTAMP_PATTERN.matcher(line)
    return if (matcher.find()) {
      matcher.group(1)
    } else {
      // If the line does not include a timestamp for some reason, return empty string to force it to use the original sort
      ""
    }
  }
}
