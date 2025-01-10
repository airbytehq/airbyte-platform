/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.kotlin.readValue
import io.airbyte.commons.logging.logback.STRUCTURED_LOG_FILE_EXTENSION
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
  val mapper: ObjectMapper,
  private val logEventLayout: LogEventLayout,
  private val meterRegistry: MeterRegistry?,
) {
  private val client = storageClientFactory.create(DocumentType.LOGS)

  // Copy the mapper to avoid changing deserialization for all usages in the containing application
  private val objectMapper = mapper.copy()

  init {
    val structuredLogEventModule = SimpleModule()
    structuredLogEventModule.addDeserializer(StackTraceElement::class.java, StackTraceElementDeserializer())
    objectMapper.registerModule(structuredLogEventModule)
  }

  fun deleteLogs(logPath: String) {
    logger.debug { "Deleting logs from path '$logPath' using ${client.storageType} storage client..." }
    client.delete(id = logPath)
    logger.debug { "Log delete request complete." }
  }

  fun getLogs(
    logPath: String,
    numLines: Int,
  ): LogEvents {
    logger.debug { "Tailing $numLines line(s) from logs from path '$logPath' using ${client.storageType} storage client..." }
    val files = client.list(id = logPath).filter { it.endsWith(STRUCTURED_LOG_FILE_EXTENSION) }
    logger.debug { "Found ${files.size} files from path '$logPath' using ${client.storageType} storage client." }

    val instrumentedFiles =
      meterRegistry.createGauge(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILES_RETRIEVED.metricName,
        logClientType = client.storageType,
        stateObject = files,
      )
    val timer =
      meterRegistry.createTimer(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILES_RETRIEVAL_TIME_MS.metricName,
        logClientType = client.storageType,
      )
    val lineCounter =
      meterRegistry.createCounter(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILE_LINE_COUNT_RETRIEVED.metricName,
        logClientType = client.storageType,
      )
    val byteCounter =
      meterRegistry.createCounter(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILE_LINE_BYTES_RETRIEVED.metricName,
        logClientType = client.storageType,
      )

    val events =
      if (timer != null) {
        timer.recordCallable {
          readStructuredLogs(files = instrumentedFiles, numLines = numLines, lineCounter = lineCounter, byteCounter = byteCounter)
        } ?: emptyList()
      } else {
        readStructuredLogs(files = instrumentedFiles, numLines = numLines, lineCounter = lineCounter, byteCounter = byteCounter)
      }
    return LogEvents(events = events)
  }

  fun tailCloudLogs(
    logPath: String,
    numLines: Int,
  ): List<String> {
    logger.debug { "Tailing $numLines line(s) from logs from path '$logPath' using ${client.storageType} storage client..." }
    val files = client.list(id = logPath)
    logger.debug { "Found ${files.size} files from path '$logPath' using ${client.storageType} storage client." }

    val instrumentedFiles =
      meterRegistry.createGauge(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILES_RETRIEVED.metricName,
        logClientType = client.storageType,
        stateObject = files,
      )
    val timer =
      meterRegistry.createTimer(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILES_RETRIEVAL_TIME_MS.metricName,
        logClientType = client.storageType,
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
    val lineCounter =
      meterRegistry.createCounter(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILE_LINE_COUNT_RETRIEVED.metricName,
        logClientType = client.storageType,
      )
    val byteCounter =
      meterRegistry.createCounter(
        metricName = OssMetricsRegistry.LOG_CLIENT_FILE_LINE_BYTES_RETRIEVED.metricName,
        logClientType = client.storageType,
      )

    val isStructured = files.all { it.endsWith(suffix = STRUCTURED_LOG_FILE_EXTENSION) }

    /*
     * This logic is here to handle logs created before the introduction of structured logs.  If any of the log files
     * written for a job are unstructured, use the old logic.  Otherwise, use the new logic to treat the file contents
     * as structured events.
     */
    return if (isStructured) {
      formatStructuredLogs(events = readStructuredLogs(files = files, numLines = numLines, lineCounter = lineCounter, byteCounter = byteCounter))
    } else {
      handleUnstructuredLogs(files = files, numLines = numLines, lineCounter = lineCounter, byteCounter = byteCounter)
    }
  }

  private fun formatStructuredLogs(events: List<LogEvent>): List<String> = events.map { logEventLayout.doLayout(logEvent = it) }

  private fun readStructuredLogs(
    files: List<String>,
    numLines: Int,
    lineCounter: Counter?,
    byteCounter: Counter?,
  ): List<LogEvent> {
    var count = 0
    val logLines =
      files
        .asSequence()
        .map { file ->
          val events = client.read(id = file)
          byteCounter?.increment(events?.length?.toDouble() ?: 0.0)
          extractEvents(events = events)
        }.flatMap { it.events }
        .takeWhile {
          count++
          lineCounter?.increment()
          count <= numLines
        }.sortedBy { it.timestamp }
        .toList()
    return logLines
  }

  private fun handleUnstructuredLogs(
    files: List<String>,
    numLines: Int,
    lineCounter: Counter?,
    byteCounter: Counter?,
  ): List<String> {
    val lines = mutableListOf<String>()

    // run is necessary to allow the forEach calls to return early
    run {
      files.forEach { file ->
        val fileLines =
          if (file.endsWith(suffix = STRUCTURED_LOG_FILE_EXTENSION)) {
            val logEvents = extractEvents(events = client.read(id = file))
            logEvents.events.map(logEventLayout::doLayout)
          } else {
            extractLogLines(fileContents = client.read(id = file))
          }
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

  private fun extractEvents(events: String?): LogEvents = events?.let { objectMapper.readValue(it) } ?: LogEvents(events = emptyList())

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
