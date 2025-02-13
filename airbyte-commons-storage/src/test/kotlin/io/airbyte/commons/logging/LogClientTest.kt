/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import ch.qos.logback.classic.Level
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.commons.logging.logback.STRUCTURED_LOG_FILE_EXTENSION
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.commons.storage.StorageType
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import kotlin.io.path.createTempFile
import kotlin.io.path.pathString

internal class LogClientTest {
  private lateinit var logUtils: LogUtils
  private lateinit var logEventLayout: LogEventLayout
  private lateinit var objectMapper: ObjectMapper

  @BeforeEach
  fun setup() {
    logUtils = LogUtils()
    logUtils.init()
    logEventLayout = LogEventLayout(logUtils = logUtils)
    objectMapper = MoreMappers.initMapper()
    val module = SimpleModule()
    module.addDeserializer(StackTraceElement::class.java, StackTraceElementDeserializer())
    module.addSerializer(StackTraceElement::class.java, StackTraceElementSerializer())
    objectMapper.registerModules(module)
  }

  @AfterEach
  fun teardown() {
    logUtils.close()
  }

  @Test
  fun testDeleteLogs() {
    val storageClient =
      mockk<StorageClient> {
        every { delete(any()) } returns true
      }
    val storageClientFactory =
      mockk<StorageClientFactory> {
        every { create(DocumentType.LOGS) } returns storageClient
      }
    val logPath = "log-path"
    val logClient =
      LogClient(
        storageClientFactory = storageClientFactory,
        mapper = objectMapper,
        logEventLayout = logEventLayout,
        meterRegistry = null,
      )

    logClient.deleteLogs(logPath = logPath)

    verify(exactly = 1) { storageClient.delete(logPath) }
  }

  @Test
  fun testGetStructuredLogs() {
    val logFile = createTempFile(prefix = "log", suffix = STRUCTURED_LOG_FILE_EXTENSION)
    val logPath = logFile.pathString
    val numLines = 100

    val logEvents = buildLogEvents(numLines = (numLines * 2), startingTimestamp = 0L)

    logFile.toFile().writeText(
      text = objectMapper.writeValueAsString(logEvents),
    )

    val storageClient =
      mockk<StorageClient> {
        every { list(any()) } returns listOf(logPath)
        every { read(any()) } returns logFile.toFile().readText()
        every { storageType } returns StorageType.LOCAL
      }
    val storageClientFactory =
      mockk<StorageClientFactory> {
        every { create(DocumentType.LOGS) } returns storageClient
      }
    val logClient =
      LogClient(
        storageClientFactory = storageClientFactory,
        mapper = objectMapper,
        logEventLayout = logEventLayout,
        meterRegistry = null,
      )

    val result = logClient.getLogs(logPath = logPath, numLines = numLines)
    assertEquals(numLines, result.events.size)
    assertEquals("log line 1", result.events.first().message)
    assertEquals(1000L, result.events.first().timestamp)
    assertEquals("log line $numLines", result.events.last().message)
    assertEquals((numLines * 1000).toLong(), result.events.last().timestamp)
    verify(exactly = 1) { storageClient.list(logPath) }
  }

  @Test
  fun testTailLogFilesLocal() {
    val logFile = createTempFile(prefix = "log", suffix = ".log")
    val logPath = logFile.pathString
    val numLines = 100
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    logFile.toFile().writeText(
      text =
        (1..(numLines * 2)).joinToString(separator = "\n") {
          "${
            Instant.ofEpochMilli(it.toLong() * 1000 * 60).atZone(ZoneId.of("UTC")).format(formatter)
          } log line $it"
        },
    )

    val storageClient =
      mockk<StorageClient> {
        every { list(any()) } returns listOf(logPath)
        every { read(any()) } returns logFile.toFile().readText()
        every { storageType } returns StorageType.LOCAL
      }
    val storageClientFactory =
      mockk<StorageClientFactory> {
        every { create(DocumentType.LOGS) } returns storageClient
      }
    val logClient =
      LogClient(
        storageClientFactory = storageClientFactory,
        mapper = objectMapper,
        logEventLayout = logEventLayout,
        meterRegistry = null,
      )

    val logs = logClient.tailCloudLogs(logPath = logPath, numLines = numLines)
    assertEquals(numLines, logs.size)
    assertEquals("1970-01-01 00:01:00 log line 1", logs.first())
    assertEquals("1970-01-01 01:40:00 log line $numLines", logs.last())
    verify(exactly = 1) { storageClient.list(logPath) }
  }

  @Test
  fun testTailStructuredLogFilesLocal() {
    val logFile = createTempFile(prefix = "log", suffix = STRUCTURED_LOG_FILE_EXTENSION)
    val logPath = logFile.pathString
    val numLines = 100

    val logEvents = buildLogEvents(numLines = (numLines * 2), startingTimestamp = 0L)

    logFile.toFile().writeText(
      text = objectMapper.writeValueAsString(logEvents),
    )

    val storageClient =
      mockk<StorageClient> {
        every { list(any()) } returns listOf(logPath)
        every { read(any()) } returns logFile.toFile().readText()
        every { storageType } returns StorageType.LOCAL
      }
    val storageClientFactory =
      mockk<StorageClientFactory> {
        every { create(DocumentType.LOGS) } returns storageClient
      }
    val logClient =
      LogClient(
        storageClientFactory = storageClientFactory,
        mapper = objectMapper,
        logEventLayout = logEventLayout,
        meterRegistry = null,
      )

    val logs = logClient.tailCloudLogs(logPath = logPath, numLines = numLines)
    assertEquals(numLines, logs.size)
    assertEquals(logEventLayout.doLayout(logEvents.events.first()), logs.first())
    assertEquals(logEventLayout.doLayout(logEvents.events.take(numLines).last()), logs.last())
    verify(exactly = 1) { storageClient.list(logPath) }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType::class, names = ["AZURE", "GCS", "MINIO", "S3"])
  fun testTailLogFilesCloudStorage(storageType: StorageType) {
    val logPath = "log-path"
    val numLines = 100

    val fileContents1 =
      listOf(
        "2024-10-01 12:23:45 line1",
        "2024-10-01 12:23:46 line2",
        "2024-10-01 12:23:47 line3",
        "2024-10-01 12:23:48 line4",
      ).joinToString(separator = "\n")
    val fileContents2 =
      listOf(
        "2024-10-01 12:22:45 line5",
        "2024-10-01 12:22:46 line6",
        "2024-10-01 12:22:47 line7",
        "2024-10-01 12:22:48 line8",
      ).joinToString(separator = "\n")
    val fileContents3 =
      listOf(
        "2024-10-01 12:24:45 line9",
        "2024-10-01 12:24:46 line10",
        "2024-10-01 12:24:47 line11",
        "2024-10-01 12:24:48 line12",
      ).joinToString(separator = "\n")
    val fileContents4 =
      listOf(
        "2024-10-01 12:25:45 line13",
        "2024-10-01 12:25:46 line14",
        "2024-10-01 12:25:47 line15",
        "2024-10-01 12:25:48 line16",
      ).joinToString(separator = "\n")
    val files = listOf("file1", "file2", "file3", "file4")

    // GCS client returns the list of files in descending order
    val fileList = if (storageType == StorageType.GCS) files.reversed() else files

    val storageClient =
      mockk<StorageClient> {
        every { list(any()) } returns fileList
        every { read("file1") } returns fileContents1
        every { read("file2") } returns fileContents2
        every { read("file3") } returns fileContents3
        every { read("file4") } returns fileContents4
        every { this@mockk.storageType } returns storageType
      }
    val storageClientFactory =
      mockk<StorageClientFactory> {
        every { create(DocumentType.LOGS) } returns storageClient
      }
    val logClient =
      LogClient(
        storageClientFactory = storageClientFactory,
        mapper = objectMapper,
        logEventLayout = logEventLayout,
        meterRegistry = null,
      )
    val logs = logClient.tailCloudLogs(logPath = logPath, numLines = numLines)
    assertEquals(16, logs.size)
    assertEquals("2024-10-01 12:22:45 line5", logs.first())
    assertEquals("2024-10-01 12:25:48 line16", logs.last())
    verify(exactly = 1) { storageClient.list(logPath) }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType::class, names = ["AZURE", "GCS", "MINIO", "S3"])
  fun testTailStructuredLogFilesCloudStorage(storageType: StorageType) {
    val logPath = "log-path"
    val numLines = 100
    val startingInstant = Instant.now()
    val logEvents1 = buildLogEvents(numLines = 4, startingTimestamp = startingInstant.toEpochMilli())
    val logEvents2 = buildLogEvents(numLines = 4, startingTimestamp = startingInstant.toEpochMilli() - (300 * 1000))
    val logEvents3 = buildLogEvents(numLines = 4, startingTimestamp = startingInstant.toEpochMilli() + (300 * 1000))
    val logEvents4 = buildLogEvents(numLines = 4, startingTimestamp = startingInstant.toEpochMilli() + (600 * 1000))
    val files =
      listOf(
        "file1$STRUCTURED_LOG_FILE_EXTENSION",
        "file2$STRUCTURED_LOG_FILE_EXTENSION",
        "file3$STRUCTURED_LOG_FILE_EXTENSION",
        "file4$STRUCTURED_LOG_FILE_EXTENSION",
      )

    // GCS client returns the list of files in descending order
    val fileList = if (storageType == StorageType.GCS) files.reversed() else files

    val storageClient =
      mockk<StorageClient> {
        every { list(any()) } returns fileList
        every { read("file1$STRUCTURED_LOG_FILE_EXTENSION") } returns objectMapper.writeValueAsString(logEvents1)
        every { read("file2$STRUCTURED_LOG_FILE_EXTENSION") } returns objectMapper.writeValueAsString(logEvents2)
        every { read("file3$STRUCTURED_LOG_FILE_EXTENSION") } returns objectMapper.writeValueAsString(logEvents3)
        every { read("file4$STRUCTURED_LOG_FILE_EXTENSION") } returns objectMapper.writeValueAsString(logEvents4)
        every { this@mockk.storageType } returns storageType
      }
    val storageClientFactory =
      mockk<StorageClientFactory> {
        every { create(DocumentType.LOGS) } returns storageClient
      }
    val logClient =
      LogClient(
        storageClientFactory = storageClientFactory,
        mapper = objectMapper,
        logEventLayout = logEventLayout,
        meterRegistry = null,
      )
    val logs = logClient.tailCloudLogs(logPath = logPath, numLines = numLines)
    assertEquals(16, logs.size)
    assertEquals(logEventLayout.doLayout(logEvents2.events.first()), logs.first())
    assertEquals(logEventLayout.doLayout(logEvents4.events.last()), logs.last())
    verify(exactly = 1) { storageClient.list(logPath) }
  }

  @Test
  fun testTailLogFilesMultiline() {
    val logFile = createTempFile(prefix = "log", suffix = ".log")
    val logPath = logFile.pathString
    val numLines = 100

    logFile.toFile().writeText(
      text =
        """
        2024-10-11 13:56:38 replication-orchestrator > Destination finished successfully — exiting read dest...
        2024-10-11 13:56:38 replication-orchestrator > readFromDestination: done. (writeToDestFailed:false, dest.isFinished:true)
        2024-10-11 13:56:38 replication-orchestrator > thread status... timeout thread: false , replication thread: true
        2024-10-11 13:56:38 replication-orchestrator > Closing StateCheckSumCountEventHandler
        2024-10-11 13:56:38 replication-orchestrator > No checksum errors were reported in the entire sync.
        2024-10-11 13:56:41 replication-orchestrator > Closing StateCheckSumEventPubSubWriter
        2024-10-11 13:56:42 replication-orchestrator > sync summary: {
          "status" : "completed",
          "recordsSynced" : 1,
          "bytesSynced" : 23827,
          "startTime" : 1728654992088,
          "endTime" : 1728655002580,
          "totalStats" : {
            "bytesCommitted" : 23827,
            "bytesEmitted" : 23827,
            "destinationStateMessagesEmitted" : 1,
            "destinationWriteEndTime" : 1728654998511,
            "destinationWriteStartTime" : 1728654992407,
            "meanSecondsBeforeSourceStateMessageEmitted" : 0,
            "maxSecondsBeforeSourceStateMessageEmitted" : 1,
            "maxSecondsBetweenStateMessageEmittedandCommitted" : 4,
            "meanSecondsBetweenStateMessageEmittedandCommitted" : 4,
            "recordsEmitted" : 1,
            "recordsCommitted" : 1,
            "replicationEndTime" : 1728654998784,
            "replicationStartTime" : 1728654992088,
            "sourceReadEndTime" : 1728654994300,
            "sourceReadStartTime" : 1728654992484,
            "sourceStateMessagesEmitted" : 1
          },
          "streamStats" : [ {
            "streamName" : "pokemon",
            "stats" : {
              "bytesCommitted" : 23827,
              "bytesEmitted" : 23827,
              "recordsEmitted" : 1,
              "recordsCommitted" : 1
            }
          } ],
          "performanceMetrics" : {
            "processFromSource" : {
              "elapsedTimeInNanos" : 472775623,
              "executionCount" : 5,
              "avgExecTimeInNanos" : 9.45551246E7
            },
            "readFromSource" : {
              "elapsedTimeInNanos" : 1784395901,
              "executionCount" : 86,
              "avgExecTimeInNanos" : 2.0748789546511628E7
            },
            "processFromDest" : {
              "elapsedTimeInNanos" : 37620331,
              "executionCount" : 1,
              "avgExecTimeInNanos" : 3.7620331E7
            },
            "writeToDest" : {
              "elapsedTimeInNanos" : 402007848,
              "executionCount" : 2,
              "avgExecTimeInNanos" : 2.01003924E8
            },
            "readFromDest" : {
              "elapsedTimeInNanos" : 5976277142,
              "executionCount" : 509,
              "avgExecTimeInNanos" : 1.1741212459724952E7
            }
          }
        }
        2024-10-11 13:56:42 replication-orchestrator > failures: [ ]
        2024-10-11 13:56:42 replication-orchestrator > 
        2024-10-11 13:56:42 replication-orchestrator > ----- END REPLICATION -----
        2024-10-11 13:56:42 replication-orchestrator > 
        2024-10-11 13:56:43 replication-orchestrator > Returning output...
        """.trimIndent(),
    )

    val storageClient =
      mockk<StorageClient> {
        every { list(any()) } returns listOf(logPath)
        every { read(any()) } returns logFile.toFile().readText()
        every { storageType } returns StorageType.LOCAL
      }
    val storageClientFactory =
      mockk<StorageClientFactory> {
        every { create(DocumentType.LOGS) } returns storageClient
      }
    val logClient =
      LogClient(
        storageClientFactory = storageClientFactory,
        mapper = objectMapper,
        logEventLayout = logEventLayout,
        meterRegistry = null,
      )

    val logs = logClient.tailCloudLogs(logPath = logPath, numLines = numLines)
    assertEquals(12, logs.size)
    assertEquals("2024-10-11 13:56:38 replication-orchestrator > Destination finished successfully — exiting read dest...", logs.first())
    assertEquals("2024-10-11 13:56:43 replication-orchestrator > Returning output...", logs.last())
    verify(exactly = 1) { storageClient.list(logPath) }
  }

  @Test
  fun testTailStructuredLogFilesMultiline() {
    val logFile = createTempFile(prefix = "log", suffix = STRUCTURED_LOG_FILE_EXTENSION)
    val logPath = logFile.pathString
    val numLines = 100
    val logLevel = Level.INFO.toString()
    val logSource = LogSource.REPLICATION_ORCHESTRATOR

    val logEvents =
      LogEvents(
        events =
          listOf(
            LogEvent(
              timestamp =
                LocalDateTime
                  .parse("2024-10-11 13:56:38", EVENT_TIMESTAMP_FORMATTER)
                  .atZone(UTC_ZONE_ID)
                  .toInstant()
                  .toEpochMilli(),
              message = "Destination finished successfully — exiting read dest...",
              logSource = logSource,
              level = logLevel,
            ),
            LogEvent(
              timestamp =
                LocalDateTime
                  .parse("2024-10-11 13:56:38", EVENT_TIMESTAMP_FORMATTER)
                  .atZone(UTC_ZONE_ID)
                  .toInstant()
                  .toEpochMilli(),
              message = "readFromDestination: done. (writeToDestFailed:false, dest.isFinished:true)",
              logSource = logSource,
              level = logLevel,
            ),
            LogEvent(
              timestamp =
                LocalDateTime
                  .parse("2024-10-11 13:56:38", EVENT_TIMESTAMP_FORMATTER)
                  .atZone(UTC_ZONE_ID)
                  .toInstant()
                  .toEpochMilli(),
              message = "thread status... timeout thread: false , replication thread: true)",
              logSource = logSource,
              level = logLevel,
            ),
            LogEvent(
              timestamp =
                LocalDateTime
                  .parse("2024-10-11 13:56:41", EVENT_TIMESTAMP_FORMATTER)
                  .atZone(UTC_ZONE_ID)
                  .toInstant()
                  .toEpochMilli(),
              message = "Closing StateCheckSumEventPubSubWriter",
              logSource = logSource,
              level = logLevel,
            ),
            LogEvent(
              timestamp =
                LocalDateTime
                  .parse("2024-10-11 13:56:42", EVENT_TIMESTAMP_FORMATTER)
                  .atZone(UTC_ZONE_ID)
                  .toInstant()
                  .toEpochMilli(),
              message = """sync summary: {
  "status" : "completed",
  "recordsSynced" : 1,
  "bytesSynced" : 23827,
  "startTime" : 1728654992088,
  "endTime" : 1728655002580,
  "totalStats" : {
    "bytesCommitted" : 23827,
    "bytesEmitted" : 23827,
    "destinationStateMessagesEmitted" : 1,
    "destinationWriteEndTime" : 1728654998511,
    "destinationWriteStartTime" : 1728654992407,
    "meanSecondsBeforeSourceStateMessageEmitted" : 0,
    "maxSecondsBeforeSourceStateMessageEmitted" : 1,
    "maxSecondsBetweenStateMessageEmittedandCommitted" : 4,
    "meanSecondsBetweenStateMessageEmittedandCommitted" : 4,
    "recordsEmitted" : 1,
    "recordsCommitted" : 1,
    "replicationEndTime" : 1728654998784,
    "replicationStartTime" : 1728654992088,
    "sourceReadEndTime" : 1728654994300,
    "sourceReadStartTime" : 1728654992484,
    "sourceStateMessagesEmitted" : 1
  },
  "streamStats" : [ {
    "streamName" : "pokemon",
    "stats" : {
      "bytesCommitted" : 23827,
      "bytesEmitted" : 23827,
      "recordsEmitted" : 1,
      "recordsCommitted" : 1
    }
  } ],
  "performanceMetrics" : {
    "processFromSource" : {
      "elapsedTimeInNanos" : 472775623,
      "executionCount" : 5,
      "avgExecTimeInNanos" : 9.45551246E7
    },
    "readFromSource" : {
      "elapsedTimeInNanos" : 1784395901,
      "executionCount" : 86,
      "avgExecTimeInNanos" : 2.0748789546511628E7
    },
    "processFromDest" : {
      "elapsedTimeInNanos" : 37620331,
      "executionCount" : 1,
      "avgExecTimeInNanos" : 3.7620331E7
    },
    "writeToDest" : {
      "elapsedTimeInNanos" : 402007848,
      "executionCount" : 2,
      "avgExecTimeInNanos" : 2.01003924E8
    },
    "readFromDest" : {
      "elapsedTimeInNanos" : 5976277142,
      "executionCount" : 509,
      "avgExecTimeInNanos" : 1.1741212459724952E7
    }
  }
}""",
              logSource = logSource,
              level = logLevel,
            ),
            LogEvent(
              timestamp =
                LocalDateTime
                  .parse("2024-10-11 13:56:42", EVENT_TIMESTAMP_FORMATTER)
                  .atZone(UTC_ZONE_ID)
                  .toInstant()
                  .toEpochMilli(),
              message = "failures: [ ]",
              logSource = logSource,
              level = logLevel,
            ),
            LogEvent(
              timestamp =
                LocalDateTime
                  .parse("2024-10-11 13:56:42", EVENT_TIMESTAMP_FORMATTER)
                  .atZone(UTC_ZONE_ID)
                  .toInstant()
                  .toEpochMilli(),
              message = "",
              logSource = logSource,
              level = logLevel,
            ),
            LogEvent(
              timestamp =
                LocalDateTime
                  .parse("2024-10-11 13:56:42", EVENT_TIMESTAMP_FORMATTER)
                  .atZone(UTC_ZONE_ID)
                  .toInstant()
                  .toEpochMilli(),
              message = "----- END REPLICATION -----",
              logSource = logSource,
              level = logLevel,
            ),
            LogEvent(
              timestamp =
                LocalDateTime
                  .parse("2024-10-11 13:56:42", EVENT_TIMESTAMP_FORMATTER)
                  .atZone(UTC_ZONE_ID)
                  .toInstant()
                  .toEpochMilli(),
              message = "",
              logSource = logSource,
              level = logLevel,
            ),
            LogEvent(
              timestamp =
                LocalDateTime
                  .parse("2024-10-11 13:56:43", EVENT_TIMESTAMP_FORMATTER)
                  .atZone(UTC_ZONE_ID)
                  .toInstant()
                  .toEpochMilli(),
              message = "Returning output...",
              logSource = logSource,
              level = logLevel,
            ),
          ),
      )

    logFile.toFile().writeText(text = objectMapper.writeValueAsString(logEvents))

    val storageClient =
      mockk<StorageClient> {
        every { list(any()) } returns listOf(logPath)
        every { read(any()) } returns logFile.toFile().readText()
        every { storageType } returns StorageType.LOCAL
      }
    val storageClientFactory =
      mockk<StorageClientFactory> {
        every { create(DocumentType.LOGS) } returns storageClient
      }
    val logClient =
      LogClient(
        storageClientFactory = storageClientFactory,
        mapper = objectMapper,
        logEventLayout = logEventLayout,
        meterRegistry = null,
      )

    val logs = logClient.tailCloudLogs(logPath = logPath, numLines = numLines)
    assertEquals(logEvents.events.size, logs.size)
    assertEquals(logEventLayout.doLayout(logEvent = logEvents.events.first()), logs.first())
    assertEquals(logEventLayout.doLayout(logEvent = logEvents.events.last()), logs.last())
    verify(exactly = 1) { storageClient.list(logPath) }
  }

  private fun buildLogEvents(
    numLines: Int,
    startingTimestamp: Long,
  ): LogEvents {
    val events =
      (1..numLines).map {
        LogEvent(
          timestamp = startingTimestamp + (it * 1000).toLong(),
          message = "log line $it",
          logSource = LogSource.PLATFORM,
          level = Level.INFO.toString(),
        )
      }

    return LogEvents(events = events)
  }
}
