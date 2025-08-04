/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import io.airbyte.api.model.generated.AttemptFailureSummary
import io.airbyte.api.model.generated.AttemptInfoRead
import io.airbyte.api.model.generated.AttemptInfoReadLogs
import io.airbyte.api.model.generated.AttemptRead
import io.airbyte.api.model.generated.AttemptStats
import io.airbyte.api.model.generated.AttemptStreamStats
import io.airbyte.api.model.generated.DestinationDefinitionRead
import io.airbyte.api.model.generated.FailureOrigin
import io.airbyte.api.model.generated.FailureType
import io.airbyte.api.model.generated.JobConfigType
import io.airbyte.api.model.generated.JobDebugRead
import io.airbyte.api.model.generated.JobInfoLightRead
import io.airbyte.api.model.generated.JobInfoRead
import io.airbyte.api.model.generated.JobRead
import io.airbyte.api.model.generated.JobRefreshConfig
import io.airbyte.api.model.generated.JobWithAttemptsRead
import io.airbyte.api.model.generated.LogFormatType
import io.airbyte.api.model.generated.ResetConfig
import io.airbyte.api.model.generated.SourceDefinitionRead
import io.airbyte.api.model.generated.SynchronousJobRead
import io.airbyte.commons.enums.isCompatible
import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.commons.logging.LogEvent
import io.airbyte.commons.logging.LogEvents
import io.airbyte.commons.logging.LogSource
import io.airbyte.commons.logging.LogUtils
import io.airbyte.commons.server.converters.JobConverter.Companion.extractRefreshConfigIfNeeded
import io.airbyte.commons.server.converters.JobConverter.Companion.getDebugJobInfoRead
import io.airbyte.commons.server.scheduler.SynchronousJobMetadata
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.config.AirbyteStream
import io.airbyte.config.Attempt
import io.airbyte.config.AttemptStatus
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.FailureReason
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobOutput
import io.airbyte.config.JobResetConnectionConfig
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.RefreshConfig
import io.airbyte.config.RefreshStream
import io.airbyte.config.ResetSourceConfiguration
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.StreamSyncStats
import io.airbyte.config.SyncMode
import io.airbyte.config.SyncStats
import jakarta.validation.Valid
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import java.nio.file.Path
import java.util.List
import java.util.Optional
import java.util.UUID
import java.util.stream.Collectors
import java.util.stream.Stream

internal class JobConverterTest {
  private lateinit var jobConverter: JobConverter
  private lateinit var logClientManager: LogClientManager
  private lateinit var logUtils: LogUtils

  private val jobId = 100L
  private val attemptNumber = 0
  private val jobConfigId = "123"
  private val jobStatus = JobStatus.RUNNING
  private val attemptStatus = AttemptStatus.RUNNING
  private val configType = ConfigType.SYNC
  private val recordsEmitted = 15L
  private val bytesEmitted = 100L
  private val recordsCommitted = 10L
  private val stateMessagesEmitted = 2L
  private val streamName = "stream1"
  private val partialSuccess = false

  private val jobConfig: JobConfig =
    JobConfig()
      .withConfigType(configType)
      .withSync(
        JobSyncConfig().withConfiguredAirbyteCatalog(
          ConfiguredAirbyteCatalog().withStreams(
            listOf(
              ConfiguredAirbyteStream(
                AirbyteStream(USERS, emptyObject(), listOf(SyncMode.INCREMENTAL)),
                SyncMode.INCREMENTAL,
                DestinationSyncMode.APPEND,
              ),
              ConfiguredAirbyteStream(
                AirbyteStream(ACCOUNTS, emptyObject(), listOf(SyncMode.INCREMENTAL)),
                SyncMode.INCREMENTAL,
                DestinationSyncMode.APPEND,
              ),
            ),
          ),
        ),
      )

  private val jobOutput: JobOutput? =
    JobOutput()
      .withOutputType(JobOutput.OutputType.SYNC)
      .withSync(
        StandardSyncOutput()
          .withStandardSyncSummary(
            StandardSyncSummary()
              .withRecordsSynced(recordsEmitted)
              .withBytesSynced(bytesEmitted)
              .withTotalStats(
                SyncStats()
                  .withRecordsEmitted(recordsEmitted)
                  .withBytesEmitted(bytesEmitted)
                  .withSourceStateMessagesEmitted(stateMessagesEmitted)
                  .withRecordsCommitted(recordsCommitted),
              ).withStreamStats(
                listOf(
                  StreamSyncStats()
                    .withStreamName(streamName)
                    .withStats(
                      SyncStats()
                        .withRecordsEmitted(recordsEmitted)
                        .withBytesEmitted(bytesEmitted)
                        .withSourceStateMessagesEmitted(stateMessagesEmitted)
                        .withRecordsCommitted(recordsCommitted),
                    ),
                ),
              ),
          ),
      )

  private val jobInfoUnstructuredLogs: JobInfoRead =
    JobInfoRead()
      .job(
        JobRead()
          .id(jobId)
          .configId(jobConfigId)
          .status(io.airbyte.api.model.generated.JobStatus.RUNNING)
          .configType(JobConfigType.SYNC)
          .enabledStreams(
            listOf(
              io.airbyte.api.model.generated.StreamDescriptor().name(
                USERS,
              ),
              io.airbyte.api.model.generated
                .StreamDescriptor()
                .name(ACCOUNTS),
            ),
          ).createdAt(CREATED_AT)
          .updatedAt(CREATED_AT)
          .startedAt(CREATED_AT),
      ).attempts(
        List.of<@Valid AttemptInfoRead?>(
          AttemptInfoRead()
            .attempt(
              AttemptRead()
                .id(attemptNumber.toLong())
                .status(io.airbyte.api.model.generated.AttemptStatus.RUNNING)
                .recordsSynced(recordsEmitted)
                .bytesSynced(bytesEmitted)
                .totalStats(
                  AttemptStats()
                    .recordsEmitted(recordsEmitted)
                    .bytesEmitted(bytesEmitted)
                    .stateMessagesEmitted(stateMessagesEmitted)
                    .recordsCommitted(recordsCommitted),
                ).streamStats(
                  List.of<@Valid AttemptStreamStats?>(
                    AttemptStreamStats()
                      .streamName(streamName)
                      .stats(
                        AttemptStats()
                          .recordsEmitted(recordsEmitted)
                          .bytesEmitted(bytesEmitted)
                          .stateMessagesEmitted(stateMessagesEmitted)
                          .recordsCommitted(recordsCommitted),
                      ),
                  ),
                ).updatedAt(CREATED_AT)
                .createdAt(CREATED_AT)
                .endedAt(CREATED_AT)
                .failureSummary(
                  AttemptFailureSummary()
                    .failures(
                      List.of<@Valid io.airbyte.api.model.generated.FailureReason?>(
                        io.airbyte.api.model.generated
                          .FailureReason()
                          .failureOrigin(FailureOrigin.SOURCE)
                          .failureType(FailureType.SYSTEM_ERROR)
                          .externalMessage(FAILURE_EXTERNAL_MESSAGE)
                          .stacktrace(FAILURE_STACKTRACE)
                          .timestamp(FAILURE_TIMESTAMP),
                      ),
                    ).partialSuccess(partialSuccess),
                ),
            ).logType(LogFormatType.FORMATTED)
            .logs(AttemptInfoReadLogs().logLines(mutableListOf<String?>())),
        ),
      )

  private val version = "0.33.4"
  private val airbyteVersion = AirbyteVersion(version)
  private val sourceDefinitionRead: SourceDefinitionRead? = SourceDefinitionRead().sourceDefinitionId(UUID.randomUUID())
  private val destinationDefinitionRead: DestinationDefinitionRead? = DestinationDefinitionRead().destinationDefinitionId(UUID.randomUUID())

  private val jobDebugInfo: JobDebugRead? =
    JobDebugRead()
      .id(jobId)
      .configId(jobConfigId)
      .status(io.airbyte.api.model.generated.JobStatus.RUNNING)
      .configType(JobConfigType.SYNC)
      .airbyteVersion(airbyteVersion.serialize())
      .sourceDefinition(sourceDefinitionRead)
      .destinationDefinition(destinationDefinitionRead)

  private val jobWithAttemptsReadUnstructuredLogs: JobWithAttemptsRead? =
    JobWithAttemptsRead()
      .job(jobInfoUnstructuredLogs.getJob())
      .attempts(
        jobInfoUnstructuredLogs.getAttempts().stream().map<AttemptRead?> { obj: AttemptInfoRead? -> obj!!.getAttempt() }.collect(
          Collectors.toList(),
        ),
      )

  private val failureSummary: io.airbyte.config.AttemptFailureSummary? =
    io.airbyte.config
      .AttemptFailureSummary()
      .withFailures(List.of<FailureReason?>(FAILURE_REASON))
      .withPartialSuccess(partialSuccess)

  @Test
  fun testEnumCompatibility() {
    Assertions.assertTrue(isCompatible<ConfigType, JobConfigType>())
    Assertions.assertTrue(isCompatible<JobStatus, io.airbyte.api.model.generated.JobStatus>())
    Assertions.assertTrue(isCompatible<AttemptStatus, io.airbyte.api.model.generated.AttemptStatus>())
    Assertions.assertTrue(isCompatible<FailureReason.FailureOrigin, FailureOrigin>())
    Assertions.assertTrue(isCompatible<FailureReason.FailureType, FailureType>())
  }

  @ParameterizedTest
  @MethodSource("getExtractRefreshScenarios")
  fun testExtractRefresh(
    job: Job,
    expectedConfig: Optional<JobRefreshConfig?>?,
  ) {
    val actualConfig: Optional<JobRefreshConfig> = extractRefreshConfigIfNeeded(job)

    Assertions.assertEquals(expectedConfig, actualConfig)
  }

  @Nested
  internal inner class TestJob {
    private var job: Job? = null

    @BeforeEach
    fun setUp() {
      logClientManager = Mockito.mock<LogClientManager>(LogClientManager::class.java)
      logUtils = Mockito.mock<LogUtils>(LogUtils::class.java)
      jobConverter = JobConverter(logClientManager, logUtils)
      val attempt =
        Attempt(
          attemptNumber,
          jobId,
          LOG_PATH,
          null,
          jobOutput,
          attemptStatus,
          null,
          failureSummary,
          CREATED_AT,
          CREATED_AT,
          CREATED_AT,
        )
      job =
        Job(
          jobId,
          jobConfig.getConfigType(),
          jobConfigId,
          jobConfig,
          listOf(attempt),
          jobStatus,
          CREATED_AT,
          CREATED_AT,
          CREATED_AT,
          true,
        )
    }

    @Test
    fun testGetJobInfoRead() {
      Mockito
        .`when`(logClientManager.getLogs(ArgumentMatchers.any<Path?>()))
        .thenReturn(LogEvents(mutableListOf<LogEvent>(), "1"))
      Assertions.assertEquals(jobInfoUnstructuredLogs, jobConverter.getJobInfoRead(job!!))
    }

    @Test
    fun testGetJobInfoLightRead() {
      val expected = JobInfoLightRead().job(jobInfoUnstructuredLogs.getJob())
      Assertions.assertEquals(expected, jobConverter.getJobInfoLightRead(job!!))
    }

    @Test
    fun testGetDebugJobInfoRead() {
      Assertions.assertEquals(
        jobDebugInfo,
        getDebugJobInfoRead(jobInfoUnstructuredLogs, sourceDefinitionRead, destinationDefinitionRead, airbyteVersion),
      )
    }

    @Test
    fun testGetJobWithAttemptsRead() {
      Assertions.assertEquals(jobWithAttemptsReadUnstructuredLogs, JobConverter.getJobWithAttemptsRead(job!!))
    }

    @Test
    fun testGetJobWithAttemptsReadStructuredLogs() {
      val logEventVersion = "1"
      Mockito.`when`(logClientManager.getLogs(ArgumentMatchers.any<Path?>())).thenReturn(
        LogEvents(listOf(LogEvent(System.currentTimeMillis(), "message", "INFO", LogSource.PLATFORM, null, null)), logEventVersion),
      )
      val jobInfoRead = jobConverter.getJobInfoRead(job!!)
      Assertions.assertEquals(LogFormatType.STRUCTURED, jobInfoRead.getAttempts().first().getLogType())
      Assertions.assertEquals(
        logEventVersion,
        jobInfoRead
          .getAttempts()
          .first()
          .getLogs()
          .getVersion(),
      )
      Assertions.assertEquals(
        1,
        jobInfoRead
          .getAttempts()
          .first()
          .getLogs()
          .getEvents()
          .size,
      )
    }

    // this test intentionally only looks at the reset config as the rest is the same here.
    @Test
    fun testResetJobIncludesResetConfig() {
      val resetConfig =
        JobConfig()
          .withConfigType(ConfigType.RESET_CONNECTION)
          .withResetConnection(
            JobResetConnectionConfig().withResetSourceConfiguration(
              ResetSourceConfiguration().withStreamsToReset(
                listOf(
                  StreamDescriptor().withName(USERS),
                  StreamDescriptor().withName(ACCOUNTS),
                ),
              ),
            ),
          )
      val resetJob =
        Job(
          jobId,
          ConfigType.RESET_CONNECTION,
          jobConfigId,
          resetConfig,
          mutableListOf<Attempt>(),
          JobStatus.SUCCEEDED,
          CREATED_AT,
          CREATED_AT,
          CREATED_AT,
          true,
        )

      val expectedResetConfig =
        ResetConfig().streamsToReset(
          listOf(
            io.airbyte.api.model.generated
              .StreamDescriptor()
              .name(USERS),
            io.airbyte.api.model.generated
              .StreamDescriptor()
              .name(ACCOUNTS),
          ),
        )
      Assertions.assertEquals(expectedResetConfig, jobConverter.getJobInfoRead(resetJob).getJob().getResetConfig())
    }

    @Test
    fun testResetJobExcludesConfigIfNull() {
      val resetConfig =
        JobConfig()
          .withConfigType(ConfigType.RESET_CONNECTION)
          .withResetConnection(JobResetConnectionConfig().withResetSourceConfiguration(null))
      val resetJob =
        Job(
          jobId,
          ConfigType.RESET_CONNECTION,
          jobConfigId,
          resetConfig,
          mutableListOf<Attempt>(),
          JobStatus.SUCCEEDED,
          CREATED_AT,
          CREATED_AT,
          CREATED_AT,
          true,
        )

      Assertions.assertNull(jobConverter.getJobInfoRead(resetJob).getJob().getResetConfig())
    }
  }

  @Nested
  internal inner class TestSynchronousJob {
    private var metadata: SynchronousJobMetadata? = null
    private val jobId: UUID = UUID.randomUUID()
    private val configType = ConfigType.DISCOVER_SCHEMA
    private val configId: Optional<UUID> = Optional.empty<UUID>()
    private val jobSucceeded = false
    private val connectorConfigUpdated = false
    private val synchronousJobInfoUnstructuredLogs: SynchronousJobRead? =
      SynchronousJobRead()
        .id(jobId)
        .configType(JobConfigType.DISCOVER_SCHEMA)
        .configId(configId.toString())
        .createdAt(CREATED_AT)
        .endedAt(CREATED_AT)
        .succeeded(jobSucceeded)
        .connectorConfigurationUpdated(connectorConfigUpdated)
        .logType(LogFormatType.FORMATTED)
        .logs(AttemptInfoReadLogs().logLines(ArrayList<String?>()))
        .failureReason(
          io.airbyte.api.model.generated
            .FailureReason()
            .failureOrigin(FailureOrigin.SOURCE)
            .failureType(FailureType.SYSTEM_ERROR)
            .externalMessage(FAILURE_EXTERNAL_MESSAGE)
            .stacktrace(FAILURE_STACKTRACE)
            .timestamp(FAILURE_TIMESTAMP),
        )

    @BeforeEach
    fun setUp() {
      logClientManager = Mockito.mock<LogClientManager>(LogClientManager::class.java)
      jobConverter = JobConverter(logClientManager, Mockito.mock<LogUtils?>())
      metadata =
        SynchronousJobMetadata(
          jobId,
          configType,
          null,
          CREATED_AT,
          CREATED_AT,
          jobSucceeded,
          connectorConfigUpdated,
          LOG_PATH,
          FAILURE_REASON,
        )
    }

    @Test
    fun testSynchronousJobRead() {
      Mockito
        .`when`(logClientManager.getLogs(ArgumentMatchers.any<Path?>()))
        .thenReturn(LogEvents(mutableListOf<LogEvent>(), "1"))
      Assertions.assertEquals(synchronousJobInfoUnstructuredLogs, jobConverter.getSynchronousJobRead(metadata!!))
    }
  }

  companion object {
    private val CREATED_AT = System.currentTimeMillis() / 1000
    private val LOG_PATH: Path = Path.of("log_path")
    private const val FAILURE_EXTERNAL_MESSAGE = "something went wrong"
    private val FAILURE_TIMESTAMP = System.currentTimeMillis()
    private const val FAILURE_STACKTRACE = "stacktrace"
    private const val USERS = "users"
    private const val ACCOUNTS = "accounts"
    private val FAILURE_REASON: FailureReason =
      FailureReason()
        .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
        .withExternalMessage(FAILURE_EXTERNAL_MESSAGE)
        .withStacktrace(FAILURE_STACKTRACE)
        .withTimestamp(FAILURE_TIMESTAMP)

    @JvmStatic
    private fun getExtractRefreshScenarios(): Stream<Arguments> =
      Stream.of<Arguments>(
        Arguments.of(
          Job(
            1,
            ConfigType.SYNC,
            "",
            JobConfig(),
            mutableListOf<Attempt>(),
            JobStatus.SUCCEEDED,
            1L,
            13,
            37,
            true,
          ),
          Optional.empty<Any?>(),
        ),
        Arguments.of(
          Job(
            1,
            ConfigType.RESET_CONNECTION,
            "",
            JobConfig(),
            mutableListOf<Attempt>(),
            JobStatus.SUCCEEDED,
            1L,
            13,
            37,
            true,
          ),
          Optional.empty<Any?>(),
        ),
        Arguments.of(
          Job(
            1,
            ConfigType.REFRESH,
            "",
            JobConfig()
              .withRefresh(
                RefreshConfig().withStreamsToRefresh(
                  List.of<RefreshStream?>(
                    RefreshStream().withStreamDescriptor(
                      StreamDescriptor().withName("test"),
                    ),
                  ),
                ),
              ),
            mutableListOf<Attempt>(),
            JobStatus.SUCCEEDED,
            1L,
            13,
            37,
            true,
          ),
          Optional.of<JobRefreshConfig?>(
            JobRefreshConfig().streamsToRefresh(
              listOf(
                io.airbyte.api.model.generated
                  .StreamDescriptor()
                  .name("test"),
              ),
            ),
          ),
        ),
        Arguments.of(
          Job(
            1,
            ConfigType.REFRESH,
            "",
            JobConfig()
              .withRefresh(
                RefreshConfig().withStreamsToRefresh(
                  List.of<RefreshStream?>(RefreshStream().withStreamDescriptor(null)),
                ),
              ),
            mutableListOf<Attempt>(),
            JobStatus.SUCCEEDED,
            1L,
            13,
            37,
            true,
          ),
          Optional.empty<Any?>(),
        ),
      )
  }
}
