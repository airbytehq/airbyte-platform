/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import io.airbyte.api.model.generated.AttemptFailureSummary
import io.airbyte.api.model.generated.AttemptInfoRead
import io.airbyte.api.model.generated.AttemptInfoReadLogs
import io.airbyte.api.model.generated.AttemptRead
import io.airbyte.api.model.generated.AttemptStats
import io.airbyte.api.model.generated.AttemptStatus
import io.airbyte.api.model.generated.AttemptStreamStats
import io.airbyte.api.model.generated.DestinationDefinitionRead
import io.airbyte.api.model.generated.FailureOrigin
import io.airbyte.api.model.generated.FailureType
import io.airbyte.api.model.generated.JobConfigType
import io.airbyte.api.model.generated.JobDebugRead
import io.airbyte.api.model.generated.JobInfoLightRead
import io.airbyte.api.model.generated.JobInfoRead
import io.airbyte.api.model.generated.JobOptionalRead
import io.airbyte.api.model.generated.JobRead
import io.airbyte.api.model.generated.JobRefreshConfig
import io.airbyte.api.model.generated.JobStatus
import io.airbyte.api.model.generated.JobWithAttemptsRead
import io.airbyte.api.model.generated.LogFormatType
import io.airbyte.api.model.generated.LogLevel
import io.airbyte.api.model.generated.LogRead
import io.airbyte.api.model.generated.LogSource
import io.airbyte.api.model.generated.ResetConfig
import io.airbyte.api.model.generated.SourceDefinitionRead
import io.airbyte.api.model.generated.SynchronousJobRead
import io.airbyte.commons.converters.ApiConverters.Companion.toApi
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.logging.LogCaller
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.commons.logging.LogEvent
import io.airbyte.commons.logging.LogUtils
import io.airbyte.commons.server.scheduler.SynchronousJobMetadata
import io.airbyte.commons.server.scheduler.SynchronousResponse
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.config.Attempt
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.FailureReason
import io.airbyte.config.Job
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobConfigProxy
import io.airbyte.config.JobOutput
import io.airbyte.config.RefreshStream
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.StreamSyncStats
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.micronaut.core.util.CollectionUtils
import jakarta.annotation.Nullable
import jakarta.inject.Singleton
import jakarta.validation.Valid
import java.io.IOException
import java.nio.file.Path
import java.util.Optional
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors
import java.util.stream.Stream

/**
 * Convert between API and internal versions of job models.
 */
@Singleton
class JobConverter(
  private val logClientManager: LogClientManager,
  private val logUtils: LogUtils,
) {
  fun getJobInfoRead(job: Job): JobInfoRead =
    JobInfoRead()
      .job(getJobWithAttemptsRead(job).job)
      .attempts(
        job.attempts
          .stream()
          .map { attempt: Attempt -> this.getAttemptInfoRead(attempt) }
          .collect(Collectors.toList<@Valid AttemptInfoRead?>()),
      )

  fun getJobInfoLightRead(job: Job): JobInfoLightRead = JobInfoLightRead().job(getJobRead(job))

  fun getJobOptionalRead(job: Optional<Job>): JobOptionalRead {
    if (job.isEmpty) {
      return JobOptionalRead()
    }
    return JobOptionalRead().job(getJobRead(job.get()))
  }

  fun getAttemptInfoRead(attempt: Attempt): AttemptInfoRead {
    val attemptInfoReadLogs = getAttemptLogs(attempt.logPath, attempt.jobId)
    return AttemptInfoRead()
      .attempt(getAttemptRead(attempt))
      .logType(if (CollectionUtils.isNotEmpty(attemptInfoReadLogs.events)) LogFormatType.STRUCTURED else LogFormatType.FORMATTED)
      .logs(attemptInfoReadLogs)
  }

  fun getLogRead(logPath: Path?): LogRead {
    try {
      return LogRead().logLines(logClientManager.getJobLogFile(logPath))
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
  }

  fun getAttemptLogs(
    logPath: Path?,
    jobId: Long?,
  ): AttemptInfoReadLogs {
    val logEvents = logClientManager.getLogs(logPath)
    return if (CollectionUtils.isNotEmpty(logEvents.events)) {
      AttemptInfoReadLogs()
        .events(toModelLogEvents(logEvents.events, logUtils))
        .version(logEvents.version)
    } else {
      AttemptInfoReadLogs().logLines(getLogRead(logPath).logLines)
    }
  }

  fun getSynchronousJobRead(response: SynchronousResponse<*>): SynchronousJobRead = getSynchronousJobRead(response.metadata)

  fun getSynchronousJobRead(metadata: SynchronousJobMetadata): SynchronousJobRead {
    val configType =
      metadata.configType.convertTo<JobConfigType>()
    val attemptInfoReadLogs = getAttemptLogs(metadata.logPath, null)

    return SynchronousJobRead()
      .id(metadata.id)
      .configType(configType)
      .configId(metadata.getConfigId().toString())
      .createdAt(metadata.createdAt)
      .endedAt(metadata.endedAt)
      .succeeded(metadata.isSucceeded)
      .connectorConfigurationUpdated(metadata.isConnectorConfigurationUpdated)
      .logType(if (CollectionUtils.isNotEmpty(attemptInfoReadLogs.events)) LogFormatType.STRUCTURED else LogFormatType.FORMATTED)
      .logs(attemptInfoReadLogs)
      .failureReason(getFailureReason(metadata.failureReason, TimeUnit.SECONDS.toMillis(metadata.endedAt)))
  }

  companion object {
    @JvmStatic
    fun getDebugJobInfoRead(
      jobInfoRead: JobInfoRead,
      sourceDefinitionRead: SourceDefinitionRead?,
      destinationDefinitionRead: DestinationDefinitionRead?,
      airbyteVersion: AirbyteVersion,
    ): JobDebugRead =
      JobDebugRead()
        .id(jobInfoRead.job.id)
        .configId(jobInfoRead.job.configId)
        .configType(jobInfoRead.job.configType)
        .status(jobInfoRead.job.status)
        .airbyteVersion(airbyteVersion.serialize())
        .sourceDefinition(sourceDefinitionRead)
        .destinationDefinition(destinationDefinitionRead)

    @JvmStatic
    fun getJobWithAttemptsRead(job: Job): JobWithAttemptsRead =
      JobWithAttemptsRead()
        .job(getJobRead(job))
        .attempts(
          job.attempts
            .stream()
            .sorted(Comparator.comparingInt { obj: Attempt -> obj.getAttemptNumber() })
            .map { attempt: Attempt -> getAttemptRead(attempt) }
            .toList(),
        )

    @JvmStatic
    fun getJobRead(job: Job): JobRead {
      val configId = job.scope
      val configType = job.configType.convertTo<JobConfigType>()

      return JobRead()
        .id(job.id)
        .configId(configId)
        .configType(configType)
        .enabledStreams(extractEnabledStreams(job))
        .resetConfig(extractResetConfigIfReset(job).orElse(null))
        .refreshConfig(extractRefreshConfigIfNeeded(job).orElse(null))
        .createdAt(job.createdAtInSecond)
        .updatedAt(job.updatedAtInSecond)
        .startedAt(job.startedAtInSecond)
        .status(job.status.convertTo<JobStatus>())
    }

    /**
     * If the job type is REFRESH or CLEAR/RESET, extracts the streams from the job config. Otherwise,
     * returns null.
     *
     * @param job - job
     * @return List of the streams associated with the job
     */
    @JvmStatic
    fun getStreamsAssociatedWithJob(job: Job): List<StreamDescriptor>? {
      val jobRead = getJobRead(job)
      return when (job.configType) {
        ConfigType.REFRESH -> {
          jobRead.refreshConfig.streamsToRefresh
            .stream()
            .map { streamDescriptor: io.airbyte.api.model.generated.StreamDescriptor ->
              StreamDescriptor()
                .withName(streamDescriptor.name)
                .withNamespace(streamDescriptor.namespace)
            }.collect(Collectors.toList())
        }

        ConfigType.CLEAR, ConfigType.RESET_CONNECTION -> {
          jobRead.resetConfig.streamsToReset
            .stream()
            .map { streamDescriptor: io.airbyte.api.model.generated.StreamDescriptor ->
              StreamDescriptor()
                .withName(streamDescriptor.name)
                .withNamespace(streamDescriptor.namespace)
            }.collect(Collectors.toList())
        }

        else -> {
          null
        }
      }
    }

    /**
     * If the job is of type RESET, extracts the part of the reset config that we expose in the API.
     * Otherwise, returns empty optional.
     *
     * @param job - job
     * @return api representation of reset config
     */
    private fun extractResetConfigIfReset(job: Job): Optional<ResetConfig> {
      if (job.configType == ConfigType.RESET_CONNECTION) {
        if (job.config.resetConnection.resetSourceConfiguration == null) {
          return Optional.empty()
        }
        return Optional.ofNullable(
          ResetConfig().streamsToReset(
            job.config.resetConnection.resetSourceConfiguration.streamsToReset
              .stream()
              .map { obj: io.airbyte.config.StreamDescriptor -> obj.toApi() }
              .toList(),
          ),
        )
      } else {
        return Optional.empty()
      }
    }

    /**
     * If the job is of type RESET, extracts the part of the reset config that we expose in the API.
     * Otherwise, returns empty optional.
     *
     * @param job - job
     * @return api representation of refresh config
     */
    @JvmStatic
    fun extractRefreshConfigIfNeeded(job: Job): Optional<JobRefreshConfig> {
      if (job.configType == ConfigType.REFRESH) {
        val refreshedStreams =
          job.config.refresh.streamsToRefresh
            .stream()
            .flatMap { refreshStream: RefreshStream -> Stream.ofNullable(refreshStream.streamDescriptor) }
            .map { obj: io.airbyte.config.StreamDescriptor -> obj.toApi() }
            .toList()
        if (refreshedStreams.isEmpty()) {
          return Optional.empty()
        }
        return Optional.ofNullable(JobRefreshConfig().streamsToRefresh(refreshedStreams))
      } else {
        return Optional.empty()
      }
    }

    @JvmStatic
    fun getAttemptInfoWithoutLogsRead(attempt: Attempt): AttemptInfoRead =
      AttemptInfoRead()
        .attempt(getAttemptRead(attempt))

    fun getAttemptRead(attempt: Attempt): AttemptRead =
      AttemptRead()
        .id(attempt.getAttemptNumber().toLong())
        .status(attempt.status?.convertTo<AttemptStatus>())
        .bytesSynced(
          attempt
            .getOutput() // TODO (parker) remove after frontend switches to totalStats
            .map { obj: JobOutput -> obj.sync }
            .map { obj: StandardSyncOutput -> obj.standardSyncSummary }
            .map { obj: StandardSyncSummary -> obj.bytesSynced }
            .orElse(null),
        ).recordsSynced(
          attempt
            .getOutput() // TODO (parker) remove after frontend switches to totalStats
            .map { obj: JobOutput -> obj.sync }
            .map { obj: StandardSyncOutput -> obj.standardSyncSummary }
            .map { obj: StandardSyncSummary -> obj.recordsSynced }
            .orElse(null),
        ).totalStats(getTotalAttemptStats(attempt))
        .streamStats(getAttemptStreamStats(attempt))
        .createdAt(attempt.createdAtInSecond)
        .updatedAt(attempt.updatedAtInSecond)
        .endedAt(attempt.getEndedAtInSecond().orElse(null))
        .failureSummary(getAttemptFailureSummary(attempt))

    private fun getTotalAttemptStats(attempt: Attempt): AttemptStats? {
      val totalStats =
        attempt
          .getOutput()
          .map { obj: JobOutput -> obj.sync }
          .map { obj: StandardSyncOutput -> obj.standardSyncSummary }
          .map { obj: StandardSyncSummary -> obj.totalStats }
          .orElse(null)

      if (totalStats == null) {
        return null
      }

      return AttemptStats()
        .bytesEmitted(totalStats.bytesEmitted)
        .recordsEmitted(totalStats.recordsEmitted)
        .stateMessagesEmitted(totalStats.sourceStateMessagesEmitted)
        .recordsCommitted(totalStats.recordsCommitted)
    }

    private fun getAttemptStreamStats(attempt: Attempt): List<AttemptStreamStats>? {
      val streamStats =
        attempt
          .getOutput()
          .map { obj: JobOutput -> obj.sync }
          .map { obj: StandardSyncOutput -> obj.standardSyncSummary }
          .map { obj: StandardSyncSummary -> obj.streamStats }
          .orElse(null)

      if (streamStats == null) {
        return null
      }

      return streamStats
        .stream()
        .map { streamStat: StreamSyncStats ->
          AttemptStreamStats()
            .streamName(streamStat.streamName)
            .stats(
              AttemptStats()
                .bytesEmitted(streamStat.stats.bytesEmitted)
                .recordsEmitted(streamStat.stats.recordsEmitted)
                .stateMessagesEmitted(streamStat.stats.sourceStateMessagesEmitted)
                .recordsCommitted(streamStat.stats.recordsCommitted),
            )
        }.collect(Collectors.toList())
    }

    private fun getAttemptFailureSummary(attempt: Attempt): AttemptFailureSummary? {
      val failureSummary = attempt.getFailureSummary().orElse(null) ?: return null

      return AttemptFailureSummary()
        .failures(
          failureSummary.failures
            .stream()
            .map { failureReason: FailureReason? ->
              getFailureReason(
                failureReason,
                TimeUnit.SECONDS.toMillis(attempt.updatedAtInSecond),
              )
            }.toList(),
        ).partialSuccess(failureSummary.partialSuccess)
    }

    private fun getFailureReason(
      @Nullable failureReason: FailureReason?,
      defaultTimestamp: Long,
    ): io.airbyte.api.model.generated.FailureReason? {
      if (failureReason == null) {
        return null
      }
      return io.airbyte.api.model.generated
        .FailureReason()
        .failureOrigin(
          failureReason.failureOrigin?.convertTo<FailureOrigin>(),
        ).failureType(failureReason.failureType?.convertTo<FailureType>())
        .externalMessage(failureReason.externalMessage)
        .internalMessage(failureReason.internalMessage)
        .stacktrace(failureReason.stacktrace)
        .timestamp(if (failureReason.timestamp != null) failureReason.timestamp else defaultTimestamp)
        .retryable(failureReason.retryable)
    }

    private fun extractEnabledStreams(job: Job): List<io.airbyte.api.model.generated.StreamDescriptor> {
      val configuredCatalog = JobConfigProxy(job.config).configuredCatalog
      return if (configuredCatalog != null) {
        configuredCatalog.streams
          .stream()
          .map { s: ConfiguredAirbyteStream ->
            io.airbyte.api.model.generated
              .StreamDescriptor()
              .name(s.stream.name)
              .namespace(s.stream.namespace)
          }.collect(
            Collectors.toList(),
          )
      } else {
        listOf()
      }
    }

    private fun toModelLogEvents(
      logEvents: List<LogEvent>,
      logUtils: LogUtils,
    ): List<io.airbyte.api.model.generated.LogEvent> =
      logEvents
        .stream()
        .map { e: LogEvent ->
          val logEvent =
            io.airbyte.api.model.generated
              .LogEvent()
          logEvent.logSource = LogSource.fromString(e.logSource.displayName.lowercase())
          logEvent.level = LogLevel.fromString(e.level.lowercase())
          logEvent.message = e.message
          logEvent.timestamp = e.timestamp
          logEvent.stackTrace = logUtils.convertThrowableToStackTrace(e.throwable)
          logEvent.caller = toModelLogCaller(e.caller)
          logEvent
        }.toList()

    private fun toModelLogCaller(logCaller: LogCaller?): io.airbyte.api.model.generated.LogCaller? {
      if (logCaller != null) {
        val modelLogCaller =
          io.airbyte.api.model.generated
            .LogCaller()
        modelLogCaller.className = logCaller.className
        modelLogCaller.methodName = logCaller.methodName
        modelLogCaller.lineNumber = logCaller.lineNumber
        modelLogCaller.threadName = logCaller.threadName
        return modelLogCaller
      } else {
        return null
      }
    }
  }
}
