/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import io.airbyte.api.model.generated.AttemptInfoReadLogs
import io.airbyte.api.model.generated.LogLevel
import io.airbyte.api.model.generated.LogRead
import io.airbyte.api.model.generated.LogSource
import io.airbyte.api.model.generated.SynchronousJobRead
import io.airbyte.api.server.generated.models.AttemptFailureSummary
import io.airbyte.api.server.generated.models.AttemptInfoRead
import io.airbyte.api.server.generated.models.AttemptRead
import io.airbyte.api.server.generated.models.AttemptStats
import io.airbyte.api.server.generated.models.AttemptStatus
import io.airbyte.api.server.generated.models.AttemptStreamStats
import io.airbyte.api.server.generated.models.DestinationDefinitionRead
import io.airbyte.api.server.generated.models.FailureOrigin
import io.airbyte.api.server.generated.models.FailureType
import io.airbyte.api.server.generated.models.JobConfigType
import io.airbyte.api.server.generated.models.JobDebugRead
import io.airbyte.api.server.generated.models.JobInfoLightRead
import io.airbyte.api.server.generated.models.JobInfoRead
import io.airbyte.api.server.generated.models.JobOptionalRead
import io.airbyte.api.server.generated.models.JobRead
import io.airbyte.api.server.generated.models.JobRefreshConfig
import io.airbyte.api.server.generated.models.JobStatus
import io.airbyte.api.server.generated.models.JobWithAttemptsRead
import io.airbyte.api.server.generated.models.LogFormatType
import io.airbyte.api.server.generated.models.ResetConfig
import io.airbyte.api.server.generated.models.SourceDefinitionRead
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.json.Jsons
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
import io.airbyte.config.StreamSyncStats
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.micronaut.core.util.CollectionUtils
import jakarta.annotation.Nullable
import jakarta.inject.Singleton
import java.io.IOException
import java.nio.file.Path
import java.util.Optional
import java.util.concurrent.TimeUnit

/**
 * Convert between API and internal versions of job models.
 */
@Singleton
class JobConverter(
  private val logClientManager: LogClientManager,
  private val logUtils: LogUtils,
) {
  fun getJobInfoRead(job: Job): JobInfoRead =
    JobInfoRead(
      job = getJobRead(job),
      attempts =
        job.attempts
          .map { attempt: Attempt -> this.getAttemptInfoRead(attempt) },
    )

  fun getJobInfoLightRead(job: Job): JobInfoLightRead = JobInfoLightRead(job = getJobRead(job))

  fun getJobOptionalRead(job: Optional<Job>): JobOptionalRead {
    if (job.isEmpty) {
      return JobOptionalRead()
    }
    return JobOptionalRead(job = getJobRead(job.get()))
  }

  fun getAttemptInfoRead(attempt: Attempt): AttemptInfoRead {
    val attemptInfoReadLogs = getAttemptLogs(attempt.logPath, attempt.jobId)
    return AttemptInfoRead(
      attempt = getAttemptRead(attempt),
      logType = if (CollectionUtils.isNotEmpty(attemptInfoReadLogs.events)) LogFormatType.STRUCTURED else LogFormatType.FORMATTED,
      logs = attemptInfoReadLogs,
    )
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
    // SynchronousJobRead is still the Java model (check/discover results move with the catalog models)
    val configType =
      metadata.configType.convertTo<io.airbyte.api.model.generated.JobConfigType>()
    val attemptInfoReadLogs = getAttemptLogs(metadata.logPath, null)

    return SynchronousJobRead()
      .id(metadata.id)
      .configType(configType)
      .configId(metadata.getConfigId().toString())
      .createdAt(metadata.createdAt)
      .endedAt(metadata.endedAt)
      .succeeded(metadata.isSucceeded)
      .connectorConfigurationUpdated(metadata.isConnectorConfigurationUpdated)
      .logType(
        if (CollectionUtils.isNotEmpty(attemptInfoReadLogs.events)) {
          io.airbyte.api.model.generated.LogFormatType.STRUCTURED
        } else {
          io.airbyte.api.model.generated.LogFormatType.FORMATTED
        },
      ).logs(attemptInfoReadLogs)
      .failureReason(
        Jsons.convertValue(
          getFailureReason(metadata.failureReason, TimeUnit.SECONDS.toMillis(metadata.endedAt)),
          io.airbyte.api.model.generated.FailureReason::class.java,
        ),
      )
  }

  companion object {
    @JvmStatic
    fun getDebugJobInfoRead(
      jobInfoRead: JobInfoRead,
      sourceDefinitionRead: SourceDefinitionRead,
      destinationDefinitionRead: DestinationDefinitionRead,
      airbyteVersion: AirbyteVersion,
    ): JobDebugRead =
      JobDebugRead(
        id = jobInfoRead.job.id,
        configId = jobInfoRead.job.configId,
        configType = jobInfoRead.job.configType,
        status = jobInfoRead.job.status,
        airbyteVersion = airbyteVersion.serialize(),
        sourceDefinition = sourceDefinitionRead,
        destinationDefinition = destinationDefinitionRead,
      )

    @JvmStatic
    fun getJobWithAttemptsRead(job: Job): JobWithAttemptsRead =
      JobWithAttemptsRead(
        job = getJobRead(job),
        attempts =
          job.attempts
            .sortedWith(Comparator.comparingInt { obj: Attempt -> obj.attemptNumber })
            .map { attempt: Attempt -> getAttemptRead(attempt) },
      )

    @JvmStatic
    fun getJobRead(job: Job): JobRead {
      val configId = job.scope
      val configType = job.configType.convertTo<JobConfigType>()

      return JobRead(
        id = job.id,
        configId = configId,
        configType = configType,
        enabledStreams = extractEnabledStreams(job),
        resetConfig = extractResetConfigIfReset(job).orElse(null),
        refreshConfig = extractRefreshConfigIfNeeded(job).orElse(null),
        createdAt = job.createdAtInSecond,
        updatedAt = job.updatedAtInSecond,
        startedAt = job.startedAtInSecond,
        status = job.status.convertTo<JobStatus>(),
      )
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
          jobRead.refreshConfig!!
            .streamsToRefresh!!
            .map { streamDescriptor: io.airbyte.api.server.generated.models.StreamDescriptor ->
              StreamDescriptor()
                .withName(streamDescriptor.name)
                .withNamespace(streamDescriptor.namespace)
            }
        }

        ConfigType.CLEAR, ConfigType.RESET_CONNECTION -> {
          jobRead.resetConfig!!
            .streamsToReset!!
            .map { streamDescriptor: io.airbyte.api.server.generated.models.StreamDescriptor ->
              StreamDescriptor()
                .withName(streamDescriptor.name)
                .withNamespace(streamDescriptor.namespace)
            }
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
          ResetConfig(
            streamsToReset =
              job.config.resetConnection.resetSourceConfiguration.streamsToReset
                .map { obj: io.airbyte.config.StreamDescriptor ->
                  io.airbyte.api.server.generated.models
                    .StreamDescriptor(name = obj.name, namespace = obj.namespace)
                },
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
            .mapNotNull { refreshStream ->
              refreshStream.streamDescriptor?.let {
                io.airbyte.api.server.generated.models.StreamDescriptor(
                  name = it.name,
                  namespace = it.namespace,
                )
              }
            }
        if (refreshedStreams.isEmpty()) {
          return Optional.empty()
        }
        return Optional.ofNullable(JobRefreshConfig(streamsToRefresh = refreshedStreams))
      } else {
        return Optional.empty()
      }
    }

    @JvmStatic
    fun getAttemptInfoWithoutLogsRead(attempt: Attempt): AttemptInfoRead = AttemptInfoRead(attempt = getAttemptRead(attempt))

    fun getAttemptRead(attempt: Attempt): AttemptRead =
      AttemptRead(
        id = attempt.attemptNumber.toLong(),
        status = attempt.status.convertTo<AttemptStatus>(),
        // TODO (parker) remove after frontend switches to totalStats
        bytesSynced =
          attempt.output
            ?.sync
            ?.standardSyncSummary
            ?.bytesSynced,
        // TODO (parker) remove after frontend switches to totalStats
        recordsSynced =
          attempt.output
            ?.sync
            ?.standardSyncSummary
            ?.recordsSynced,
        totalStats = getTotalAttemptStats(attempt),
        streamStats = getAttemptStreamStats(attempt),
        createdAt = attempt.createdAtInSecond,
        updatedAt = attempt.updatedAtInSecond,
        endedAt = attempt.endedAtInSecond,
        failureSummary = getAttemptFailureSummary(attempt),
      )

    private fun getTotalAttemptStats(attempt: Attempt): AttemptStats? {
      val totalStats =
        attempt.output
          ?.sync
          ?.standardSyncSummary
          ?.totalStats

      if (totalStats == null) {
        return null
      }

      return AttemptStats(
        bytesEmitted = totalStats.bytesEmitted,
        recordsEmitted = totalStats.recordsEmitted,
        stateMessagesEmitted = totalStats.sourceStateMessagesEmitted,
        recordsCommitted = totalStats.recordsCommitted,
      )
    }

    private fun getAttemptStreamStats(attempt: Attempt): List<AttemptStreamStats>? {
      val streamStats =
        attempt.output
          ?.sync
          ?.standardSyncSummary
          ?.streamStats

      if (streamStats == null) {
        return null
      }

      return streamStats
        .map { streamStat: StreamSyncStats ->
          AttemptStreamStats(
            streamName = streamStat.streamName,
            stats =
              AttemptStats(
                bytesEmitted = streamStat.stats.bytesEmitted,
                recordsEmitted = streamStat.stats.recordsEmitted,
                stateMessagesEmitted = streamStat.stats.sourceStateMessagesEmitted,
                recordsCommitted = streamStat.stats.recordsCommitted,
              ),
          )
        }
    }

    private fun getAttemptFailureSummary(attempt: Attempt): AttemptFailureSummary? {
      val failureSummary = attempt.failureSummary ?: return null

      return AttemptFailureSummary(
        failures =
          failureSummary.failures
            .mapNotNull { failureReason: FailureReason? ->
              getFailureReason(
                failureReason,
                TimeUnit.SECONDS.toMillis(attempt.updatedAtInSecond),
              )
            },
        partialSuccess = failureSummary.partialSuccess,
      )
    }

    private fun getFailureReason(
      @Nullable failureReason: FailureReason?,
      defaultTimestamp: Long,
    ): io.airbyte.api.server.generated.models.FailureReason? {
      if (failureReason == null) {
        return null
      }
      return io.airbyte.api.server.generated.models.FailureReason(
        failureOrigin = failureReason.failureOrigin?.convertTo<FailureOrigin>(),
        failureType = failureReason.failureType?.convertTo<FailureType>(),
        externalMessage = failureReason.externalMessage,
        internalMessage = failureReason.internalMessage,
        stacktrace = failureReason.stacktrace,
        timestamp = failureReason.timestamp ?: defaultTimestamp,
        retryable = failureReason.retryable,
      )
    }

    private fun extractEnabledStreams(job: Job): List<io.airbyte.api.server.generated.models.StreamDescriptor> {
      val configuredCatalog = JobConfigProxy(job.config).configuredCatalog
      return if (configuredCatalog != null) {
        configuredCatalog.streams
          .map { s: ConfiguredAirbyteStream ->
            io.airbyte.api.server.generated.models
              .StreamDescriptor(name = s.stream.name, namespace = s.stream.namespace)
          }
      } else {
        listOf()
      }
    }

    fun toModelLogEvents(
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
