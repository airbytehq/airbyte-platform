/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters;

import io.airbyte.api.model.generated.AttemptFailureSummary;
import io.airbyte.api.model.generated.AttemptInfoRead;
import io.airbyte.api.model.generated.AttemptInfoReadLogs;
import io.airbyte.api.model.generated.AttemptRead;
import io.airbyte.api.model.generated.AttemptStats;
import io.airbyte.api.model.generated.AttemptStatus;
import io.airbyte.api.model.generated.AttemptStreamStats;
import io.airbyte.api.model.generated.DestinationDefinitionRead;
import io.airbyte.api.model.generated.FailureOrigin;
import io.airbyte.api.model.generated.FailureReason;
import io.airbyte.api.model.generated.FailureType;
import io.airbyte.api.model.generated.JobConfigType;
import io.airbyte.api.model.generated.JobDebugRead;
import io.airbyte.api.model.generated.JobInfoLightRead;
import io.airbyte.api.model.generated.JobInfoRead;
import io.airbyte.api.model.generated.JobOptionalRead;
import io.airbyte.api.model.generated.JobRead;
import io.airbyte.api.model.generated.JobRefreshConfig;
import io.airbyte.api.model.generated.JobStatus;
import io.airbyte.api.model.generated.JobWithAttemptsRead;
import io.airbyte.api.model.generated.LogCaller;
import io.airbyte.api.model.generated.LogEvent;
import io.airbyte.api.model.generated.LogFormatType;
import io.airbyte.api.model.generated.LogLevel;
import io.airbyte.api.model.generated.LogRead;
import io.airbyte.api.model.generated.LogSource;
import io.airbyte.api.model.generated.ResetConfig;
import io.airbyte.api.model.generated.SourceDefinitionRead;
import io.airbyte.api.model.generated.StreamDescriptor;
import io.airbyte.api.model.generated.SynchronousJobRead;
import io.airbyte.commons.converters.ApiConverters;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.logging.LogClientManager;
import io.airbyte.commons.logging.LogEvents;
import io.airbyte.commons.logging.LogUtils;
import io.airbyte.commons.server.scheduler.SynchronousJobMetadata;
import io.airbyte.commons.server.scheduler.SynchronousResponse;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.Attempt;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobConfigProxy;
import io.airbyte.config.JobOutput;
import io.airbyte.config.ResetSourceConfiguration;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncStats;
import io.micronaut.core.util.CollectionUtils;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Convert between API and internal versions of job models.
 */
@Singleton
public class JobConverter {

  private final LogClientManager logClientManager;

  private final LogUtils logUtils;

  public JobConverter(final LogClientManager logClientManager,
                      final LogUtils logUtils) {
    this.logClientManager = logClientManager;
    this.logUtils = logUtils;
  }

  public JobInfoRead getJobInfoRead(final Job job) {
    return new JobInfoRead()
        .job(getJobWithAttemptsRead(job).getJob())
        .attempts(job.getAttempts().stream().map(this::getAttemptInfoRead).collect(Collectors.toList()));
  }

  public JobInfoLightRead getJobInfoLightRead(final Job job) {
    return new JobInfoLightRead().job(getJobRead(job));
  }

  public JobOptionalRead getJobOptionalRead(final Optional<Job> job) {
    if (job.isEmpty()) {
      return new JobOptionalRead();
    }
    return new JobOptionalRead().job(getJobRead(job.get()));
  }

  public static JobDebugRead getDebugJobInfoRead(final JobInfoRead jobInfoRead,
                                                 final SourceDefinitionRead sourceDefinitionRead,
                                                 final DestinationDefinitionRead destinationDefinitionRead,
                                                 final AirbyteVersion airbyteVersion) {
    return new JobDebugRead()
        .id(jobInfoRead.getJob().getId())
        .configId(jobInfoRead.getJob().getConfigId())
        .configType(jobInfoRead.getJob().getConfigType())
        .status(jobInfoRead.getJob().getStatus())
        .airbyteVersion(airbyteVersion.serialize())
        .sourceDefinition(sourceDefinitionRead)
        .destinationDefinition(destinationDefinitionRead);
  }

  public static JobWithAttemptsRead getJobWithAttemptsRead(final Job job) {
    return new JobWithAttemptsRead()
        .job(getJobRead(job))
        .attempts(job.getAttempts().stream()
            .sorted(Comparator.comparingInt(Attempt::getAttemptNumber))
            .map(JobConverter::getAttemptRead)
            .toList());
  }

  public static JobRead getJobRead(final Job job) {
    final String configId = job.getScope();
    final JobConfigType configType = Enums.convertTo(job.getConfigType(), JobConfigType.class);

    return new JobRead()
        .id(job.getId())
        .configId(configId)
        .configType(configType)
        .enabledStreams(extractEnabledStreams(job))
        .resetConfig(extractResetConfigIfReset(job).orElse(null))
        .refreshConfig(extractRefreshConfigIfNeeded(job).orElse(null))
        .createdAt(job.getCreatedAtInSecond())
        .updatedAt(job.getUpdatedAtInSecond())
        .startedAt(job.getStartedAtInSecond().isPresent() ? job.getStartedAtInSecond().get() : null)
        .status(Enums.convertTo(job.getStatus(), JobStatus.class));
  }

  /**
   * If the job type is REFRESH or CLEAR/RESET, extracts the streams from the job config. Otherwise,
   * returns null.
   *
   * @param job - job
   * @return List of the streams associated with the job
   */
  public static List<io.airbyte.protocol.models.StreamDescriptor> getStreamsAssociatedWithJob(final Job job) {
    final JobRead jobRead = getJobRead(job);
    switch (job.getConfigType()) {
      case REFRESH -> {
        return jobRead.getRefreshConfig().getStreamsToRefresh().stream().map(streamDescriptor -> new io.airbyte.protocol.models.StreamDescriptor()
            .withName(streamDescriptor.getName())
            .withNamespace(streamDescriptor.getNamespace())).collect(Collectors.toList());
      }
      case CLEAR, RESET_CONNECTION -> {
        return jobRead.getResetConfig().getStreamsToReset().stream().map(streamDescriptor -> new io.airbyte.protocol.models.StreamDescriptor()
            .withName(streamDescriptor.getName())
            .withNamespace(streamDescriptor.getNamespace())).collect(Collectors.toList());
      }
      default -> {
        return null;
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
  private static Optional<ResetConfig> extractResetConfigIfReset(final Job job) {
    if (job.getConfigType() == ConfigType.RESET_CONNECTION) {
      final ResetSourceConfiguration resetSourceConfiguration = job.getConfig().getResetConnection().getResetSourceConfiguration();
      if (resetSourceConfiguration == null) {
        return Optional.empty();
      }
      return Optional.ofNullable(
          new ResetConfig().streamsToReset(job.getConfig().getResetConnection().getResetSourceConfiguration().getStreamsToReset()
              .stream()
              .map(ApiConverters::toApi)
              .toList()));
    } else {
      return Optional.empty();
    }
  }

  /**
   * If the job is of type RESET, extracts the part of the reset config that we expose in the API.
   * Otherwise, returns empty optional.
   *
   * @param job - job
   * @return api representation of refresh config
   */
  public static Optional<JobRefreshConfig> extractRefreshConfigIfNeeded(final Job job) {
    if (job.getConfigType() == ConfigType.REFRESH) {
      final List<StreamDescriptor> refreshedStreams = job.getConfig().getRefresh().getStreamsToRefresh()
          .stream().flatMap(refreshStream -> Stream.ofNullable(refreshStream.getStreamDescriptor()))
          .map(ApiConverters::toApi)
          .toList();
      if (refreshedStreams.isEmpty()) {
        return Optional.empty();
      }
      return Optional.ofNullable(new JobRefreshConfig().streamsToRefresh(refreshedStreams));
    } else {
      return Optional.empty();
    }
  }

  public AttemptInfoRead getAttemptInfoRead(final Attempt attempt) {
    final AttemptInfoReadLogs attemptInfoReadLogs = getAttemptLogs(attempt.getLogPath(), attempt.getJobId());
    return new AttemptInfoRead()
        .attempt(getAttemptRead(attempt))
        .logType(CollectionUtils.isNotEmpty(attemptInfoReadLogs.getEvents()) ? LogFormatType.STRUCTURED : LogFormatType.FORMATTED)
        .logs(attemptInfoReadLogs);
  }

  public static AttemptInfoRead getAttemptInfoWithoutLogsRead(final Attempt attempt) {
    return new AttemptInfoRead()
        .attempt(getAttemptRead(attempt));
  }

  public static AttemptRead getAttemptRead(final Attempt attempt) {
    return new AttemptRead()
        .id((long) attempt.getAttemptNumber())
        .status(Enums.convertTo(attempt.getStatus(), AttemptStatus.class))
        .bytesSynced(attempt.getOutput() // TODO (parker) remove after frontend switches to totalStats
            .map(JobOutput::getSync)
            .map(StandardSyncOutput::getStandardSyncSummary)
            .map(StandardSyncSummary::getBytesSynced)
            .orElse(null))
        .recordsSynced(attempt.getOutput() // TODO (parker) remove after frontend switches to totalStats
            .map(JobOutput::getSync)
            .map(StandardSyncOutput::getStandardSyncSummary)
            .map(StandardSyncSummary::getRecordsSynced)
            .orElse(null))
        .totalStats(getTotalAttemptStats(attempt))
        .streamStats(getAttemptStreamStats(attempt))
        .createdAt(attempt.getCreatedAtInSecond())
        .updatedAt(attempt.getUpdatedAtInSecond())
        .endedAt(attempt.getEndedAtInSecond().orElse(null))
        .failureSummary(getAttemptFailureSummary(attempt));
  }

  private static AttemptStats getTotalAttemptStats(final Attempt attempt) {
    final SyncStats totalStats = attempt.getOutput()
        .map(JobOutput::getSync)
        .map(StandardSyncOutput::getStandardSyncSummary)
        .map(StandardSyncSummary::getTotalStats)
        .orElse(null);

    if (totalStats == null) {
      return null;
    }

    return new AttemptStats()
        .bytesEmitted(totalStats.getBytesEmitted())
        .recordsEmitted(totalStats.getRecordsEmitted())
        .stateMessagesEmitted(totalStats.getSourceStateMessagesEmitted())
        .recordsCommitted(totalStats.getRecordsCommitted());
  }

  private static List<AttemptStreamStats> getAttemptStreamStats(final Attempt attempt) {
    final List<StreamSyncStats> streamStats = attempt.getOutput()
        .map(JobOutput::getSync)
        .map(StandardSyncOutput::getStandardSyncSummary)
        .map(StandardSyncSummary::getStreamStats)
        .orElse(null);

    if (streamStats == null) {
      return null;
    }

    return streamStats.stream()
        .map(streamStat -> new AttemptStreamStats()
            .streamName(streamStat.getStreamName())
            .stats(new AttemptStats()
                .bytesEmitted(streamStat.getStats().getBytesEmitted())
                .recordsEmitted(streamStat.getStats().getRecordsEmitted())
                .stateMessagesEmitted(streamStat.getStats().getSourceStateMessagesEmitted())
                .recordsCommitted(streamStat.getStats().getRecordsCommitted())))
        .collect(Collectors.toList());
  }

  private static AttemptFailureSummary getAttemptFailureSummary(final Attempt attempt) {
    final io.airbyte.config.AttemptFailureSummary failureSummary = attempt.getFailureSummary().orElse(null);

    if (failureSummary == null) {
      return null;
    }
    return new AttemptFailureSummary()
        .failures(failureSummary.getFailures().stream()
            .map(failureReason -> getFailureReason(failureReason, TimeUnit.SECONDS.toMillis(attempt.getUpdatedAtInSecond())))
            .toList())
        .partialSuccess(failureSummary.getPartialSuccess());
  }

  public LogRead getLogRead(final Path logPath) {
    try {
      return new LogRead().logLines(logClientManager.getJobLogFile(logPath));
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  public AttemptInfoReadLogs getAttemptLogs(final Path logPath, final Long jobId) {
    final LogEvents logEvents = logClientManager.getLogs(logPath);
    if (CollectionUtils.isNotEmpty(logEvents.getEvents())) {
      return new AttemptInfoReadLogs().events(toModelLogEvents(logEvents.getEvents(), logUtils)).version(logEvents.getVersion());
    } else {
      return new AttemptInfoReadLogs().logLines(getLogRead(logPath).getLogLines());
    }
  }

  private static FailureReason getFailureReason(final @Nullable io.airbyte.config.FailureReason failureReason, final long defaultTimestamp) {
    if (failureReason == null) {
      return null;
    }
    return new FailureReason()
        .failureOrigin(Enums.convertTo(failureReason.getFailureOrigin(), FailureOrigin.class))
        .failureType(Enums.convertTo(failureReason.getFailureType(), FailureType.class))
        .externalMessage(failureReason.getExternalMessage())
        .internalMessage(failureReason.getInternalMessage())
        .stacktrace(failureReason.getStacktrace())
        .timestamp(failureReason.getTimestamp() != null ? failureReason.getTimestamp() : defaultTimestamp)
        .retryable(failureReason.getRetryable());
  }

  public SynchronousJobRead getSynchronousJobRead(final SynchronousResponse<?> response) {
    return getSynchronousJobRead(response.getMetadata());
  }

  public SynchronousJobRead getSynchronousJobRead(final SynchronousJobMetadata metadata) {
    final JobConfigType configType = Enums.convertTo(metadata.getConfigType(), JobConfigType.class);
    final AttemptInfoReadLogs attemptInfoReadLogs = getAttemptLogs(metadata.getLogPath(), null);

    return new SynchronousJobRead()
        .id(metadata.getId())
        .configType(configType)
        .configId(String.valueOf(metadata.getConfigId()))
        .createdAt(metadata.getCreatedAt())
        .endedAt(metadata.getEndedAt())
        .succeeded(metadata.isSucceeded())
        .connectorConfigurationUpdated(metadata.isConnectorConfigurationUpdated())
        .logType(CollectionUtils.isNotEmpty(attemptInfoReadLogs.getEvents()) ? LogFormatType.STRUCTURED : LogFormatType.FORMATTED)
        .logs(attemptInfoReadLogs)
        .failureReason(getFailureReason(metadata.getFailureReason(), TimeUnit.SECONDS.toMillis(metadata.getEndedAt())));
  }

  private static List<StreamDescriptor> extractEnabledStreams(final Job job) {
    final var configuredCatalog = new JobConfigProxy(job.getConfig()).getConfiguredCatalog();
    return configuredCatalog != null
        ? configuredCatalog.getStreams().stream()
            .map(s -> new StreamDescriptor().name(s.getStream().getName()).namespace(s.getStream().getNamespace())).collect(Collectors.toList())
        : List.of();
  }

  private static List<LogEvent> toModelLogEvents(final List<io.airbyte.commons.logging.LogEvent> logEvents, final LogUtils logUtils) {
    return logEvents.stream().map(e -> {
      final LogEvent logEvent = new LogEvent();
      logEvent.setLogSource(LogSource.fromString(e.getLogSource().getDisplayName().toLowerCase(Locale.ROOT)));
      logEvent.setLevel(LogLevel.fromString(e.getLevel().toLowerCase(Locale.ROOT)));
      logEvent.setMessage(e.getMessage());
      logEvent.setTimestamp(e.getTimestamp());
      logEvent.setStackTrace(logUtils.convertThrowableToStackTrace(e.getThrowable()));
      logEvent.setCaller(toModelLogCaller(e.getCaller()));
      return logEvent;
    }).toList();
  }

  private static LogCaller toModelLogCaller(final io.airbyte.commons.logging.LogCaller logCaller) {
    if (logCaller != null) {
      final LogCaller modelLogCaller = new LogCaller();
      modelLogCaller.setClassName(logCaller.getClassName());
      modelLogCaller.setMethodName(logCaller.getMethodName());
      modelLogCaller.setLineNumber(logCaller.getLineNumber());
      modelLogCaller.setThreadName(logCaller.getThreadName());
      return modelLogCaller;
    } else {
      return null;
    }
  }

}
