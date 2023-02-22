/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.scheduler;

import io.airbyte.commons.temporal.JobMetadata;
import io.airbyte.config.FailureReason;
import io.airbyte.config.JobConfig.ConfigType;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public class SynchronousJobMetadata {

  private final UUID id;
  private final ConfigType configType;
  private final UUID configId;

  private final long createdAt;
  private final long endedAt;
  private final boolean succeeded;
  private final boolean connectorConfigurationUpdated;

  private final Path logPath;

  private final FailureReason failureReason;

  public static SynchronousJobMetadata fromJobMetadata(final JobMetadata jobMetadata,
                                                       final UUID id,
                                                       final ConfigType configType,
                                                       final UUID configId,
                                                       final boolean connectorConfigurationUpdated,
                                                       final long createdAt,
                                                       final long endedAt,
                                                       final FailureReason failureReason) {
    return new SynchronousJobMetadata(
        id,
        configType,
        configId,
        createdAt,
        endedAt,
        jobMetadata.isSucceeded(),
        connectorConfigurationUpdated,
        jobMetadata.getLogPath(),
        failureReason);
  }

  public SynchronousJobMetadata(final UUID id,
                                final ConfigType configType,
                                final UUID configId,
                                final long createdAt,
                                final long endedAt,
                                final boolean succeeded,
                                final boolean connectorConfigurationUpdated,
                                final Path logPath,
                                final FailureReason failureReason) {
    this.id = id;
    this.configType = configType;
    this.configId = configId;
    this.createdAt = createdAt;
    this.endedAt = endedAt;
    this.succeeded = succeeded;
    this.connectorConfigurationUpdated = connectorConfigurationUpdated;
    this.logPath = logPath;
    this.failureReason = failureReason;
  }

  public UUID getId() {
    return id;
  }

  public ConfigType getConfigType() {
    return configType;
  }

  public Optional<UUID> getConfigId() {
    return Optional.ofNullable(configId);
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public long getEndedAt() {
    return endedAt;
  }

  public boolean isSucceeded() {
    return succeeded;
  }

  public boolean isConnectorConfigurationUpdated() {
    return connectorConfigurationUpdated;
  }

  public Path getLogPath() {
    return logPath;
  }

  public FailureReason getFailureReason() {
    return failureReason;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SynchronousJobMetadata that = (SynchronousJobMetadata) o;
    return createdAt == that.createdAt && endedAt == that.endedAt && succeeded == that.succeeded
        && connectorConfigurationUpdated == that.connectorConfigurationUpdated && Objects.equals(id, that.id)
        && configType == that.configType && Objects.equals(configId, that.configId) && Objects.equals(logPath, that.logPath)
        && Objects.equals(failureReason, that.failureReason);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, configType, configId, createdAt, endedAt, succeeded, connectorConfigurationUpdated, logPath, failureReason);
  }

  @Override
  public String toString() {
    return "SynchronousJobMetadata{" +
        "id=" + id +
        ", configType=" + configType +
        ", configId=" + configId +
        ", createdAt=" + createdAt +
        ", endedAt=" + endedAt +
        ", succeeded=" + succeeded +
        ", connectorConfigurationUpdated=" + connectorConfigurationUpdated +
        ", logPath=" + logPath +
        ", failureReason=" + failureReason +
        '}';
  }

  public static SynchronousJobMetadata mock(final ConfigType configType) {
    final long now = Instant.now().toEpochMilli();
    final UUID configId = null;
    final boolean succeeded = true;
    final boolean connectorConfigurationUpdated = false;
    final Path logPath = null;
    final FailureReason failureReason = new FailureReason();

    return new SynchronousJobMetadata(
        UUID.randomUUID(),
        configType,
        configId,
        now,
        now,
        succeeded,
        connectorConfigurationUpdated,
        logPath,
        failureReason);
  }

}
