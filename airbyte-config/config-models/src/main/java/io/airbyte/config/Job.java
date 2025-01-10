/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.google.common.base.Preconditions;
import io.airbyte.config.JobConfig.ConfigType;
import jakarta.annotation.Nullable;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * POJO / accessors for the job domain model.
 */
public class Job {

  public static final Set<ConfigType> REPLICATION_TYPES = EnumSet.of(ConfigType.SYNC, ConfigType.RESET_CONNECTION, ConfigType.REFRESH);
  public static final Set<ConfigType> SYNC_REPLICATION_TYPES = EnumSet.of(ConfigType.SYNC, ConfigType.REFRESH);

  private final long id;
  private final ConfigType configType;
  private final String scope;
  private final JobConfig config;
  private final JobStatus status;
  private final Long startedAtInSecond;
  private final long createdAtInSecond;
  private final long updatedAtInSecond;
  private final List<Attempt> attempts;

  public Job(final long id,
             final ConfigType configType,
             final String scope,
             final JobConfig config,
             final List<Attempt> attempts,
             final JobStatus status,
             final @Nullable Long startedAtInSecond,
             final long createdAtInSecond,
             final long updatedAtInSecond) {
    this.id = id;
    this.configType = configType;
    this.scope = scope;
    this.config = config;
    this.attempts = attempts;
    this.status = status;
    this.startedAtInSecond = startedAtInSecond;
    this.createdAtInSecond = createdAtInSecond;
    this.updatedAtInSecond = updatedAtInSecond;
  }

  /**
   * Get job id.
   *
   * @return job id
   */
  public long getId() {
    return id;
  }

  /**
   * Get job type. At this point this is only sync and reset.
   *
   * @return config type
   */
  public ConfigType getConfigType() {
    return configType;
  }

  /**
   * Get scope for a job.
   *
   * @return scope
   */
  public String getScope() {
    return scope;
  }

  /**
   * Get config for a job.
   *
   * @return config
   */
  public JobConfig getConfig() {
    return config;
  }

  /**
   * Get all attempts for the job. No order guarantees.
   *
   * @return list of attempts
   */
  public List<Attempt> getAttempts() {
    return attempts;
  }

  /**
   * Get number of attempts.
   *
   * @return number of attempts
   */
  public int getAttemptsCount() {
    return attempts.size();
  }

  /**
   * Get job status.
   *
   * @return status of job
   */
  public JobStatus getStatus() {
    return status;
  }

  /**
   * Get started at of job in seconds.
   *
   * @return started at
   */
  public Optional<Long> getStartedAtInSecond() {
    return Optional.ofNullable(startedAtInSecond);
  }

  /**
   * Get create at of job in seconds.
   *
   * @return create at
   */
  public long getCreatedAtInSecond() {
    return createdAtInSecond;
  }

  /**
   * Get updated at of job in seconds.
   *
   * @return updated at
   */
  public long getUpdatedAtInSecond() {
    return updatedAtInSecond;
  }

  /**
   * Get the successful attempt, if present. By definition there should only ever be at most one
   * successful attempt for a job.
   *
   * @return successful attempt, if present.
   */
  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  public Optional<Attempt> getSuccessfulAttempt() {
    final List<Attempt> successfulAttempts = getAttempts()
        .stream()
        .filter(a -> a.getStatus() == AttemptStatus.SUCCEEDED)
        .collect(Collectors.toList());

    Preconditions.checkState(successfulAttempts.size() <= 1, String.format("Job %s has multiple successful attempts.", getId()));
    if (successfulAttempts.size() == 1) {
      return Optional.of(successfulAttempts.get(0));
    } else {
      return Optional.empty();
    }
  }

  /**
   * Get success output if present.
   *
   * @return if present, job output
   */
  public Optional<JobOutput> getSuccessOutput() {
    return getSuccessfulAttempt().flatMap(Attempt::getOutput);
  }

  /**
   * Get the last attempt by created_at for the job that failed.
   *
   * @return the last attempt. empty optional, if there have been no attempts that have failed.
   */
  public Optional<Attempt> getLastFailedAttempt() {
    return getAttempts()
        .stream()
        .sorted(Comparator.comparing(Attempt::getCreatedAtInSecond).reversed())
        .filter(a -> a.getStatus() == AttemptStatus.FAILED)
        .findFirst();
  }

  /**
   * Get the last attempt by created_at for the job.
   *
   * @return the last attempt. empty optional, if there have been no attempts.
   */
  public Optional<Attempt> getLastAttempt() {
    return getAttempts()
        .stream()
        .max(Comparator.comparing(Attempt::getCreatedAtInSecond));
  }

  /**
   * Get attempt with a given attempt number.
   *
   * @param attemptNumber attempt number to select
   * @return selected attempt. empty optional, if attempt is not present.
   */
  public Optional<Attempt> getAttemptByNumber(final int attemptNumber) {
    return getAttempts()
        .stream()
        .filter(a -> a.getAttemptNumber() == attemptNumber)
        .findFirst();
  }

  /**
   * Test if the job has running attempts.
   *
   * @return true if has running. otherwise, false.
   */
  public boolean hasRunningAttempt() {
    return getAttempts().stream().anyMatch(a -> !Attempt.isAttemptInTerminalState(a));
  }

  /**
   * Test if the current status is a terminal status.
   *
   * @return true if terminal. otherwise, false.
   */
  public boolean isJobInTerminalState() {
    return JobStatus.TERMINAL_STATUSES.contains(getStatus());
  }

  /**
   * Validate that it is legal, according to the job status state machine, for the job to transition
   * from its current status to the provided status.
   *
   * @param newStatus candidate new status
   * @throws IllegalStateException if the new status is not a legal transition
   */
  public void validateStatusTransition(final JobStatus newStatus) throws IllegalStateException {
    final Set<JobStatus> validNewStatuses = JobStatus.VALID_STATUS_CHANGES.get(status);

    if (!validNewStatuses.contains(newStatus)) {
      throw new IllegalStateException(String.format(
          "Transitioning Job %d from JobStatus %s to %s is not allowed. The only valid statuses that an be transitioned to from %s are %s",
          id,
          status,
          newStatus,
          status,
          JobStatus.VALID_STATUS_CHANGES.get(status)));
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Job job = (Job) o;
    return id == job.id
        && createdAtInSecond == job.createdAtInSecond
        && updatedAtInSecond == job.updatedAtInSecond
        && Objects.equals(scope, job.scope)
        && Objects.equals(config, job.config)
        && Objects.equals(configType, job.configType)
        && status == job.status
        && Objects.equals(startedAtInSecond, job.startedAtInSecond)
        && Objects.equals(attempts, job.attempts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, scope, config, configType, status, startedAtInSecond, createdAtInSecond, updatedAtInSecond, attempts);
  }

  @Override
  public String toString() {
    return "Job{"
        + "id=" + id
        + ", scope='" + scope + '\''
        + ", config=" + config
        + ", config_type=" + configType
        + ", status=" + status
        + ", startedAtInSecond=" + startedAtInSecond
        + ", createdAtInSecond=" + createdAtInSecond
        + ", updatedAtInSecond=" + updatedAtInSecond
        + ", attempts=" + attempts
        + '}';
  }

}
