/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job;

import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.AttemptSyncConfig;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobOutput;
import io.airbyte.config.NormalizationSummary;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncStats;
import io.airbyte.persistence.job.models.Attempt;
import io.airbyte.persistence.job.models.AttemptNormalizationStatus;
import io.airbyte.persistence.job.models.AttemptWithJobInfo;
import io.airbyte.persistence.job.models.Job;
import io.airbyte.persistence.job.models.JobStatus;
import io.airbyte.persistence.job.models.JobStatusSummary;
import io.airbyte.persistence.job.models.JobWithStatusAndTimestamp;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * General interface methods for persistence to the Jobs database. This database is separate from
 * the config database as job-related tables has an order of magnitude higher load and scale
 * differently from the config tables.
 */
public interface JobPersistence {

  //
  // SIMPLE GETTERS
  //

  /**
   * Retrieve the combined and per stream stats for a single attempt.
   *
   * @return {@link AttemptStats}
   */
  AttemptStats getAttemptStats(long jobId, int attemptNumber) throws IOException;

  /**
   * Alternative method to retrieve combined and per stream stats per attempt for a list of jobs to
   * avoid overloading the database with too many queries.
   * <p>
   * This implementation is intended to utilise complex joins under the hood to reduce the potential
   * N+1 database pattern.
   *
   * @param jobIds job ids to fetch for
   * @return attempt status for desired jobs
   * @throws IOException while interacting with the db
   */
  Map<JobAttemptPair, AttemptStats> getAttemptStats(List<Long> jobIds) throws IOException;

  /**
   * Retrieve only the combined stats for a single attempt.
   *
   * @return {@link AttemptStats}
   */
  SyncStats getAttemptCombinedStats(long jobId, int attemptNumber) throws IOException;

  List<NormalizationSummary> getNormalizationSummary(long jobId, int attemptNumber) throws IOException;

  Job getJob(long jobId) throws IOException;

  /**
   * Enqueue a new job. Its initial status will be pending.
   *
   * @param scope key that will be used to determine if two jobs should not be run at the same time;
   *        it is the primary id of the standard sync (StandardSync#connectionId)
   * @param jobConfig configuration for the job
   * @return job id
   * @throws IOException exception due to interaction with persistence
   */
  Optional<Long> enqueueJob(String scope, JobConfig jobConfig) throws IOException;

  /**
   * Set job status from current status to PENDING. Throws {@link IllegalStateException} if the job is
   * in a terminal state.
   *
   * @param jobId job to reset
   * @throws IOException exception due to interaction with persistence
   */
  void resetJob(long jobId) throws IOException;

  //
  // JOB LIFECYCLE
  //

  /**
   * Set job status from current status to CANCELLED. If already in a terminal status, no op.
   *
   * @param jobId job to cancel
   * @throws IOException exception due to interaction with persistence
   */
  void cancelJob(long jobId) throws IOException;

  /**
   * Set job status from current status to FAILED. If already in a terminal status, no op.
   *
   * @param jobId job to fail
   * @throws IOException exception due to interaction with persistence
   */
  void failJob(long jobId) throws IOException;

  /**
   * Create a new attempt for a job and return its attempt number. Throws
   * {@link IllegalStateException} if the job is already in a terminal state.
   *
   * @param jobId job for which an attempt will be created
   * @param logPath path where logs should be written for the attempt
   * @return The attempt number of the created attempt (see {@link DefaultJobPersistence})
   * @throws IOException exception due to interaction with persistence
   */
  int createAttempt(long jobId, Path logPath) throws IOException;

  /**
   * Sets an attempt to FAILED. Also attempts to set the parent job to INCOMPLETE. The job's status
   * will not be changed if it is already in a terminal state.
   *
   * @param jobId job id
   * @param attemptNumber attempt id
   * @throws IOException exception due to interaction with persistence
   */
  void failAttempt(long jobId, int attemptNumber) throws IOException;

  //
  // ATTEMPT LIFECYCLE
  //

  /**
   * Sets an attempt to SUCCEEDED. Also attempts to set the parent job to SUCCEEDED. The job's status
   * is changed regardless of what state it is in.
   *
   * @param jobId job id
   * @param attemptNumber attempt id
   * @throws IOException exception due to interaction with persistence
   */
  void succeedAttempt(long jobId, int attemptNumber) throws IOException;

  /**
   * Sets an attempt's temporal workflow id. Later used to cancel the workflow.
   */
  void setAttemptTemporalWorkflowInfo(long jobId, int attemptNumber, String temporalWorkflowId, String processingTaskQueue) throws IOException;

  /**
   * Retrieves an attempt's temporal workflow id. Used to cancel the workflow.
   */
  Optional<String> getAttemptTemporalWorkflowId(long jobId, int attemptNumber) throws IOException;

  //
  // END OF LIFECYCLE
  //

  /**
   * Retrieves an Attempt from a given jobId and attemptNumber.
   */
  Optional<Attempt> getAttemptForJob(long jobId, int attemptNumber) throws IOException;

  /**
   * When the output is a StandardSyncOutput, caller of this method should persist
   * StandardSyncOutput#state in the configs database by calling
   * ConfigRepository#updateConnectionState, which takes care of persisting the connection state.
   */
  void writeOutput(long jobId, int attemptNumber, JobOutput output) throws IOException;

  void writeStats(long jobId,
                  int attemptNumber,
                  Long estimatedRecords,
                  Long estimatedBytes,
                  Long recordsEmitted,
                  Long bytesEmitted,
                  Long recordsCommitted,
                  Long bytesCommitted,
                  List<StreamSyncStats> streamStats)
      throws IOException;

  /**
   * Writes a summary of all failures that occurred during the attempt.
   *
   * @param jobId job id
   * @param attemptNumber attempt number
   * @param failureSummary summary containing failure metadata and ordered list of failures
   * @throws IOException exception due to interaction with persistence
   */
  void writeAttemptFailureSummary(long jobId, int attemptNumber, AttemptFailureSummary failureSummary) throws IOException;

  /**
   * Writes the attempt-specific configuration used to build the sync input during the attempt.
   *
   * @param jobId job id
   * @param attemptNumber attempt number
   * @param attemptSyncConfig attempt-specific configuration used to build the sync input for this
   *        attempt
   * @throws IOException exception due to interaction with persistence
   */
  void writeAttemptSyncConfig(long jobId, int attemptNumber, AttemptSyncConfig attemptSyncConfig) throws IOException;

  /**
   * Get count of jobs beloging to the specified connection. This override allows passing several
   * query filters.
   *
   * @param configTypes - the type of config, e.g. sync
   * @param connectionId - ID of the connection for which the job count should be retrieved
   * @param status - status to filter by
   * @param createdAtStart - minimum created at date to filter by
   * @param createdAtEnd - maximum created at date to filter by
   * @param updatedAtStart - minimum updated at date to filter by
   * @param updatedAtEnd - maximum updated at date to filter by
   * @return count of jobs belonging to the specified connection
   */
  Long getJobCount(final Set<ConfigType> configTypes,
                   final String connectionId,
                   final JobStatus status,
                   final OffsetDateTime createdAtStart,
                   final OffsetDateTime createdAtEnd,
                   final OffsetDateTime updatedAtStart,
                   final OffsetDateTime updatedAtEnd)
      throws IOException;

  /**
   * List jobs of a connection. Pageable.
   *
   * @param configTypes - type of config, e.g. sync
   * @param configId - id of that config
   * @return lists job in descending order by created_at
   * @throws IOException - what you do when you IO
   */
  List<Job> listJobs(Set<ConfigType> configTypes, String configId, int limit) throws IOException;

  /**
   * List jobs of a connection with filters. Pageable.
   *
   * @param configTypes - type of config, e.g. sync
   * @param configId - id of that config
   * @return lists job in descending order by created_at
   * @throws IOException - what you do when you IO
   */
  List<Job> listJobs(
                     Set<JobConfig.ConfigType> configTypes,
                     String configId,
                     int limit,
                     int offset,
                     JobStatus status,
                     OffsetDateTime createdAtStart,
                     OffsetDateTime createdAtEnd,
                     OffsetDateTime updatedAtStart,
                     OffsetDateTime updatedAtEnd,
                     String orderByField,
                     String orderByMethod)
      throws IOException;

  /**
   * List jobs of a connection. Pageable.
   *
   * @param configTypes - type of config, e.g. sync
   * @param workspaceIds - ids of requested workspaces
   * @return lists job in descending order by created_at
   * @throws IOException - what you do when you IO
   */
  List<Job> listJobs(
                     Set<JobConfig.ConfigType> configTypes,
                     List<UUID> workspaceIds,
                     int limit,
                     int offset,
                     JobStatus status,
                     OffsetDateTime createdAtStart,
                     OffsetDateTime createdAtEnd,
                     OffsetDateTime updatedAtStart,
                     OffsetDateTime updatedAtEnd,
                     String orderByField,
                     String orderByMethod)
      throws IOException;

  /**
   * List jobs of a config type after a certain time.
   *
   * @param configType The type of job
   * @param attemptEndedAtTimestamp The timestamp after which you want the jobs
   * @return List of jobs that have attempts after the provided timestamp
   */
  List<Job> listJobs(ConfigType configType, Instant attemptEndedAtTimestamp) throws IOException;

  /**
   * List jobs with id.
   *
   * @param configTypes - type of config, e.g. sync
   * @param connectionId - id of the connection for which jobs should be retrieved
   * @param includingJobId - id of the job that should be the included in the list, if it exists in
   *        the connection
   * @param pagesize - the pagesize that should be used when building the list (response may include
   *        multiple pages)
   * @return List of jobs in descending created_at order including the specified job. Will include
   *         multiple pages of jobs if required to include the specified job. If the specified job
   *         does not exist in the connection, the returned list will be empty.
   */
  List<Job> listJobsIncludingId(Set<JobConfig.ConfigType> configTypes, String connectionId, long includingJobId, int pagesize) throws IOException;

  List<Job> listJobsWithStatus(JobStatus status) throws IOException;

  List<Job> listJobsWithStatus(Set<JobConfig.ConfigType> configTypes, JobStatus status) throws IOException;

  List<Job> listJobsWithStatus(JobConfig.ConfigType configType, JobStatus status) throws IOException;

  List<Job> listJobsForConnectionWithStatuses(UUID connectionId, Set<JobConfig.ConfigType> configTypes, Set<JobStatus> statuses) throws IOException;

  List<AttemptWithJobInfo> listAttemptsForConnectionAfterTimestamp(UUID connectionId,
                                                                   ConfigType configType,
                                                                   Instant attemptEndedAtTimestamp)
      throws IOException;

  /**
   * List job statuses and timestamps for connection id.
   *
   * @param connectionId The ID of the connection
   * @param configTypes The types of jobs
   * @param jobCreatedAtTimestamp The timestamp after which you want the jobs
   * @return List of jobs that only include information regarding id, status, timestamps from a
   *         specific connection that have attempts after the provided timestamp, sorted by jobs'
   *         createAt in descending order
   */
  List<JobWithStatusAndTimestamp> listJobStatusAndTimestampWithConnection(UUID connectionId,
                                                                          Set<JobConfig.ConfigType> configTypes,
                                                                          Instant jobCreatedAtTimestamp)
      throws IOException;

  Optional<Job> getLastReplicationJob(UUID connectionId) throws IOException;

  Optional<Job> getLastSyncJob(UUID connectionId) throws IOException;

  List<JobStatusSummary> getLastSyncJobForConnections(final List<UUID> connectionIds) throws IOException;

  List<Job> getRunningSyncJobForConnections(final List<UUID> connectionIds) throws IOException;

  Optional<Job> getFirstReplicationJob(UUID connectionId) throws IOException;

  Optional<Job> getNextJob() throws IOException;

  /**
   * List attempts after a certain type of a type. Used for cloud billing.
   *
   * @param configType The type of job
   * @param attemptEndedAtTimestamp The timestamp after which you want the attempts
   * @return List of attempts (with job attached) that ended after the provided timestamp, sorted by
   *         attempts' endedAt in ascending order
   * @throws IOException while interacting with the db.
   */
  List<AttemptWithJobInfo> listAttemptsWithJobInfo(ConfigType configType, Instant attemptEndedAtTimestamp, final int limit) throws IOException;

  /**
   * Returns the AirbyteVersion.
   */
  Optional<String> getVersion() throws IOException;

  /**
   * Set the airbyte version.
   */
  void setVersion(String airbyteVersion) throws IOException;
  /// ARCHIVE

  /**
   * Get the max supported Airbyte Protocol Version.
   */
  Optional<Version> getAirbyteProtocolVersionMax() throws IOException;

  /**
   * Set the max supported Airbyte Protocol Version.
   */
  void setAirbyteProtocolVersionMax(Version version) throws IOException;

  /**
   * Get the min supported Airbyte Protocol Version.
   */
  Optional<Version> getAirbyteProtocolVersionMin() throws IOException;

  /**
   * Set the min supported Airbyte Protocol Version.
   */
  void setAirbyteProtocolVersionMin(Version version) throws IOException;

  /**
   * Get the current Airbyte Protocol Version range if defined.
   */
  Optional<AirbyteProtocolVersionRange> getCurrentProtocolVersionRange() throws IOException;

  /**
   * Returns a deployment UUID.
   */
  Optional<UUID> getDeployment() throws IOException;

  /**
   * Set deployment id. If one is already set, the new value is ignored.
   */
  void setDeployment(UUID uuid) throws IOException;

  /**
   * Purges job history while ensuring that the latest saved-state information is maintained.
   */
  void purgeJobHistory();
  // a deployment references a setup of airbyte. it is created the first time the docker compose or
  // K8s is ready.

  List<AttemptNormalizationStatus> getAttemptNormalizationStatusesForJob(final Long jobId) throws IOException;

  /**
   * Convenience POJO for various stats data structures.
   *
   * @param combinedStats stats for the job
   * @param perStreamStats stats for each stream
   */
  record AttemptStats(SyncStats combinedStats, List<StreamSyncStats> perStreamStats) {

  }

  /**
   * Pair of the job id and attempt number.
   *
   * @param id job id
   * @param attemptNumber attempt number
   */
  record JobAttemptPair(long id, int attemptNumber) {

  }

}
