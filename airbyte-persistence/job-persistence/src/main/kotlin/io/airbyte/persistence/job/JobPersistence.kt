/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job

import io.airbyte.commons.version.AirbyteProtocolVersionRange
import io.airbyte.commons.version.Version
import io.airbyte.config.Attempt
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.AttemptSyncConfig
import io.airbyte.config.AttemptWithJobInfo
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobOutput
import io.airbyte.config.JobStatus
import io.airbyte.config.JobStatusSummary
import io.airbyte.config.StreamSyncStats
import io.airbyte.config.SyncStats
import java.io.IOException
import java.nio.file.Path
import java.time.Instant
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

/**
 * General interface methods for persistence to the Jobs database. This database is separate from
 * the config database as job-related tables has an order of magnitude higher load and scale
 * differently from the config tables.
 */
interface JobPersistence {
  /**
   * Retrieve the combined and per stream stats for a single attempt. This included the stream
   * metadata which means that was resumed and was backfilled are properly populated.
   *
   * @return [AttemptStats]
   */
  @Throws(IOException::class)
  fun getAttemptStatsWithStreamMetadata(
    jobId: Long,
    attemptNumber: Int,
  ): AttemptStats

  /**
   * Retrieve the combined and per stream stats for a single attempt.
   *
   * @return [AttemptStats]
   */
  @Deprecated("")
  @Throws(IOException::class)
  fun getAttemptStats(
    jobId: Long,
    attemptNumber: Int,
  ): AttemptStats

  /**
   * Alternative method to retrieve combined and per stream stats per attempt for a list of jobs to
   * avoid overloading the database with too many queries.
   *
   *
   * This implementation is intended to utilise complex joins under the hood to reduce the potential
   * N+1 database pattern.
   *
   * @param jobIds job ids to fetch for
   * @return attempt status for desired jobs
   * @throws IOException while interacting with the db
   */
  @Throws(IOException::class)
  fun getAttemptStats(jobIds: List<Long>?): Map<JobAttemptPair, AttemptStats>

  /**
   * Retrieve only the combined stats for a single attempt.
   *
   * @return [AttemptStats]
   */
  @Throws(IOException::class)
  fun getAttemptCombinedStats(
    jobId: Long,
    attemptNumber: Int,
  ): SyncStats?

  @Throws(IOException::class)
  fun getJob(jobId: Long): Job

  /**
   * Enqueue a new job. Its initial status will be pending.
   *
   * @param scope key that will be used to determine if two jobs should not be run at the same time;
   * it is the primary id of the standard sync (StandardSync#connectionId)
   * @param jobConfig configuration for the job
   * @param isScheduled whether the job is scheduled or not
   * @return job id
   * @throws IOException exception due to interaction with persistence
   */
  @Throws(IOException::class)
  fun enqueueJob(
    scope: String,
    jobConfig: JobConfig,
    isScheduled: Boolean,
  ): Optional<Long>

  //
  // JOB LIFECYCLE
  //

  /**
   * Set job status from current status to CANCELLED. If already in a terminal status, no op.
   *
   * @param jobId job to cancel
   * @throws IOException exception due to interaction with persistence
   */
  @Throws(IOException::class)
  fun cancelJob(jobId: Long)

  /**
   * Set job status from current status to FAILED. If already in a terminal status, no op.
   *
   * @param jobId job to fail
   * @throws IOException exception due to interaction with persistence
   */
  @Throws(IOException::class)
  fun failJob(jobId: Long)

  /**
   * Create a new attempt for a job and return its attempt number. Throws
   * [IllegalStateException] if the job is already in a terminal state.
   *
   * @param jobId job for which an attempt will be created
   * @param logPath path where logs should be written for the attempt
   * @return The attempt number of the created attempt (see [DefaultJobPersistence])
   * @throws IOException exception due to interaction with persistence
   */
  @Throws(IOException::class)
  fun createAttempt(
    jobId: Long,
    logPath: Path,
  ): Int

  /**
   * Sets an attempt to FAILED. Also attempts to set the parent job to INCOMPLETE. The job's status
   * will not be changed if it is already in a terminal state.
   *
   * @param jobId job id
   * @param attemptNumber attempt id
   * @throws IOException exception due to interaction with persistence
   */
  @Throws(IOException::class)
  fun failAttempt(
    jobId: Long,
    attemptNumber: Int,
  )

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
  @Throws(IOException::class)
  fun succeedAttempt(
    jobId: Long,
    attemptNumber: Int,
  )

  //
  // END OF LIFECYCLE
  //

  /**
   * Retrieves an Attempt from a given jobId and attemptNumber.
   */
  @Throws(IOException::class)
  fun getAttemptForJob(
    jobId: Long,
    attemptNumber: Int,
  ): Optional<Attempt>

  /**
   * When the output is a StandardSyncOutput, caller of this method should persist
   * StandardSyncOutput#state in the configs database by calling
   * ConfigRepository#updateConnectionState, which takes care of persisting the connection state.
   */
  @Throws(IOException::class)
  fun writeOutput(
    jobId: Long,
    attemptNumber: Int,
    output: JobOutput,
  )

  @Throws(IOException::class)
  fun writeStats(
    jobId: Long,
    attemptNumber: Int,
    estimatedRecords: Long?,
    estimatedBytes: Long?,
    recordsEmitted: Long?,
    bytesEmitted: Long?,
    recordsCommitted: Long?,
    bytesCommitted: Long?,
    recordsRejected: Long?,
    connectionId: UUID?,
    streamStats: List<StreamSyncStats>?,
  )

  /**
   * Writes a summary of all failures that occurred during the attempt.
   *
   * @param jobId job id
   * @param attemptNumber attempt number
   * @param failureSummary summary containing failure metadata and ordered list of failures
   * @throws IOException exception due to interaction with persistence
   */
  @Throws(IOException::class)
  fun writeAttemptFailureSummary(
    jobId: Long,
    attemptNumber: Int,
    failureSummary: AttemptFailureSummary?,
  )

  /**
   * Writes the attempt-specific configuration used to build the sync input during the attempt.
   *
   * @param jobId job id
   * @param attemptNumber attempt number
   * @param attemptSyncConfig attempt-specific configuration used to build the sync input for this
   * attempt
   * @throws IOException exception due to interaction with persistence
   */
  @Throws(IOException::class)
  fun writeAttemptSyncConfig(
    jobId: Long,
    attemptNumber: Int,
    attemptSyncConfig: AttemptSyncConfig?,
  )

  /**
   * Get count of jobs beloging to the specified connection. This override allows passing several
   * query filters.
   *
   * @param configTypes - the type of config, e.g. sync
   * @param connectionId - ID of the connection for which the job count should be retrieved
   * @param statuses - statuses to filter by
   * @param createdAtStart - minimum created at date to filter by
   * @param createdAtEnd - maximum created at date to filter by
   * @param updatedAtStart - minimum updated at date to filter by
   * @param updatedAtEnd - maximum updated at date to filter by
   * @return count of jobs belonging to the specified connection
   */
  @Throws(IOException::class)
  fun getJobCount(
    configTypes: Set<ConfigType>,
    connectionId: String?,
    statuses: List<JobStatus>?,
    createdAtStart: OffsetDateTime?,
    createdAtEnd: OffsetDateTime?,
    updatedAtStart: OffsetDateTime?,
    updatedAtEnd: OffsetDateTime?,
  ): Long

  /**
   * List jobs of a connection. Pageable.
   *
   * @param configTypes - type of config, e.g. sync
   * @param configId - id of that config
   * @return lists job in descending order by created_at
   * @throws IOException - what you do when you IO
   */
  @Throws(IOException::class)
  fun listJobs(
    configTypes: Set<ConfigType>,
    configId: String?,
    limit: Int,
  ): List<Job>

  @Throws(IOException::class)
  fun listJobs(
    configTypes: Set<ConfigType>,
    jobStatuses: Set<JobStatus>?,
    configId: String?,
    pagesize: Int,
  ): List<Job>

  @Throws(IOException::class)
  fun listJobsForConvertingToEvents(
    configTypes: Set<ConfigType>,
    jobStatuses: Set<JobStatus>?,
    createdAtStart: OffsetDateTime?,
    createdAtEnd: OffsetDateTime?,
  ): List<Job>

  /**
   * List jobs based on job IDs, nothing more.
   *
   * @param jobIds the set of Job ids to list jobs for
   * @return list of jobs
   * @throws IOException you never know
   */
  @Throws(IOException::class)
  fun listJobsLight(jobIds: Set<Long>): List<Job>

  @Throws(IOException::class)
  fun listJobsLight(
    configTypes: Set<ConfigType>,
    configId: String?,
    pagesize: Int,
  ): List<Job>

  @Throws(IOException::class)
  fun listJobsLight(
    configTypes: Set<ConfigType>,
    configId: String?,
    limit: Int,
    offset: Int,
    statuses: List<JobStatus>?,
    createdAtStart: OffsetDateTime?,
    createdAtEnd: OffsetDateTime?,
    updatedAtStart: OffsetDateTime?,
    updatedAtEnd: OffsetDateTime?,
    orderByField: String?,
    orderByMethod: String?,
  ): List<Job>

  @Throws(IOException::class)
  fun listJobsLight(
    configTypes: Set<ConfigType>,
    workspaceIds: List<UUID>,
    limit: Int,
    offset: Int,
    statuses: List<JobStatus>?,
    createdAtStart: OffsetDateTime?,
    createdAtEnd: OffsetDateTime?,
    updatedAtStart: OffsetDateTime?,
    updatedAtEnd: OffsetDateTime?,
    orderByField: String?,
    orderByMethod: String?,
  ): List<Job>

  /**
   * List jobs with id.
   *
   * @param configTypes - type of config, e.g. sync
   * @param connectionId - id of the connection for which jobs should be retrieved
   * @param includingJobId - id of the job that should be the included in the list, if it exists in
   * the connection
   * @param pagesize - the pagesize that should be used when building the list (response may include
   * multiple pages)
   * @return List of jobs in descending created_at order including the specified job. Will include
   * multiple pages of jobs if required to include the specified job. If the specified job
   * does not exist in the connection, the returned list will be empty.
   */
  @Throws(IOException::class)
  fun listJobsIncludingId(
    configTypes: Set<ConfigType>,
    connectionId: String?,
    includingJobId: Long,
    pagesize: Int,
  ): List<Job>

  @Throws(IOException::class)
  fun listJobsForConnectionWithStatuses(
    connectionId: UUID,
    configTypes: Set<ConfigType>,
    statuses: Set<JobStatus>,
  ): List<Job>

  @Throws(IOException::class)
  fun listAttemptsForConnectionAfterTimestamp(
    connectionId: UUID,
    configType: ConfigType,
    attemptEndedAtTimestamp: Instant,
  ): List<AttemptWithJobInfo>

  @Throws(IOException::class)
  fun getLastReplicationJob(connectionId: UUID): Optional<Job>

  @Throws(IOException::class)
  fun getLastReplicationJobWithCancel(connectionId: UUID): Optional<Job>

  @Throws(IOException::class)
  fun getLastSyncJob(connectionId: UUID): Optional<Job>

  @Throws(IOException::class)
  fun getLastSyncJobForConnections(connectionIds: List<UUID>): List<JobStatusSummary>

  @Throws(IOException::class)
  fun getRunningSyncJobForConnections(connectionIds: List<UUID>): List<Job>

  @Throws(IOException::class)
  fun getRunningJobForConnection(connectionId: UUID): List<Job>

  @Throws(IOException::class)
  fun getFirstReplicationJob(connectionId: UUID): Optional<Job>

  @Throws(IOException::class)
  fun getVersion(): Optional<String>

  /**
   * Set the airbyte version.
   */
  @Throws(IOException::class)
  fun setVersion(airbyteVersion: String?)

  /** ARCHIVE */
  @Throws(IOException::class)
  fun getAirbyteProtocolVersionMax(): Optional<Version>

  /**
   * Set the max supported Airbyte Protocol Version.
   */
  @Throws(IOException::class)
  fun setAirbyteProtocolVersionMax(version: Version)

  @Throws(IOException::class)
  fun getAirbyteProtocolVersionMin(): Optional<Version>

  /**
   * Set the min supported Airbyte Protocol Version.
   */
  @Throws(IOException::class)
  fun setAirbyteProtocolVersionMin(version: Version)

  @Throws(IOException::class)
  fun getCurrentProtocolVersionRange(): Optional<AirbyteProtocolVersionRange>

  @Throws(IOException::class)
  fun getDeployment(): Optional<UUID>

  /**
   * Set deployment id. If one is already set, the new value is ignored.
   */
  @Throws(IOException::class)
  fun setDeployment(uuid: UUID)

  // a deployment references a setup of airbyte. it is created the first time the docker compose or
  // K8s is ready.

  /**
   * Convenience POJO for various stats data structures.
   *
   * @param combinedStats stats for the job
   * @param perStreamStats stats for each stream
   */
  @JvmRecord
  data class AttemptStats(
    @JvmField val combinedStats: SyncStats?,
    @JvmField val perStreamStats: List<StreamSyncStats>,
  )

  /**
   * Pair of the job id and attempt number.
   *
   * @param id job id
   * @param attemptNumber attempt number
   */
  @JvmRecord
  data class JobAttemptPair(
    val id: Long,
    val attemptNumber: Int,
  )
}
