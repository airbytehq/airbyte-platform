/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.model.generated.AttemptStats
import io.airbyte.api.model.generated.AttemptStreamStats
import io.airbyte.api.model.generated.CreateNewAttemptNumberResponse
import io.airbyte.api.model.generated.InternalOperationResult
import io.airbyte.api.model.generated.SaveAttemptSyncConfigRequestBody
import io.airbyte.api.model.generated.SaveStatsRequestBody
import io.airbyte.api.model.generated.SaveStreamAttemptMetadataRequestBody
import io.airbyte.api.model.generated.StreamAttemptMetadata
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.logging.DEFAULT_LOG_FILENAME
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.converters.JobConverter
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.errors.IdNotFoundKnownException
import io.airbyte.commons.server.errors.UnprocessableContentException
import io.airbyte.commons.server.handlers.helpers.JobCreationAndStatusUpdateHelper
import io.airbyte.commons.temporal.TemporalUtils.Companion.getJobRoot
import io.airbyte.config.Attempt
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobConfigProxy
import io.airbyte.config.JobOutput
import io.airbyte.config.RefreshStream
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.StreamSyncStats
import io.airbyte.config.SyncMode
import io.airbyte.config.SyncStats
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.StatePersistence
import io.airbyte.config.persistence.helper.GenerationBumper
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.StreamAttemptMetadataService
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.EnableResumableFullRefresh
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.IOException
import java.nio.file.Path
import java.util.UUID
import java.util.stream.Collectors

private val log = KotlinLogging.logger {}

/**
 * AttemptHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
open class AttemptHandler(
  private val jobPersistence: JobPersistence,
  private val statePersistence: StatePersistence,
  private val jobConverter: JobConverter,
  private val featureFlagClient: FeatureFlagClient,
  private val jobCreationAndStatusUpdateHelper: JobCreationAndStatusUpdateHelper,
  @param:Named("workspaceRoot") private val workspaceRoot: Path,
  private val generationBumper: GenerationBumper,
  private val connectionService: ConnectionService,
  private val destinationService: DestinationService,
  private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
  private val streamAttemptMetadataService: StreamAttemptMetadataService,
  private val apiPojoConverters: ApiPojoConverters,
) {
  @Throws(
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun createNewAttemptNumber(jobId: Long): CreateNewAttemptNumberResponse {
    val job: Job
    try {
      job = jobPersistence.getJob(jobId)
    } catch (e: RuntimeException) {
      throw UnprocessableContentException(String.format("Could not find jobId: %s", jobId), e)
    }

    val jobRoot = getJobRoot(workspaceRoot, jobId.toString(), job.getAttemptsCount().toLong())
    val logFilePath: Path = jobRoot.resolve(DEFAULT_LOG_FILENAME)
    val persistedAttemptNumber = jobPersistence.createAttempt(jobId, logFilePath)

    val connectionId = UUID.fromString(job.scope)
    val standardSync = connectionService.getStandardSync(connectionId)
    val standardDestinationDefinition =
      destinationService.getDestinationDefinitionFromDestination(standardSync.destinationId)
    val destinationConnection = destinationService.getDestinationConnection(standardSync.destinationId)
    val destinationVersion =
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.workspaceId,
        destinationConnection.destinationId,
      )
    val supportRefreshes = destinationVersion.supportsRefreshes

    // Action done in the first attempt
    if (persistedAttemptNumber == 0) {
      updateGenerationAndStateForFirstAttempt(job, connectionId, supportRefreshes)
    } else {
      updateGenerationAndStateForSubsequentAttempts(job, supportRefreshes)
    }

    jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.ATTEMPT_CREATED_BY_RELEASE_STAGE, job)
    jobCreationAndStatusUpdateHelper.emitAttemptCreatedEvent(job, persistedAttemptNumber)

    return CreateNewAttemptNumberResponse().attemptNumber(persistedAttemptNumber)
  }

  @VisibleForTesting
  @Throws(IOException::class)
  fun updateGenerationAndStateForSubsequentAttempts(
    job: Job,
    supportRefreshes: Boolean,
  ) {
    // We cannot easily do this in a transaction as the attempt and state tables are in separate logical
    // databases.
    val enableResumableFullRefresh = featureFlagClient.boolVariation(EnableResumableFullRefresh, Connection(job.scope))
    val evaluateResumableFlag = enableResumableFullRefresh && supportRefreshes
    val stateToClear =
      getFullRefreshStreamsToClear(
        JobConfigProxy(job.config).configuredCatalog!!,
        job.id,
        evaluateResumableFlag,
      )

    generationBumper.updateGenerationForStreams(UUID.fromString(job.scope), job.id, listOf(), stateToClear)
    if (!stateToClear.isEmpty()) {
      statePersistence.bulkDelete(UUID.fromString(job.scope), stateToClear)
    }
  }

  @VisibleForTesting
  @Throws(IOException::class)
  fun updateGenerationAndStateForFirstAttempt(
    job: Job,
    connectionId: UUID,
    supportRefreshes: Boolean,
  ) {
    if (job.configType == JobConfig.ConfigType.REFRESH) {
      check(supportRefreshes) { "Trying to create a refresh attempt for a destination which doesn't support refreshes" }
      val streamToReset = getFullRefresh(job.config.refresh.configuredAirbyteCatalog, true).toMutableSet()
      streamToReset.addAll(
        job.config.refresh.streamsToRefresh
          .stream()
          .map { obj: RefreshStream -> obj.streamDescriptor }
          .collect(Collectors.toSet()),
      )
      generationBumper.updateGenerationForStreams(connectionId, job.id, listOf(), streamToReset)

      statePersistence.bulkDelete(UUID.fromString(job.scope), streamToReset)
    } else if (job.configType == JobConfig.ConfigType.SYNC) {
      val fullRefreshes = getFullRefresh(job.config.sync.configuredAirbyteCatalog, true)
      generationBumper.updateGenerationForStreams(connectionId, job.id, listOf(), fullRefreshes)

      statePersistence.bulkDelete(UUID.fromString(job.scope), fullRefreshes)
    } else if (job.configType == JobConfig.ConfigType.CLEAR || job.configType == JobConfig.ConfigType.RESET_CONNECTION) {
      val resetStreams = java.util.Set.copyOf(job.config.resetConnection.resetSourceConfiguration.streamsToReset)

      generationBumper.updateGenerationForStreams(connectionId, job.id, listOf(), resetStreams)
      statePersistence.bulkDelete(UUID.fromString(job.scope), resetStreams)
    }
  }

  @VisibleForTesting
  fun getFullRefresh(
    catalog: ConfiguredAirbyteCatalog,
    supportResumableFullRefresh: Boolean,
  ): Set<StreamDescriptor> {
    if (!supportResumableFullRefresh) {
      return setOf()
    }

    return catalog.streams
      .stream()
      .filter { stream: ConfiguredAirbyteStream -> stream.syncMode == SyncMode.FULL_REFRESH }
      .map { stream: ConfiguredAirbyteStream ->
        StreamDescriptor()
          .withName(stream.stream.name)
          .withNamespace(stream.stream.namespace)
      }.collect(Collectors.toSet())
  }

  @VisibleForTesting
  fun getFullRefreshStreamsToClear(
    catalog: ConfiguredAirbyteCatalog,
    id: Long,
    excludeResumableStreams: Boolean,
  ): Set<StreamDescriptor> {
    if (catalog == null) {
      throw BadRequestException("Missing configured catalog for job: $id")
    }
    val configuredStreams =
      catalog.streams
        ?: throw BadRequestException("Missing configured catalog stream for job: $id")

    return configuredStreams
      .stream()
      .filter { s: ConfiguredAirbyteStream ->
        if (s.syncMode == SyncMode.FULL_REFRESH) {
          if (excludeResumableStreams) {
            return@filter s.stream.isResumable == null || !s.stream.isResumable!!
          } else {
            return@filter true
          }
        } else {
          return@filter false
        }
      }.map { s: ConfiguredAirbyteStream -> StreamDescriptor().withName(s.stream.name).withNamespace(s.stream.namespace) }
      .collect(Collectors.toSet())
  }

  @Throws(IOException::class)
  fun getAttemptForJob(
    jobId: Long,
    attemptNo: Int,
  ): Attempt {
    val read = jobPersistence.getAttemptForJob(jobId, attemptNo)

    if (read.isEmpty) {
      throw IdNotFoundKnownException(
        String.format("Could not find attempt for job_id: %d and attempt no: %d", jobId, attemptNo),
        String.format("%d_%d", jobId, attemptNo),
      )
    }

    return read.get()
  }

  @Throws(IOException::class)
  fun getAttemptCombinedStats(
    jobId: Long,
    attemptNo: Int,
  ): AttemptStats {
    val stats =
      jobPersistence.getAttemptCombinedStats(jobId, attemptNo)
        ?: throw IdNotFoundKnownException(
          String.format("Could not find attempt stats for job_id: %d and attempt no: %d", jobId, attemptNo),
          String.format("%d_%d", jobId, attemptNo),
        )

    return AttemptStats()
      .recordsEmitted(stats.recordsEmitted)
      .bytesEmitted(stats.bytesEmitted)
      .bytesCommitted(stats.bytesCommitted)
      .recordsCommitted(stats.recordsCommitted)
      .recordsRejected(stats.recordsRejected)
      .estimatedRecords(stats.estimatedRecords)
      .estimatedBytes(stats.estimatedBytes)
  }

  fun saveStats(requestBody: SaveStatsRequestBody): InternalOperationResult {
    try {
      val stats = requestBody.stats
      val streamStats =
        requestBody.streamStats
          .stream()
          .map { s: AttemptStreamStats ->
            StreamSyncStats()
              .withStreamName(s.streamName)
              .withStreamNamespace(s.streamNamespace)
              .withStats(
                SyncStats()
                  .withBytesEmitted(s.stats.bytesEmitted)
                  .withRecordsEmitted(s.stats.recordsEmitted)
                  .withBytesCommitted(s.stats.bytesCommitted)
                  .withRecordsCommitted(s.stats.recordsCommitted)
                  .withRecordsRejected(s.stats.recordsRejected)
                  .withEstimatedBytes(s.stats.estimatedBytes)
                  .withEstimatedRecords(s.stats.estimatedRecords),
              )
          }.collect(Collectors.toList())

      jobPersistence.writeStats(
        requestBody.jobId,
        requestBody.attemptNumber,
        stats.estimatedRecords,
        stats.estimatedBytes,
        stats.recordsEmitted,
        stats.bytesEmitted,
        stats.recordsCommitted,
        stats.bytesCommitted,
        stats.recordsRejected,
        requestBody.connectionId,
        streamStats,
      )
    } catch (ioe: IOException) {
      log.error(ioe) { "IOException when setting temporal workflow in attempt;" }
      return InternalOperationResult().succeeded(false)
    }

    return InternalOperationResult().succeeded(true)
  }

  fun saveStreamMetadata(requestBody: SaveStreamAttemptMetadataRequestBody): InternalOperationResult {
    try {
      streamAttemptMetadataService.upsertStreamAttemptMetadata(
        requestBody.jobId,
        requestBody.attemptNumber.toLong(),
        requestBody.streamMetadata
          .stream()
          .map { s: StreamAttemptMetadata ->
            io.airbyte.data.services.StreamAttemptMetadata(
              s.streamName,
              s.streamNamespace,
              s.wasBackfilled,
              s.wasResumed,
            )
          }.toList(),
      )
      return InternalOperationResult().succeeded(true)
    } catch (e: Exception) {
      log.error(e) { "failed to save steam metadata for job: ${requestBody.jobId} attempt: ${requestBody.attemptNumber}" }
      return InternalOperationResult().succeeded(false)
    }
  }

  fun saveSyncConfig(requestBody: SaveAttemptSyncConfigRequestBody): InternalOperationResult {
    try {
      jobPersistence.writeAttemptSyncConfig(
        requestBody.jobId,
        requestBody.attemptNumber,
        apiPojoConverters.attemptSyncConfigToInternal(requestBody.syncConfig),
      )
    } catch (ioe: IOException) {
      log.error(ioe) { "IOException when saving AttemptSyncConfig for attempt;" }
      return InternalOperationResult().succeeded(false)
    }
    return InternalOperationResult().succeeded(true)
  }

  @Throws(IOException::class)
  fun failAttempt(
    attemptNumber: Int,
    jobId: Long,
    rawFailureSummary: Any?,
    rawSyncOutput: Any?,
  ) {
    var failureSummary: AttemptFailureSummary? = null
    if (rawFailureSummary != null) {
      try {
        failureSummary = Jsons.convertValue(rawFailureSummary, AttemptFailureSummary::class.java)
      } catch (e: Exception) {
        throw BadRequestException("Unable to parse failureSummary.", e)
      }
    }
    var output: StandardSyncOutput? = null
    if (rawSyncOutput != null) {
      try {
        output = Jsons.convertValue(rawSyncOutput, StandardSyncOutput::class.java)
      } catch (e: Exception) {
        throw BadRequestException("Unable to parse standardSyncOutput.", e)
      }
    }

    jobCreationAndStatusUpdateHelper.traceFailures(failureSummary)

    jobPersistence.failAttempt(jobId, attemptNumber)
    jobPersistence.writeAttemptFailureSummary(jobId, attemptNumber, failureSummary)

    if (output != null) {
      val jobOutput = JobOutput().withSync(output)
      jobPersistence.writeOutput(jobId, attemptNumber, jobOutput)
    }

    val job = jobPersistence.getJob(jobId)
    jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.ATTEMPT_FAILED_BY_RELEASE_STAGE, job)
    jobCreationAndStatusUpdateHelper.emitAttemptCompletedEventIfAttemptPresent(job)
    jobCreationAndStatusUpdateHelper.trackFailures(failureSummary)
  }
}
