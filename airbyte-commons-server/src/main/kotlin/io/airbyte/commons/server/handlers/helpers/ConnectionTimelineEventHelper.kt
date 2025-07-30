/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.model.generated.AirbyteCatalogDiff
import io.airbyte.api.model.generated.CatalogDiff
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.api.model.generated.ConnectionUpdate
import io.airbyte.api.model.generated.JobInfoRead
import io.airbyte.api.model.generated.UserReadInConnectionEvent
import io.airbyte.commons.server.JobStatus
import io.airbyte.commons.server.converters.JobConverter.Companion.getStreamsAssociatedWithJob
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Attempt
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.Job
import io.airbyte.config.JobConfigProxy
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.StreamSyncStats
import io.airbyte.config.User
import io.airbyte.config.persistence.OrganizationPersistence
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.shared.ConnectionDisabledEvent
import io.airbyte.data.services.shared.ConnectionEnabledEvent
import io.airbyte.data.services.shared.ConnectionSettingsChangedEvent
import io.airbyte.data.services.shared.FailedEvent
import io.airbyte.data.services.shared.FinalStatusEvent
import io.airbyte.data.services.shared.FinalStatusEvent.FinalStatus
import io.airbyte.data.services.shared.ManuallyStartedEvent
import io.airbyte.data.services.shared.SchemaChangeAutoPropagationEvent
import io.airbyte.data.services.shared.SchemaConfigUpdateEvent
import io.airbyte.domain.models.ConnectionId
import io.airbyte.domain.services.storage.ConnectorObjectStorageService
import io.airbyte.persistence.job.JobPersistence
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.annotation.Nullable
import jakarta.inject.Inject
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.IOException
import java.util.UUID
import java.util.stream.Collectors

/**
 * Handles connection timeline events.
 */
@Singleton
class ConnectionTimelineEventHelper
  @Inject
  constructor(
    @param:Named("airbyteSupportEmailDomains") private val airbyteSupportEmailDomains: Set<String>,
    private val currentUserService: CurrentUserService,
    private val organizationPersistence: OrganizationPersistence,
    private val permissionHandler: PermissionHandler,
    private val userPersistence: UserPersistence,
    private val connectorObjectStorageService: ConnectorObjectStorageService,
    private val connectionTimelineEventService: ConnectionTimelineEventService,
  ) {
    val currentUserIdIfExist: UUID?
      get() {
        try {
          return currentUserService.getCurrentUser().userId
        } catch (e: Exception) {
          log.info { "Unable to get current user associated with the request $e" }
          return null
        }
      }

    private fun isUserEmailFromAirbyteSupport(email: String): Boolean {
      val emailDomain = email.split("@")[1]
      return airbyteSupportEmailDomains.contains(emailDomain)
    }

    @Throws(IOException::class)
    private fun isAirbyteUser(user: User): Boolean {
      // User is an Airbyte user if:
      // 1. the user is an instance admin
      // 2. user email is from Airbyte domain(s), e.g. "airbyte.io".
      return permissionHandler.isUserInstanceAdmin(user.userId) && isUserEmailFromAirbyteSupport(user.email)
    }

    fun getUserReadInConnectionEvent(
      userId: UUID?,
      connectionId: UUID?,
    ): UserReadInConnectionEvent? {
      try {
        val res = userPersistence.getUser(userId)
        if (res.isEmpty) {
          // Deleted user
          return UserReadInConnectionEvent()
            .isDeleted(true)
        }
        val user = res.get()
        // Check if this event was triggered by an Airbyter Support.
        if (isAirbyteUser(user)) {
          // Check if this connection is in external customers workspaces.
          // 1. get the associated organization
          val organization = organizationPersistence.getOrganizationByConnectionId(connectionId).orElseThrow()
          // 2. check the email of the organization owner
          if (!isUserEmailFromAirbyteSupport(organization.email)) {
            // Airbyters took an action in customer's workspaces. Obfuscate Airbyter's real name.
            return UserReadInConnectionEvent()
              .id(user.userId)
              .name(AIRBYTE_SUPPORT_USER_NAME)
          }
        }
        return UserReadInConnectionEvent()
          .id(user.userId)
          .name(user.name)
          .email(user.email)
      } catch (e: Exception) {
        log.error(e) { "Error while retrieving user information." }
        return null
      }
    }

    @JvmRecord
    data class TimelineJobStats(
      @JvmField val loadedBytes: Long,
      @JvmField val loadedRecords: Long,
      @JvmField val rejectedRecords: Long,
    )

    @VisibleForTesting
    fun buildTimelineJobStats(
      job: Job,
      attemptStats: List<JobPersistence.AttemptStats>,
    ): TimelineJobStats {
      val configuredCatalog = JobConfigProxy(job.config).configuredCatalog
      val streams = configuredCatalog?.streams ?: listOf()

      var bytesLoaded: Long = 0
      var recordsLoaded: Long = 0
      var recordsRejected: Long = 0

      for ((currentStream, syncMode) in streams) {
        val streamStats =
          attemptStats
            .stream()
            .flatMap { a: JobPersistence.AttemptStats ->
              a.perStreamStats
                .stream()
                .filter { o: StreamSyncStats ->
                  currentStream.name == o.streamName &&
                    (
                      (currentStream.namespace == null && o.streamNamespace == null) ||
                        (currentStream.namespace != null && currentStream.namespace == o.streamNamespace)
                    )
                }
            }.collect(Collectors.toList())
        if (!streamStats.isEmpty()) {
          val records = StatsAggregationHelper.getAggregatedStats(syncMode, streamStats)
          recordsLoaded += records.recordsCommitted
          bytesLoaded += records.bytesCommitted
          recordsRejected += records.recordsRejected
        }
      }
      return TimelineJobStats(bytesLoaded, recordsLoaded, recordsRejected)
    }

    fun logJobSuccessEventInConnectionTimeline(
      job: Job,
      connectionId: UUID,
      attemptStats: List<JobPersistence.AttemptStats>,
    ) {
      try {
        val stats = buildTimelineJobStats(job, attemptStats)
        val event =
          FinalStatusEvent(
            job.id,
            job.createdAtInSecond,
            job.updatedAtInSecond,
            stats.loadedBytes,
            stats.loadedRecords,
            stats.rejectedRecords,
            job.getAttemptsCount(),
            job.configType.name,
            JobStatus.SUCCEEDED.name,
            getStreamsAssociatedWithJob(job),
            connectorObjectStorageService.getRejectedRecordsForJob(ConnectionId(connectionId), job, stats.rejectedRecords),
          )
        connectionTimelineEventService.writeEvent(connectionId, event, null)
      } catch (e: Exception) {
        log.error(e) { "Failed to persist timeline event for job: $job.id" }
      }
    }

    fun logJobFailureEventInConnectionTimeline(
      job: Job,
      connectionId: UUID,
      attemptStats: List<JobPersistence.AttemptStats>,
    ) {
      try {
        val stats = buildTimelineJobStats(job, attemptStats)

        val lastAttemptFailureSummary = job.getLastAttempt().flatMap { obj: Attempt -> obj.getFailureSummary() }
        val firstFailureReasonOfLastAttempt =
          lastAttemptFailureSummary.flatMap { summary: AttemptFailureSummary ->
            summary.failures.stream().findFirst()
          }
        val jobEventFailureStatus = if (stats.loadedBytes > 0) FinalStatus.INCOMPLETE.name else FinalStatus.FAILED.name
        val event =
          FailedEvent(
            job.id,
            job.createdAtInSecond,
            job.updatedAtInSecond,
            stats.loadedBytes,
            stats.loadedRecords,
            stats.rejectedRecords,
            job.getAttemptsCount(),
            job.configType.name,
            jobEventFailureStatus,
            getStreamsAssociatedWithJob(job),
            connectorObjectStorageService.getRejectedRecordsForJob(ConnectionId(connectionId), job, stats.rejectedRecords),
            firstFailureReasonOfLastAttempt,
          )
        connectionTimelineEventService.writeEvent(connectionId, event, null)
      } catch (e: Exception) {
        log.error(e) { "Failed to persist timeline event for job: $job.id" }
      }
    }

    fun logJobCancellationEventInConnectionTimeline(
      job: Job,
      attemptStats: List<JobPersistence.AttemptStats>,
    ) {
      try {
        val stats = buildTimelineJobStats(job, attemptStats)
        val connectionId = UUID.fromString(job.scope)
        val event =
          FinalStatusEvent(
            job.id,
            job.createdAtInSecond,
            job.updatedAtInSecond,
            stats.loadedBytes,
            stats.loadedRecords,
            stats.rejectedRecords,
            job.getAttemptsCount(),
            job.configType.name,
            io.airbyte.config.JobStatus.CANCELLED.name,
            getStreamsAssociatedWithJob(job),
            connectorObjectStorageService.getRejectedRecordsForJob(ConnectionId(connectionId), job, stats.rejectedRecords),
          )
        connectionTimelineEventService.writeEvent(connectionId, event, currentUserIdIfExist)
      } catch (e: Exception) {
        log.error(e) { "Failed to persist job cancelled event for job: $job.id" }
      }
    }

    fun logManuallyStartedEventInConnectionTimeline(
      connectionId: UUID,
      jobInfo: JobInfoRead?,
      streams: List<StreamDescriptor>?,
    ) {
      try {
        if (jobInfo != null && jobInfo.job != null && jobInfo.job.configType != null) {
          val event =
            ManuallyStartedEvent(
              jobInfo.job.id,
              jobInfo.job.createdAt,
              jobInfo.job.configType.name,
              streams,
            )
          connectionTimelineEventService.writeEvent(connectionId, event, currentUserIdIfExist)
        }
      } catch (e: Exception) {
        log.error(e) { "Failed to persist job started event for job: $jobInfo!!.job.id" }
      }
    }

    /**
     * When source schema change is detected and auto-propagated to all connections, we log the event in
     * each connection.
     */
    fun logSchemaChangeAutoPropagationEventInConnectionTimeline(
      connectionId: UUID,
      diff: CatalogDiff,
    ) {
      try {
        if (diff.transforms == null || diff.transforms.isEmpty()) {
          log.info { "Diff is empty. Bypassing logging an event." }
          return
        }
        log.info { "Persisting source schema change auto-propagated event for connection: {} with diff: $connectionId, diff" }
        val event = SchemaChangeAutoPropagationEvent(diff)
        connectionTimelineEventService.writeEvent(connectionId, event, null)
      } catch (e: Exception) {
        log.error(e) { "Failed to persist source schema change auto-propagated event for connection: $connectionId" }
      }
    }

    fun logSchemaConfigChangeEventInConnectionTimeline(
      connectionId: UUID,
      airbyteCatalogDiff: AirbyteCatalogDiff,
    ) {
      try {
        log.debug { "Persisting schema config change event for connection: {} with diff: $connectionId, airbyteCatalogDiff" }
        val event = SchemaConfigUpdateEvent(airbyteCatalogDiff)
        connectionTimelineEventService.writeEvent(connectionId, event, currentUserIdIfExist)
      } catch (e: Exception) {
        log.error(e) { "Failed to persist schema config change event for connection: $connectionId" }
      }
    }

    private fun addPatchIfFieldIsChanged(
      patches: MutableMap<String, Map<String, Any?>>,
      fieldName: String,
      oldValue: Any?,
      newValue: Any?,
    ) {
      if (newValue != null && newValue != oldValue) {
        val patchMap: MutableMap<String, Any?> = HashMap()
        // oldValue could be null
        patchMap["from"] = oldValue
        patchMap["to"] = newValue
        patches[fieldName] = patchMap
      }
    }

    fun logStatusChangedEventInConnectionTimeline(
      connectionId: UUID,
      status: ConnectionStatus?,
      @Nullable updateReason: String?,
      autoUpdate: Boolean,
    ) {
      try {
        if (status != null) {
          if (status == ConnectionStatus.ACTIVE) {
            val event = ConnectionEnabledEvent()
            connectionTimelineEventService.writeEvent(connectionId, event, if (autoUpdate) null else currentUserIdIfExist)
          } else if (status == ConnectionStatus.INACTIVE) {
            val event = ConnectionDisabledEvent(updateReason)
            connectionTimelineEventService.writeEvent(connectionId, event, if (autoUpdate) null else currentUserIdIfExist)
          }
        }
      } catch (e: Exception) {
        log.error(e) { "Failed to persist status changed event for connection: $connectionId" }
      }
    }

    fun logConnectionSettingsChangedEventInConnectionTimeline(
      connectionId: UUID,
      originalConnectionRead: ConnectionRead,
      patch: ConnectionUpdate,
      updateReason: String?,
      autoUpdate: Boolean,
    ) {
      try {
        // Note: if status is changed with other settings changes,we are logging them as separate events.
        // 1. log event for connection status changes
        logStatusChangedEventInConnectionTimeline(connectionId, patch.status, updateReason, autoUpdate)
        // 2. log event for other connection settings changes
        val patches: MutableMap<String, Map<String, Any?>> = HashMap()
        addPatchIfFieldIsChanged(patches, "scheduleType", originalConnectionRead.scheduleType, patch.scheduleType)
        addPatchIfFieldIsChanged(patches, "scheduleData", originalConnectionRead.scheduleData, patch.scheduleData)
        addPatchIfFieldIsChanged(patches, "name", originalConnectionRead.name, patch.name)
        addPatchIfFieldIsChanged(patches, "namespaceDefinition", originalConnectionRead.namespaceDefinition, patch.namespaceDefinition)
        addPatchIfFieldIsChanged(patches, "namespaceFormat", originalConnectionRead.namespaceFormat, patch.namespaceFormat)
        addPatchIfFieldIsChanged(patches, "prefix", originalConnectionRead.prefix, patch.prefix)
        addPatchIfFieldIsChanged(patches, "resourceRequirements", originalConnectionRead.resourceRequirements, patch.resourceRequirements)
        addPatchIfFieldIsChanged(patches, "dataplaneGroupId", originalConnectionRead.dataplaneGroupId, patch.dataplaneGroupId)
        addPatchIfFieldIsChanged(patches, "notifySchemaChanges", originalConnectionRead.notifySchemaChanges, patch.notifySchemaChanges)
        addPatchIfFieldIsChanged(
          patches,
          "notifySchemaChangesByEmail",
          originalConnectionRead.notifySchemaChangesByEmail,
          patch.notifySchemaChangesByEmail,
        )
        addPatchIfFieldIsChanged(
          patches,
          "nonBreakingChangesPreference",
          originalConnectionRead.nonBreakingChangesPreference,
          patch.nonBreakingChangesPreference,
        )
        addPatchIfFieldIsChanged(patches, "backfillPreference", originalConnectionRead.backfillPreference, patch.backfillPreference)
        if (!patches.isEmpty()) {
          val event =
            ConnectionSettingsChangedEvent(
              patches,
              updateReason,
            )
          connectionTimelineEventService.writeEvent(connectionId, event, if (autoUpdate) null else currentUserIdIfExist)
        }
      } catch (e: Exception) {
        log.error(e) { "Failed to persist connection settings changed event for connection: $connectionId" }
      }
    }

    companion object {
      private val log = KotlinLogging.logger {}
      const val AIRBYTE_SUPPORT_USER_NAME: String = "Airbyte Support"
    }
  }
