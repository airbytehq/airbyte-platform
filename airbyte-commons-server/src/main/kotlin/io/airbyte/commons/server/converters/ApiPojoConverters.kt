/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import io.airbyte.api.model.generated.ActorType
import io.airbyte.api.model.generated.AttemptSyncConfig
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionSchedule
import io.airbyte.api.model.generated.ConnectionScheduleData
import io.airbyte.api.model.generated.ConnectionScheduleDataBasicSchedule
import io.airbyte.api.model.generated.ConnectionScheduleDataCron
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.model.generated.ConnectionStateType
import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.api.model.generated.DeadlineAction
import io.airbyte.api.model.generated.JobType
import io.airbyte.api.model.generated.JobTypeResourceLimit
import io.airbyte.api.model.generated.NamespaceDefinitionType
import io.airbyte.api.model.generated.NonBreakingChangesPreference
import io.airbyte.api.model.generated.ResourceRequirements
import io.airbyte.api.model.generated.SchemaChangeBackfillPreference
import io.airbyte.api.model.generated.ScopedResourceRequirements
import io.airbyte.api.model.generated.SupportState
import io.airbyte.commons.converters.StateConverter.toApi
import io.airbyte.commons.converters.StateConverter.toInternal
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.BasicSchedule
import io.airbyte.config.ReleaseStage
import io.airbyte.config.Schedule
import io.airbyte.config.StandardSync
import io.airbyte.config.StateWrapper
import io.airbyte.config.SupportLevel
import io.airbyte.config.Tag
import io.airbyte.config.helpers.StateMessageHelper.getState
import io.airbyte.config.helpers.StateMessageHelper.getTypedState
import jakarta.inject.Singleton
import jakarta.validation.Valid
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Date
import java.util.Optional
import java.util.UUID
import java.util.stream.Collectors

/**
 * Convert between API and internal versions of airbyte models.
 */
@Singleton
class ApiPojoConverters(
  private val catalogConverter: CatalogConverter,
) {
  fun scopedResourceReqsToInternal(scopedResourceReqs: ScopedResourceRequirements?): io.airbyte.config.ScopedResourceRequirements? {
    if (scopedResourceReqs == null) {
      return null
    }

    return io.airbyte.config
      .ScopedResourceRequirements()
      .withDefault(if (scopedResourceReqs.default == null) null else this.resourceRequirementsToInternal(scopedResourceReqs.default))
      .withJobSpecific(
        if (scopedResourceReqs.jobSpecific == null) {
          null
        } else {
          scopedResourceReqs.jobSpecific
            .stream()
            .map { jobSpecific: JobTypeResourceLimit ->
              io.airbyte.config
                .JobTypeResourceLimit()
                .withJobType(this.toInternalJobType(jobSpecific.jobType))
                .withResourceRequirements(this.resourceRequirementsToInternal(jobSpecific.resourceRequirements))
            }.collect(Collectors.toList())
        },
      )
  }

  fun attemptSyncConfigToInternal(attemptSyncConfig: AttemptSyncConfig?): io.airbyte.config.AttemptSyncConfig? {
    if (attemptSyncConfig == null) {
      return null
    }

    val internalAttemptSyncConfig =
      io.airbyte.config
        .AttemptSyncConfig()
        .withSourceConfiguration(attemptSyncConfig.sourceConfiguration)
        .withDestinationConfiguration(attemptSyncConfig.destinationConfiguration)

    val connectionState = attemptSyncConfig.state
    if (connectionState != null && connectionState.stateType != ConnectionStateType.NOT_SET) {
      val stateWrapper = toInternal(attemptSyncConfig.state)
      val state = getState(stateWrapper)
      internalAttemptSyncConfig.state = state
    }

    return internalAttemptSyncConfig
  }

  fun attemptSyncConfigToApi(
    attemptSyncConfig: io.airbyte.config.AttemptSyncConfig?,
    connectionId: UUID,
  ): AttemptSyncConfig? {
    if (attemptSyncConfig == null) {
      return null
    }

    val state = attemptSyncConfig.state
    val optStateWrapper: Optional<StateWrapper> = if (state != null) getTypedState(state.state) else Optional.empty()

    return AttemptSyncConfig()
      .sourceConfiguration(attemptSyncConfig.sourceConfiguration)
      .destinationConfiguration(attemptSyncConfig.destinationConfiguration)
      .state(toApi(connectionId, optStateWrapper.orElse(null)))
  }

  fun scopedResourceReqsToApi(scopedResourceReqs: io.airbyte.config.ScopedResourceRequirements?): ScopedResourceRequirements? {
    if (scopedResourceReqs == null) {
      return null
    }

    return ScopedResourceRequirements()
      ._default(if (scopedResourceReqs.default == null) null else this.resourceRequirementsToApi(scopedResourceReqs.default))
      .jobSpecific(
        if (scopedResourceReqs.jobSpecific == null) {
          null
        } else {
          scopedResourceReqs.jobSpecific
            .stream()
            .map { jobSpecific: io.airbyte.config.JobTypeResourceLimit ->
              JobTypeResourceLimit()
                .jobType(this.toApiJobType(jobSpecific.jobType))
                .resourceRequirements(this.resourceRequirementsToApi(jobSpecific.resourceRequirements))
            }.collect(Collectors.toList<@Valid JobTypeResourceLimit?>())
        },
      )
  }

  fun resourceRequirementsToInternal(resourceReqs: ResourceRequirements?): io.airbyte.config.ResourceRequirements? {
    if (resourceReqs == null) {
      return null
    }

    return io.airbyte.config
      .ResourceRequirements()
      .withCpuRequest(resourceReqs.cpuRequest)
      .withCpuLimit(resourceReqs.cpuLimit)
      .withMemoryRequest(resourceReqs.memoryRequest)
      .withMemoryLimit(resourceReqs.memoryLimit)
  }

  fun resourceRequirementsToApi(resourceReqs: io.airbyte.config.ResourceRequirements?): ResourceRequirements? {
    if (resourceReqs == null) {
      return null
    }

    return ResourceRequirements()
      .cpuRequest(resourceReqs.cpuRequest)
      .cpuLimit(resourceReqs.cpuLimit)
      .memoryRequest(resourceReqs.memoryRequest)
      .memoryLimit(resourceReqs.memoryLimit)
  }

  fun internalToConnectionRead(standardSync: StandardSync): ConnectionRead {
    val connectionRead =
      ConnectionRead()
        .connectionId(standardSync.connectionId)
        .sourceId(standardSync.sourceId)
        .destinationId(standardSync.destinationId)
        .operationIds(standardSync.operationIds)
        .status(this.toApiStatus(standardSync.status))
        .name(standardSync.name)
        .namespaceDefinition(standardSync.namespaceDefinition?.convertTo<NamespaceDefinitionType>())
        .namespaceFormat(standardSync.namespaceFormat)
        .prefix(standardSync.prefix)
        .syncCatalog(catalogConverter.toApi(standardSync.catalog, standardSync.fieldSelectionData))
        .sourceCatalogId(standardSync.sourceCatalogId)
        .destinationCatalogId(standardSync.destinationCatalogId)
        .breakingChange(standardSync.breakingChange)
        .nonBreakingChangesPreference(standardSync.nonBreakingChangesPreference?.convertTo<NonBreakingChangesPreference>())
        .backfillPreference(standardSync.backfillPreference?.convertTo<SchemaChangeBackfillPreference>())
        .notifySchemaChanges(standardSync.notifySchemaChanges)
        .createdAt(standardSync.createdAt)
        .notifySchemaChangesByEmail(standardSync.notifySchemaChangesByEmail)
        .tags(
          standardSync.tags
            .stream()
            .map { tag: Tag -> this.toApiTag(tag) }
            .toList(),
        )

    if (standardSync.resourceRequirements != null) {
      connectionRead.resourceRequirements(this.resourceRequirementsToApi(standardSync.resourceRequirements))
    }

    this.populateConnectionReadSchedule(standardSync, connectionRead)

    return connectionRead
  }

  fun toApiJobType(jobType: io.airbyte.config.JobTypeResourceLimit.JobType): JobType? = jobType?.convertTo<JobType>()

  fun toInternalJobType(jobType: JobType): io.airbyte.config.JobTypeResourceLimit.JobType? =
    jobType?.convertTo<io.airbyte.config.JobTypeResourceLimit.JobType>()

  fun toApiTag(tag: Tag): io.airbyte.api.model.generated.Tag =
    io.airbyte.api.model.generated
      .Tag()
      .name(tag.name)
      .tagId(tag.tagId)
      .workspaceId(tag.workspaceId)
      .color(tag.color)

  fun toInternalTag(tag: io.airbyte.api.model.generated.Tag): Tag =
    Tag()
      .withName(tag.name)
      .withTagId(tag.tagId)
      .withWorkspaceId(tag.workspaceId)
      .withColor(tag.color)

  fun toInternalActorType(actorType: ActorType): io.airbyte.config.ActorType? = actorType?.convertTo<io.airbyte.config.ActorType>()

  // TODO(https://github.com/airbytehq/airbyte/issues/11432): remove these helpers.
  fun toApiTimeUnit(apiTimeUnit: Schedule.TimeUnit?): ConnectionSchedule.TimeUnitEnum? = apiTimeUnit?.convertTo<ConnectionSchedule.TimeUnitEnum>()

  fun toApiTimeUnit(timeUnit: BasicSchedule.TimeUnit?): ConnectionSchedule.TimeUnitEnum? = timeUnit?.convertTo<ConnectionSchedule.TimeUnitEnum>()

  fun toApiStatus(status: StandardSync.Status?): ConnectionStatus? = status?.convertTo<ConnectionStatus>()

  fun toPersistenceStatus(apiStatus: ConnectionStatus?): StandardSync.Status? = apiStatus?.convertTo<StandardSync.Status>()

  fun toPersistenceNonBreakingChangesPreference(preference: NonBreakingChangesPreference?): StandardSync.NonBreakingChangesPreference? =
    preference?.convertTo<StandardSync.NonBreakingChangesPreference>()

  fun toPersistenceBackfillPreference(preference: SchemaChangeBackfillPreference?): StandardSync.BackfillPreference? =
    preference?.convertTo<StandardSync.BackfillPreference>()

  fun toPersistenceTimeUnit(apiTimeUnit: ConnectionSchedule.TimeUnitEnum?): Schedule.TimeUnit? = apiTimeUnit?.convertTo<Schedule.TimeUnit>()

  fun toBasicScheduleTimeUnit(apiTimeUnit: ConnectionSchedule.TimeUnitEnum?): BasicSchedule.TimeUnit? =
    apiTimeUnit?.convertTo<BasicSchedule.TimeUnit>()

  fun toBasicScheduleTimeUnit(apiTimeUnit: ConnectionScheduleDataBasicSchedule.TimeUnitEnum?): BasicSchedule.TimeUnit? =
    apiTimeUnit?.convertTo<BasicSchedule.TimeUnit>()

  fun toLegacyScheduleTimeUnit(timeUnit: ConnectionScheduleDataBasicSchedule.TimeUnitEnum?): Schedule.TimeUnit? =
    timeUnit?.convertTo<Schedule.TimeUnit>()

  fun toApiReleaseStage(releaseStage: ReleaseStage?): io.airbyte.api.model.generated.ReleaseStage? {
    if (releaseStage == null) {
      return null
    }
    return io.airbyte.api.model.generated.ReleaseStage
      .fromValue(releaseStage.value())
  }

  fun toApiSupportLevel(supportLevel: SupportLevel?): io.airbyte.api.model.generated.SupportLevel {
    if (supportLevel == null) {
      return io.airbyte.api.model.generated.SupportLevel.NONE
    }
    return io.airbyte.api.model.generated.SupportLevel
      .fromValue(supportLevel.value())
  }

  fun toApiSupportState(supportState: ActorDefinitionVersion.SupportState?): SupportState? {
    if (supportState == null) {
      return null
    }
    return SupportState.fromValue(supportState.value())
  }

  fun toApiBreakingChange(breakingChange: ActorDefinitionBreakingChange?): io.airbyte.api.model.generated.ActorDefinitionBreakingChange? {
    if (breakingChange == null) {
      return null
    }
    val deadlineAction =
      if (breakingChange.deadlineAction == DeadlineAction.AUTO_UPGRADE.toString()) {
        DeadlineAction.AUTO_UPGRADE
      } else {
        DeadlineAction.DISABLE
      }
    return io.airbyte.api.model.generated
      .ActorDefinitionBreakingChange()
      .version(breakingChange.version.serialize())
      .message(breakingChange.message)
      .migrationDocumentationUrl(breakingChange.migrationDocumentationUrl)
      .upgradeDeadline(this.toLocalDate(breakingChange.upgradeDeadline))
      .deadlineAction(deadlineAction)
  }

  fun toLocalDate(date: String?): LocalDate? {
    if (date == null || date.isBlank()) {
      return null
    }
    return LocalDate.parse(date)
  }

  fun toOffsetDateTime(date: Date?): OffsetDateTime? {
    if (date == null) {
      return null
    }
    return date.toInstant().atOffset(ZoneOffset.UTC)
  }

  fun toApiBasicScheduleTimeUnit(timeUnit: BasicSchedule.TimeUnit?): ConnectionScheduleDataBasicSchedule.TimeUnitEnum? =
    timeUnit?.convertTo<ConnectionScheduleDataBasicSchedule.TimeUnitEnum>()

  fun toApiBasicScheduleTimeUnit(timeUnit: Schedule.TimeUnit?): ConnectionScheduleDataBasicSchedule.TimeUnitEnum? =
    timeUnit?.convertTo<ConnectionScheduleDataBasicSchedule.TimeUnitEnum>()

  fun toApiConnectionScheduleType(standardSync: StandardSync): ConnectionScheduleType =
    if (standardSync.scheduleType != null) {
      when (standardSync.scheduleType) {
        StandardSync.ScheduleType.MANUAL -> {
          ConnectionScheduleType.MANUAL
        }

        StandardSync.ScheduleType.BASIC_SCHEDULE -> {
          ConnectionScheduleType.BASIC
        }

        StandardSync.ScheduleType.CRON -> {
          ConnectionScheduleType.CRON
        }

        else -> throw RuntimeException("Unexpected scheduleType " + standardSync.scheduleType)
      }
    } else if (standardSync.manual) {
      // Legacy schema, manual sync.
      ConnectionScheduleType.MANUAL
    } else {
      // Legacy schema, basic schedule.
      ConnectionScheduleType.BASIC
    }

  fun toApiConnectionScheduleData(standardSync: StandardSync): ConnectionScheduleData? =
    if (standardSync.scheduleType != null) {
      when (standardSync.scheduleType) {
        StandardSync.ScheduleType.MANUAL -> {
          null
        }

        StandardSync.ScheduleType.BASIC_SCHEDULE -> {
          ConnectionScheduleData()
            .basicSchedule(
              ConnectionScheduleDataBasicSchedule()
                .timeUnit(this.toApiBasicScheduleTimeUnit(standardSync.scheduleData.basicSchedule.timeUnit))
                .units(standardSync.scheduleData.basicSchedule.units),
            )
        }

        StandardSync.ScheduleType.CRON -> {
          ConnectionScheduleData()
            .cron(
              ConnectionScheduleDataCron()
                .cronExpression(standardSync.scheduleData.cron.cronExpression)
                .cronTimeZone(standardSync.scheduleData.cron.cronTimeZone),
            )
        }

        else -> throw RuntimeException("Unexpected scheduleType " + standardSync.scheduleType)
      }
    } else if (standardSync.manual) {
      // Legacy schema, manual sync.
      null
    } else {
      // Legacy schema, basic schedule.
      ConnectionScheduleData()
        .basicSchedule(
          ConnectionScheduleDataBasicSchedule()
            .timeUnit(this.toApiBasicScheduleTimeUnit(standardSync.schedule.timeUnit))
            .units(standardSync.schedule.units),
        )
    }

  fun toLegacyConnectionSchedule(standardSync: StandardSync): ConnectionSchedule? =
    if (standardSync.scheduleType != null) {
      // Populate everything based on the new schema.
      when (standardSync.scheduleType) {
        StandardSync.ScheduleType.MANUAL, StandardSync.ScheduleType.CRON -> {
          // We don't populate any legacy data here.
          null
        }

        StandardSync.ScheduleType.BASIC_SCHEDULE -> {
          ConnectionSchedule()
            .timeUnit(this.toApiTimeUnit(standardSync.scheduleData.basicSchedule.timeUnit))
            .units(standardSync.scheduleData.basicSchedule.units)
        }

        else -> throw RuntimeException("Unexpected scheduleType " + standardSync.scheduleType)
      }
    } else if (standardSync.manual) {
      // Legacy schema, manual sync.
      null
    } else {
      // Legacy schema, basic schedule.
      ConnectionSchedule()
        .timeUnit(this.toApiTimeUnit(standardSync.schedule.timeUnit))
        .units(standardSync.schedule.units)
    }

  fun populateConnectionReadSchedule(
    standardSync: StandardSync,
    connectionRead: ConnectionRead,
  ) {
    connectionRead.scheduleType(this.toApiConnectionScheduleType(standardSync))
    connectionRead.scheduleData(this.toApiConnectionScheduleData(standardSync))

    // TODO(https://github.com/airbytehq/airbyte/issues/11432): only return new schema once frontend is
    // ready.
    connectionRead.schedule(this.toLegacyConnectionSchedule(standardSync))
  }
}
