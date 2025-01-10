/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters;

import io.airbyte.api.model.generated.ActorDefinitionBreakingChange;
import io.airbyte.api.model.generated.ActorDefinitionResourceRequirements;
import io.airbyte.api.model.generated.ActorType;
import io.airbyte.api.model.generated.AttemptSyncConfig;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.ConnectionSchedule;
import io.airbyte.api.model.generated.ConnectionScheduleData;
import io.airbyte.api.model.generated.ConnectionScheduleDataBasicSchedule;
import io.airbyte.api.model.generated.ConnectionScheduleDataCron;
import io.airbyte.api.model.generated.ConnectionState;
import io.airbyte.api.model.generated.ConnectionStateType;
import io.airbyte.api.model.generated.ConnectionStatus;
import io.airbyte.api.model.generated.DeadlineAction;
import io.airbyte.api.model.generated.Geography;
import io.airbyte.api.model.generated.JobType;
import io.airbyte.api.model.generated.JobTypeResourceLimit;
import io.airbyte.api.model.generated.NonBreakingChangesPreference;
import io.airbyte.api.model.generated.ReleaseStage;
import io.airbyte.api.model.generated.ResourceRequirements;
import io.airbyte.api.model.generated.SchemaChangeBackfillPreference;
import io.airbyte.api.model.generated.SupportLevel;
import io.airbyte.api.model.generated.SupportState;
import io.airbyte.commons.converters.StateConverter;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.config.BasicSchedule;
import io.airbyte.config.Schedule;
import io.airbyte.config.StandardSync;
import io.airbyte.config.State;
import io.airbyte.config.StateWrapper;
import io.airbyte.config.helpers.StateMessageHelper;
import jakarta.inject.Singleton;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Convert between API and internal versions of airbyte models.
 */
@SuppressWarnings("LineLength")
@Singleton
public class ApiPojoConverters {

  private final CatalogConverter catalogConverter;

  public ApiPojoConverters(final CatalogConverter catalogConverter) {
    this.catalogConverter = catalogConverter;
  }

  public io.airbyte.config.ActorDefinitionResourceRequirements actorDefResourceReqsToInternal(final ActorDefinitionResourceRequirements actorDefResourceReqs) {
    if (actorDefResourceReqs == null) {
      return null;
    }

    return new io.airbyte.config.ActorDefinitionResourceRequirements()
        .withDefault(actorDefResourceReqs.getDefault() == null ? null : this.resourceRequirementsToInternal(actorDefResourceReqs.getDefault()))
        .withJobSpecific(actorDefResourceReqs.getJobSpecific() == null ? null
            : actorDefResourceReqs.getJobSpecific()
                .stream()
                .map(jobSpecific -> new io.airbyte.config.JobTypeResourceLimit()
                    .withJobType(this.toInternalJobType(jobSpecific.getJobType()))
                    .withResourceRequirements(this.resourceRequirementsToInternal(jobSpecific.getResourceRequirements())))
                .collect(Collectors.toList()));
  }

  public io.airbyte.config.AttemptSyncConfig attemptSyncConfigToInternal(final AttemptSyncConfig attemptSyncConfig) {
    if (attemptSyncConfig == null) {
      return null;
    }

    final io.airbyte.config.AttemptSyncConfig internalAttemptSyncConfig = new io.airbyte.config.AttemptSyncConfig()
        .withSourceConfiguration(attemptSyncConfig.getSourceConfiguration())
        .withDestinationConfiguration(attemptSyncConfig.getDestinationConfiguration());

    final ConnectionState connectionState = attemptSyncConfig.getState();
    if (connectionState != null && connectionState.getStateType() != ConnectionStateType.NOT_SET) {
      final StateWrapper stateWrapper = StateConverter.toInternal(attemptSyncConfig.getState());
      final io.airbyte.config.State state = StateMessageHelper.getState(stateWrapper);
      internalAttemptSyncConfig.setState(state);
    }

    return internalAttemptSyncConfig;
  }

  public io.airbyte.api.model.generated.AttemptSyncConfig attemptSyncConfigToApi(final io.airbyte.config.AttemptSyncConfig attemptSyncConfig,
                                                                                 final UUID connectionId) {
    if (attemptSyncConfig == null) {
      return null;
    }

    final State state = attemptSyncConfig.getState();
    final Optional<StateWrapper> optStateWrapper = state != null ? StateMessageHelper.getTypedState(
        state.getState()) : Optional.empty();

    return new io.airbyte.api.model.generated.AttemptSyncConfig()
        .sourceConfiguration(attemptSyncConfig.getSourceConfiguration())
        .destinationConfiguration(attemptSyncConfig.getDestinationConfiguration())
        .state(StateConverter.toApi(connectionId, optStateWrapper.orElse(null)));
  }

  public ActorDefinitionResourceRequirements actorDefResourceReqsToApi(final io.airbyte.config.ActorDefinitionResourceRequirements actorDefResourceReqs) {
    if (actorDefResourceReqs == null) {
      return null;
    }

    return new ActorDefinitionResourceRequirements()
        ._default(actorDefResourceReqs.getDefault() == null ? null : this.resourceRequirementsToApi(actorDefResourceReqs.getDefault()))
        .jobSpecific(actorDefResourceReqs.getJobSpecific() == null ? null
            : actorDefResourceReqs.getJobSpecific()
                .stream()
                .map(jobSpecific -> new JobTypeResourceLimit()
                    .jobType(this.toApiJobType(jobSpecific.getJobType()))
                    .resourceRequirements(this.resourceRequirementsToApi(jobSpecific.getResourceRequirements())))
                .collect(Collectors.toList()));
  }

  public io.airbyte.config.ResourceRequirements resourceRequirementsToInternal(final ResourceRequirements resourceReqs) {
    if (resourceReqs == null) {
      return null;
    }

    return new io.airbyte.config.ResourceRequirements()
        .withCpuRequest(resourceReqs.getCpuRequest())
        .withCpuLimit(resourceReqs.getCpuLimit())
        .withMemoryRequest(resourceReqs.getMemoryRequest())
        .withMemoryLimit(resourceReqs.getMemoryLimit());
  }

  public ResourceRequirements resourceRequirementsToApi(final io.airbyte.config.ResourceRequirements resourceReqs) {
    if (resourceReqs == null) {
      return null;
    }

    return new ResourceRequirements()
        .cpuRequest(resourceReqs.getCpuRequest())
        .cpuLimit(resourceReqs.getCpuLimit())
        .memoryRequest(resourceReqs.getMemoryRequest())
        .memoryLimit(resourceReqs.getMemoryLimit());
  }

  public ConnectionRead internalToConnectionRead(final StandardSync standardSync) {
    final ConnectionRead connectionRead = new ConnectionRead()
        .connectionId(standardSync.getConnectionId())
        .sourceId(standardSync.getSourceId())
        .destinationId(standardSync.getDestinationId())
        .operationIds(standardSync.getOperationIds())
        .status(this.toApiStatus(standardSync.getStatus()))
        .name(standardSync.getName())
        .namespaceDefinition(Enums.convertTo(standardSync.getNamespaceDefinition(), io.airbyte.api.model.generated.NamespaceDefinitionType.class))
        .namespaceFormat(standardSync.getNamespaceFormat())
        .prefix(standardSync.getPrefix())
        .syncCatalog(this.catalogConverter.toApi(standardSync.getCatalog(), standardSync.getFieldSelectionData()))
        .sourceCatalogId(standardSync.getSourceCatalogId())
        .breakingChange(standardSync.getBreakingChange())
        .geography(Enums.convertTo(standardSync.getGeography(), Geography.class))
        .nonBreakingChangesPreference(Enums.convertTo(standardSync.getNonBreakingChangesPreference(), NonBreakingChangesPreference.class))
        .backfillPreference(Enums.convertTo(standardSync.getBackfillPreference(), SchemaChangeBackfillPreference.class))
        .notifySchemaChanges(standardSync.getNotifySchemaChanges())
        .createdAt(standardSync.getCreatedAt())
        .notifySchemaChangesByEmail(standardSync.getNotifySchemaChangesByEmail());

    if (standardSync.getResourceRequirements() != null) {
      connectionRead.resourceRequirements(this.resourceRequirementsToApi(standardSync.getResourceRequirements()));
    }

    this.populateConnectionReadSchedule(standardSync, connectionRead);

    return connectionRead;
  }

  public JobType toApiJobType(final io.airbyte.config.JobTypeResourceLimit.JobType jobType) {
    return Enums.convertTo(jobType, JobType.class);
  }

  public io.airbyte.config.JobTypeResourceLimit.JobType toInternalJobType(final JobType jobType) {
    return Enums.convertTo(jobType, io.airbyte.config.JobTypeResourceLimit.JobType.class);
  }

  public io.airbyte.config.ActorType toInternalActorType(final ActorType actorType) {
    return Enums.convertTo(actorType, io.airbyte.config.ActorType.class);
  }

  // TODO(https://github.com/airbytehq/airbyte/issues/11432): remove these helpers.
  public ConnectionSchedule.TimeUnitEnum toApiTimeUnit(final Schedule.TimeUnit apiTimeUnit) {
    return Enums.convertTo(apiTimeUnit, ConnectionSchedule.TimeUnitEnum.class);
  }

  public ConnectionSchedule.TimeUnitEnum toApiTimeUnit(final BasicSchedule.TimeUnit timeUnit) {
    return Enums.convertTo(timeUnit, ConnectionSchedule.TimeUnitEnum.class);
  }

  public ConnectionStatus toApiStatus(final StandardSync.Status status) {
    return Enums.convertTo(status, ConnectionStatus.class);
  }

  public StandardSync.Status toPersistenceStatus(final ConnectionStatus apiStatus) {
    return Enums.convertTo(apiStatus, StandardSync.Status.class);
  }

  public StandardSync.NonBreakingChangesPreference toPersistenceNonBreakingChangesPreference(final NonBreakingChangesPreference preference) {
    return Enums.convertTo(preference, StandardSync.NonBreakingChangesPreference.class);
  }

  public StandardSync.BackfillPreference toPersistenceBackfillPreference(final SchemaChangeBackfillPreference preference) {
    return Enums.convertTo(preference, StandardSync.BackfillPreference.class);
  }

  public Geography toApiGeography(final io.airbyte.config.Geography geography) {
    return Enums.convertTo(geography, Geography.class);
  }

  public io.airbyte.config.Geography toPersistenceGeography(final Geography apiGeography) {
    return Enums.convertTo(apiGeography, io.airbyte.config.Geography.class);
  }

  public Schedule.TimeUnit toPersistenceTimeUnit(final ConnectionSchedule.TimeUnitEnum apiTimeUnit) {
    return Enums.convertTo(apiTimeUnit, Schedule.TimeUnit.class);
  }

  public BasicSchedule.TimeUnit toBasicScheduleTimeUnit(final ConnectionSchedule.TimeUnitEnum apiTimeUnit) {
    return Enums.convertTo(apiTimeUnit, BasicSchedule.TimeUnit.class);
  }

  public BasicSchedule.TimeUnit toBasicScheduleTimeUnit(final ConnectionScheduleDataBasicSchedule.TimeUnitEnum apiTimeUnit) {
    return Enums.convertTo(apiTimeUnit, BasicSchedule.TimeUnit.class);
  }

  public Schedule.TimeUnit toLegacyScheduleTimeUnit(final ConnectionScheduleDataBasicSchedule.TimeUnitEnum timeUnit) {
    return Enums.convertTo(timeUnit, Schedule.TimeUnit.class);
  }

  public ReleaseStage toApiReleaseStage(final io.airbyte.config.ReleaseStage releaseStage) {
    if (releaseStage == null) {
      return null;
    }
    return ReleaseStage.fromValue(releaseStage.value());
  }

  public SupportLevel toApiSupportLevel(final io.airbyte.config.SupportLevel supportLevel) {
    if (supportLevel == null) {
      return SupportLevel.NONE;
    }
    return SupportLevel.fromValue(supportLevel.value());
  }

  public SupportState toApiSupportState(final io.airbyte.config.ActorDefinitionVersion.SupportState supportState) {
    if (supportState == null) {
      return null;
    }
    return SupportState.fromValue(supportState.value());
  }

  public ActorDefinitionBreakingChange toApiBreakingChange(final io.airbyte.config.ActorDefinitionBreakingChange breakingChange) {
    if (breakingChange == null) {
      return null;
    }
    final DeadlineAction deadlineAction =
        Objects.equals(breakingChange.getDeadlineAction(), DeadlineAction.AUTO_UPGRADE.toString()) ? DeadlineAction.AUTO_UPGRADE
            : DeadlineAction.DISABLE;
    return new ActorDefinitionBreakingChange()
        .version(breakingChange.getVersion().serialize())
        .message(breakingChange.getMessage())
        .migrationDocumentationUrl(breakingChange.getMigrationDocumentationUrl())
        .upgradeDeadline(this.toLocalDate(breakingChange.getUpgradeDeadline()))
        .deadlineAction(deadlineAction);
  }

  public LocalDate toLocalDate(final String date) {
    if (date == null || date.isBlank()) {
      return null;
    }
    return LocalDate.parse(date);
  }

  public OffsetDateTime toOffsetDateTime(Date date) {
    if (date == null) {
      return null;
    }
    return date.toInstant().atOffset(ZoneOffset.UTC);
  }

  public ConnectionScheduleDataBasicSchedule.TimeUnitEnum toApiBasicScheduleTimeUnit(final BasicSchedule.TimeUnit timeUnit) {
    return Enums.convertTo(timeUnit, ConnectionScheduleDataBasicSchedule.TimeUnitEnum.class);
  }

  public ConnectionScheduleDataBasicSchedule.TimeUnitEnum toApiBasicScheduleTimeUnit(final Schedule.TimeUnit timeUnit) {
    return Enums.convertTo(timeUnit, ConnectionScheduleDataBasicSchedule.TimeUnitEnum.class);
  }

  public io.airbyte.api.model.generated.ConnectionScheduleType toApiConnectionScheduleType(final StandardSync standardSync) {
    if (standardSync.getScheduleType() != null) {
      switch (standardSync.getScheduleType()) {
        case MANUAL -> {
          return io.airbyte.api.model.generated.ConnectionScheduleType.MANUAL;
        }
        case BASIC_SCHEDULE -> {
          return io.airbyte.api.model.generated.ConnectionScheduleType.BASIC;
        }
        case CRON -> {
          return io.airbyte.api.model.generated.ConnectionScheduleType.CRON;
        }
        default -> throw new RuntimeException("Unexpected scheduleType " + standardSync.getScheduleType());
      }
    } else if (standardSync.getManual()) {
      // Legacy schema, manual sync.
      return io.airbyte.api.model.generated.ConnectionScheduleType.MANUAL;
    } else {
      // Legacy schema, basic schedule.
      return io.airbyte.api.model.generated.ConnectionScheduleType.BASIC;
    }
  }

  public io.airbyte.api.model.generated.ConnectionScheduleData toApiConnectionScheduleData(final StandardSync standardSync) {
    if (standardSync.getScheduleType() != null) {
      switch (standardSync.getScheduleType()) {
        case MANUAL -> {
          return null;
        }
        case BASIC_SCHEDULE -> {
          return new ConnectionScheduleData()
              .basicSchedule(new ConnectionScheduleDataBasicSchedule()
                  .timeUnit(this.toApiBasicScheduleTimeUnit(standardSync.getScheduleData().getBasicSchedule().getTimeUnit()))
                  .units(standardSync.getScheduleData().getBasicSchedule().getUnits()));
        }
        case CRON -> {
          return new ConnectionScheduleData()
              .cron(new ConnectionScheduleDataCron()
                  .cronExpression(standardSync.getScheduleData().getCron().getCronExpression())
                  .cronTimeZone(standardSync.getScheduleData().getCron().getCronTimeZone()));
        }
        default -> throw new RuntimeException("Unexpected scheduleType " + standardSync.getScheduleType());
      }
    } else if (standardSync.getManual()) {
      // Legacy schema, manual sync.
      return null;
    } else {
      // Legacy schema, basic schedule.
      return new ConnectionScheduleData()
          .basicSchedule(new ConnectionScheduleDataBasicSchedule()
              .timeUnit(this.toApiBasicScheduleTimeUnit(standardSync.getSchedule().getTimeUnit()))
              .units(standardSync.getSchedule().getUnits()));
    }
  }

  public ConnectionSchedule toLegacyConnectionSchedule(final StandardSync standardSync) {
    if (standardSync.getScheduleType() != null) {
      // Populate everything based on the new schema.
      switch (standardSync.getScheduleType()) {
        case MANUAL, CRON -> {
          // We don't populate any legacy data here.
          return null;
        }
        case BASIC_SCHEDULE -> {
          return new ConnectionSchedule()
              .timeUnit(this.toApiTimeUnit(standardSync.getScheduleData().getBasicSchedule().getTimeUnit()))
              .units(standardSync.getScheduleData().getBasicSchedule().getUnits());
        }
        default -> throw new RuntimeException("Unexpected scheduleType " + standardSync.getScheduleType());
      }
    } else if (standardSync.getManual()) {
      // Legacy schema, manual sync.
      return null;
    } else {
      // Legacy schema, basic schedule.
      return new ConnectionSchedule()
          .timeUnit(this.toApiTimeUnit(standardSync.getSchedule().getTimeUnit()))
          .units(standardSync.getSchedule().getUnits());
    }
  }

  public void populateConnectionReadSchedule(final StandardSync standardSync, final ConnectionRead connectionRead) {
    connectionRead.scheduleType(this.toApiConnectionScheduleType(standardSync));
    connectionRead.scheduleData(this.toApiConnectionScheduleData(standardSync));

    // TODO(https://github.com/airbytehq/airbyte/issues/11432): only return new schema once frontend is
    // ready.
    connectionRead.schedule(this.toLegacyConnectionSchedule(standardSync));
  }

}
