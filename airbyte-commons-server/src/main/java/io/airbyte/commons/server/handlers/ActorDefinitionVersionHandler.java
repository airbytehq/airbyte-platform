/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.server.converters.ApiPojoConverters.toApiSupportLevel;
import static io.airbyte.commons.server.converters.ApiPojoConverters.toApiSupportState;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.model.generated.ActorDefinitionVersionBreakingChanges;
import io.airbyte.api.model.generated.ActorDefinitionVersionRead;
import io.airbyte.api.model.generated.DestinationIdRequestBody;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

/**
 * ActorDefinitionVersionHandler. Javadocs suppressed because api docs should be used as source of
 * truth.
 */
@SuppressWarnings("ParameterName")
@Singleton
public class ActorDefinitionVersionHandler {

  private final ConfigRepository configRepository;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;

  @Inject
  public ActorDefinitionVersionHandler(final ConfigRepository configRepository,
                                       final ActorDefinitionVersionHelper actorDefinitionVersionHelper) {
    this.configRepository = configRepository;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
  }

  private LocalDate getMinBreakingChangeUpgradeDeadline(final List<ActorDefinitionBreakingChange> breakingChanges) {
    return breakingChanges.stream()
        .map(ActorDefinitionBreakingChange::getUpgradeDeadline)
        .map(LocalDate::parse)
        .min(LocalDate::compareTo)
        .orElse(null);
  }

  public ActorDefinitionVersionRead getActorDefinitionVersionForSourceId(final SourceIdRequestBody sourceIdRequestBody)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final SourceConnection sourceConnection = configRepository.getSourceConnection(sourceIdRequestBody.getSourceId());
    final StandardSourceDefinition sourceDefinition = configRepository.getSourceDefinitionFromSource(sourceConnection.getSourceId());
    final ActorDefinitionVersionWithOverrideStatus versionWithOverrideStatus =
        actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(sourceDefinition, sourceConnection.getWorkspaceId(),
            sourceConnection.getSourceId());
    return createActorDefinitionVersionRead(versionWithOverrideStatus);
  }

  public ActorDefinitionVersionRead getActorDefinitionVersionForDestinationId(final DestinationIdRequestBody destinationIdRequestBody)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final DestinationConnection destinationConnection = configRepository.getDestinationConnection(destinationIdRequestBody.getDestinationId());
    final StandardDestinationDefinition destinationDefinition =
        configRepository.getDestinationDefinitionFromDestination(destinationConnection.getDestinationId());
    final ActorDefinitionVersionWithOverrideStatus versionWithOverrideStatus =
        actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(destinationDefinition,
            destinationConnection.getWorkspaceId(), destinationConnection.getDestinationId());
    return createActorDefinitionVersionRead(versionWithOverrideStatus);
  }

  @VisibleForTesting
  ActorDefinitionVersionRead createActorDefinitionVersionRead(final ActorDefinitionVersionWithOverrideStatus versionWithOverrideStatus)
      throws IOException {
    final ActorDefinitionVersion actorDefinitionVersion = versionWithOverrideStatus.actorDefinitionVersion();
    final ActorDefinitionVersionRead advRead = new ActorDefinitionVersionRead()
        .dockerRepository(actorDefinitionVersion.getDockerRepository())
        .dockerImageTag(actorDefinitionVersion.getDockerImageTag())
        .supportsDbt(Objects.requireNonNullElse(actorDefinitionVersion.getSupportsDbt(), false))
        .normalizationConfig(ApiPojoConverters.normalizationDestinationDefinitionConfigToApi(actorDefinitionVersion.getNormalizationConfig()))
        .supportState(toApiSupportState(actorDefinitionVersion.getSupportState()))
        .supportLevel(toApiSupportLevel(actorDefinitionVersion.getSupportLevel()))
        .isOverrideApplied(versionWithOverrideStatus.isOverrideApplied());

    final List<ActorDefinitionBreakingChange> breakingChanges = configRepository.listBreakingChangesForActorDefinitionVersion(actorDefinitionVersion);

    if (!breakingChanges.isEmpty()) {
      final LocalDate minUpgradeDeadline = getMinBreakingChangeUpgradeDeadline(breakingChanges);
      advRead.breakingChanges(new ActorDefinitionVersionBreakingChanges()
          .upcomingBreakingChanges(breakingChanges.stream().map(ApiPojoConverters::toApiBreakingChange).toList())
          .minUpgradeDeadline(minUpgradeDeadline));
    }

    return advRead;
  }

}
