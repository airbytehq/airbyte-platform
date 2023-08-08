/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.server.converters.ApiPojoConverters.toApiSupportState;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.model.generated.ActorDefinitionVersionBreakingChanges;
import io.airbyte.api.model.generated.ActorDefinitionVersionRead;
import io.airbyte.api.model.generated.DestinationIdRequestBody;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;

/**
 * ActorDefinitionVersionHandler. Javadocs suppressed because api docs should be used as source of
 * truth.
 */
@SuppressWarnings({"MissingJavadocMethod", "ParameterName"})
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

  private List<ActorDefinitionBreakingChange> filterFutureBreakingChanges(final List<ActorDefinitionBreakingChange> breakingChanges,
                                                                          final ActorDefinitionVersion actorDefinitionVersion) {
    final Version currentVersion = new Version(actorDefinitionVersion.getDockerImageTag());
    return breakingChanges.stream()
        .filter(breakingChange -> breakingChange.getVersion().greaterThan(currentVersion))
        .sorted((v1, v2) -> v1.getVersion().versionCompareTo(v2.getVersion()))
        .toList();
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
    final ActorDefinitionVersion actorDefinitionVersion =
        actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, sourceConnection.getWorkspaceId(), sourceConnection.getSourceId());
    return createActorDefinitionVersionRead(actorDefinitionVersion);
  }

  public ActorDefinitionVersionRead getActorDefinitionVersionForDestinationId(final DestinationIdRequestBody destinationIdRequestBody)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final DestinationConnection destinationConnection = configRepository.getDestinationConnection(destinationIdRequestBody.getDestinationId());
    final StandardDestinationDefinition destinationDefinition =
        configRepository.getDestinationDefinitionFromDestination(destinationConnection.getDestinationId());
    final ActorDefinitionVersion actorDefinitionVersion = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition,
        destinationConnection.getWorkspaceId(), destinationConnection.getDestinationId());
    return createActorDefinitionVersionRead(actorDefinitionVersion);
  }

  @VisibleForTesting
  ActorDefinitionVersionRead createActorDefinitionVersionRead(final ActorDefinitionVersion actorDefinitionVersion) throws IOException {
    final ActorDefinitionVersionRead advRead = new ActorDefinitionVersionRead()
        .dockerRepository(actorDefinitionVersion.getDockerRepository())
        .dockerImageTag(actorDefinitionVersion.getDockerImageTag())
        .supportState(toApiSupportState(actorDefinitionVersion.getSupportState()));

    final List<ActorDefinitionBreakingChange> breakingChanges =
        configRepository.listBreakingChangesForActorDefinition(actorDefinitionVersion.getActorDefinitionId());
    final List<ActorDefinitionBreakingChange> futureBreakingChanges = filterFutureBreakingChanges(breakingChanges, actorDefinitionVersion);

    if (!futureBreakingChanges.isEmpty()) {
      final LocalDate minUpgradeDeadline = getMinBreakingChangeUpgradeDeadline(futureBreakingChanges);
      advRead.breakingChanges(new ActorDefinitionVersionBreakingChanges()
          .upcomingBreakingChanges(futureBreakingChanges.stream().map(ApiPojoConverters::toApiBreakingChange).toList())
          .minUpgradeDeadline(minUpgradeDeadline));
    }

    return advRead;
  }

}
