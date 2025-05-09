/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import io.airbyte.api.model.generated.ActorDefinitionVersionBreakingChanges;
import io.airbyte.api.model.generated.DeadlineAction;
import io.airbyte.commons.server.ServerConstants;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.converters.SpecFetcher;
import io.airbyte.commons.server.errors.UnsupportedProtocolVersionException;
import io.airbyte.commons.server.scheduler.SynchronousResponse;
import io.airbyte.commons.server.scheduler.SynchronousSchedulerClient;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.helpers.ConnectorRegistryConverters;
import io.airbyte.config.persistence.ActorDefinitionVersionResolver;
import io.airbyte.config.specs.RemoteDefinitionsProvider;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * A helper class for server code that is the shared for actor definitions (source definitions and
 * destination definitions).
 */
@Singleton
public class ActorDefinitionHandlerHelper {

  private final SynchronousSchedulerClient synchronousSchedulerClient;
  private final AirbyteProtocolVersionRange protocolVersionRange;
  private final ActorDefinitionVersionResolver actorDefinitionVersionResolver;
  private final RemoteDefinitionsProvider remoteDefinitionsProvider;
  private final ActorDefinitionService actorDefinitionService;
  private final ApiPojoConverters apiPojoConverters;

  public ActorDefinitionHandlerHelper(final SynchronousSchedulerClient synchronousSchedulerClient,
                                      final AirbyteProtocolVersionRange airbyteProtocolVersionRange,
                                      final ActorDefinitionVersionResolver actorDefinitionVersionResolver,
                                      final RemoteDefinitionsProvider remoteDefinitionsProvider,
                                      final ActorDefinitionService actorDefinitionService,
                                      final ApiPojoConverters apiPojoConverters) {
    this.synchronousSchedulerClient = synchronousSchedulerClient;
    this.protocolVersionRange = airbyteProtocolVersionRange;
    this.actorDefinitionVersionResolver = actorDefinitionVersionResolver;
    this.remoteDefinitionsProvider = remoteDefinitionsProvider;
    this.actorDefinitionService = actorDefinitionService;
    this.apiPojoConverters = apiPojoConverters;
  }

  /**
   * Create a new actor definition version to set as default for a new connector from a create
   * request.
   *
   * @param dockerRepository - the docker repository
   * @param dockerImageTag - the docker image tag
   * @param documentationUrl - the documentation url
   * @param workspaceId - the workspace id
   * @return - the new actor definition version
   * @throws IOException - if there is an error fetching the spec
   */
  public ActorDefinitionVersion defaultDefinitionVersionFromCreate(final String dockerRepository,
                                                                   final String dockerImageTag,
                                                                   final URI documentationUrl,
                                                                   final UUID workspaceId)
      throws IOException {
    final ConnectorSpecification spec = getSpecForImage(
        dockerRepository,
        dockerImageTag,
        // Only custom connectors can be created via handlers.
        true,
        workspaceId);
    final String protocolVersion = getAndValidateProtocolVersionFromSpec(spec);

    return new ActorDefinitionVersion()
        .withDockerImageTag(dockerImageTag)
        .withDockerRepository(dockerRepository)
        .withSpec(spec)
        .withDocumentationUrl(documentationUrl.toString())
        .withProtocolVersion(protocolVersion)
        .withSupportLevel(io.airbyte.config.SupportLevel.NONE)
        .withInternalSupportLevel(100L)
        .withReleaseStage(io.airbyte.config.ReleaseStage.CUSTOM);
  }

  /**
   * Create a new actor definition version to set as default from an existing default version and an
   * update request.
   *
   * @param currentVersion - the current default version
   * @param newDockerImageTag - the new docker image tag
   * @param isCustomConnector - whether the connector is a custom connector
   * @param workspaceId - context in which the job will run
   * @return - a new actor definition version
   * @throws IOException - if there is an error fetching the spec
   */
  public ActorDefinitionVersion defaultDefinitionVersionFromUpdate(final ActorDefinitionVersion currentVersion,
                                                                   final ActorType actorType,
                                                                   final String newDockerImageTag,
                                                                   final boolean isCustomConnector,
                                                                   final UUID workspaceId)
      throws IOException {

    final Optional<ActorDefinitionVersion> newVersionFromDbOrRemote = actorDefinitionVersionResolver.resolveVersionForTag(
        currentVersion.getActorDefinitionId(), actorType, currentVersion.getDockerRepository(), newDockerImageTag);

    final boolean isDev = ServerConstants.DEV_IMAGE_TAG.equals(newDockerImageTag);

    // The version already exists in the database or in our registry
    if (newVersionFromDbOrRemote.isPresent()) {
      final ActorDefinitionVersion newVersion = newVersionFromDbOrRemote.get();

      if (isDev) {
        // re-fetch spec for dev images to allow for easier iteration
        final ConnectorSpecification refreshedSpec =
            getSpecForImage(currentVersion.getDockerRepository(), newDockerImageTag, isCustomConnector, workspaceId);
        newVersion.setSpec(refreshedSpec);
        newVersion.setProtocolVersion(getAndValidateProtocolVersionFromSpec(refreshedSpec));
      }

      return newVersion;
    }

    // We've never seen this version
    final ConnectorSpecification spec = getSpecForImage(currentVersion.getDockerRepository(), newDockerImageTag, isCustomConnector, workspaceId);
    final String protocolVersion = getAndValidateProtocolVersionFromSpec(spec);

    return new ActorDefinitionVersion()
        .withActorDefinitionId(currentVersion.getActorDefinitionId())
        .withDockerRepository(currentVersion.getDockerRepository())
        .withDockerImageTag(newDockerImageTag)
        .withSpec(spec)
        .withDocumentationUrl(currentVersion.getDocumentationUrl())
        .withProtocolVersion(protocolVersion)
        .withReleaseStage(currentVersion.getReleaseStage())
        .withReleaseDate(currentVersion.getReleaseDate())
        .withSupportLevel(currentVersion.getSupportLevel())
        .withInternalSupportLevel(currentVersion.getInternalSupportLevel())
        .withCdkVersion(currentVersion.getCdkVersion())
        .withLastPublished(currentVersion.getLastPublished())
        .withAllowedHosts(currentVersion.getAllowedHosts())
        .withSupportsFileTransfer(currentVersion.getSupportsFileTransfer())
        .withSupportsRefreshes(currentVersion.getSupportsRefreshes());
  }

  private ConnectorSpecification getSpecForImage(final String dockerRepository,
                                                 final String imageTag,
                                                 final boolean isCustomConnector,
                                                 final UUID workspaceId)
      throws IOException {
    final String imageName = dockerRepository + ":" + imageTag;
    final SynchronousResponse<ConnectorSpecification> getSpecResponse =
        synchronousSchedulerClient.createGetSpecJob(imageName, isCustomConnector, workspaceId);
    return SpecFetcher.getSpecFromJob(getSpecResponse);
  }

  private String getAndValidateProtocolVersionFromSpec(final ConnectorSpecification spec) {
    final Version airbyteProtocolVersion = AirbyteProtocolVersion.getWithDefault(spec.getProtocolVersion());
    if (!protocolVersionRange.isSupported(airbyteProtocolVersion)) {
      throw new UnsupportedProtocolVersionException(airbyteProtocolVersion, protocolVersionRange.min(), protocolVersionRange.max());
    }
    return airbyteProtocolVersion.serialize();
  }

  /**
   * Fetches an optional breaking change list from the registry entry for the actor definition version
   * and persists it to the DB if present. The optional is empty if the registry entry is not found.
   *
   * @param actorDefinitionVersion - the actor definition version
   * @param actorType - the actor type
   * @throws IOException - if there is an error persisting the breaking changes
   */
  public List<ActorDefinitionBreakingChange> getBreakingChanges(final ActorDefinitionVersion actorDefinitionVersion, final ActorType actorType)
      throws IOException {

    final String connectorRepository = actorDefinitionVersion.getDockerRepository();
    // We always want the most up-to-date version of the list breaking changes, in case they've been
    // updated retroactively after the version was released.
    final String dockerImageTag = "latest";
    final Optional<List<ActorDefinitionBreakingChange>> breakingChanges;
    switch (actorType) {
      case SOURCE -> {
        final Optional<ConnectorRegistrySourceDefinition> registryDef =
            remoteDefinitionsProvider.getSourceDefinitionByVersion(connectorRepository, dockerImageTag);
        breakingChanges = registryDef.map(ConnectorRegistryConverters::toActorDefinitionBreakingChanges);
      }
      case DESTINATION -> {
        final Optional<ConnectorRegistryDestinationDefinition> registryDef =
            remoteDefinitionsProvider.getDestinationDefinitionByVersion(connectorRepository, dockerImageTag);
        breakingChanges = registryDef.map(ConnectorRegistryConverters::toActorDefinitionBreakingChanges);
      }
      default -> throw new IllegalArgumentException("Actor type not supported: " + actorType);
    }

    return breakingChanges.orElse(List.of());
  }

  private Optional<ActorDefinitionBreakingChange> firstUpcomingBreakingChange(final List<ActorDefinitionBreakingChange> breakingChanges) {
    return breakingChanges.stream()
        .min(Comparator.comparing(b -> LocalDate.parse(b.getUpgradeDeadline())));
  }

  public Optional<ActorDefinitionVersionBreakingChanges> getVersionBreakingChanges(final ActorDefinitionVersion actorDefinitionVersion)
      throws IOException {
    final List<ActorDefinitionBreakingChange> breakingChanges =
        actorDefinitionService.listBreakingChangesForActorDefinitionVersion(actorDefinitionVersion);

    if (!breakingChanges.isEmpty()) {
      final Optional<ActorDefinitionBreakingChange> firstBreakingChange = firstUpcomingBreakingChange(breakingChanges);
      final LocalDate minUpgradeDeadline = firstBreakingChange.map(it -> LocalDate.parse(it.getUpgradeDeadline())).orElse(null);
      final String minDeadlineAction = firstBreakingChange.map(ActorDefinitionBreakingChange::getDeadlineAction).orElse(null);
      final DeadlineAction apiDeadlineAction =
          Objects.equals(minDeadlineAction, DeadlineAction.AUTO_UPGRADE.toString()) ? DeadlineAction.AUTO_UPGRADE : DeadlineAction.DISABLE;
      return Optional.of(new ActorDefinitionVersionBreakingChanges()
          .upcomingBreakingChanges(breakingChanges.stream().map(apiPojoConverters::toApiBreakingChange).toList())
          .minUpgradeDeadline(minUpgradeDeadline)
          .deadlineAction(apiDeadlineAction));
    } else {
      return Optional.empty();
    }
  }

}
