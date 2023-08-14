/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import io.airbyte.commons.server.ServerConstants;
import io.airbyte.commons.server.converters.SpecFetcher;
import io.airbyte.commons.server.errors.UnsupportedProtocolVersionException;
import io.airbyte.commons.server.scheduler.SynchronousResponse;
import io.airbyte.commons.server.scheduler.SynchronousSchedulerClient;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.persistence.ActorDefinitionVersionResolver;
import io.airbyte.protocol.models.ConnectorSpecification;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * A helper class for server code that is the shared for actor definitions (source definitions and
 * destination definitions).
 */
@Singleton
public class ActorDefinitionHandlerHelper {

  private final SynchronousSchedulerClient synchronousSchedulerClient;
  private final AirbyteProtocolVersionRange protocolVersionRange;
  private final ActorDefinitionVersionResolver actorDefinitionVersionResolver;

  public ActorDefinitionHandlerHelper(final SynchronousSchedulerClient synchronousSchedulerClient,
                                      final AirbyteProtocolVersionRange airbyteProtocolVersionRange,
                                      final ActorDefinitionVersionResolver actorDefinitionVersionResolver) {
    this.synchronousSchedulerClient = synchronousSchedulerClient;
    this.protocolVersionRange = airbyteProtocolVersionRange;
    this.actorDefinitionVersionResolver = actorDefinitionVersionResolver;
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
                                                                   final @Nullable UUID workspaceId)
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
        .withReleaseStage(io.airbyte.config.ReleaseStage.CUSTOM);
  }

  /**
   * Create a new actor definition version to set as default from an existing default version and an
   * update request.
   *
   * @param currentVersion - the current default version
   * @param newDockerImageTag - the new docker image tag
   * @param isCustomConnector - whether the connector is a custom connector
   * @return - a new actor definition version
   * @throws IOException - if there is an error fetching the spec
   */
  public ActorDefinitionVersion defaultDefinitionVersionFromUpdate(final ActorDefinitionVersion currentVersion,
                                                                   final ActorType actorType,
                                                                   final String newDockerImageTag,
                                                                   final boolean isCustomConnector)
      throws IOException {

    final Optional<ActorDefinitionVersion> newVersionFromDbOrRemote = actorDefinitionVersionResolver.resolveVersionForTag(
        currentVersion.getActorDefinitionId(), actorType, currentVersion.getDockerRepository(), newDockerImageTag);

    if (newVersionFromDbOrRemote.isPresent()) {
      // TODO (ella): based on the existing behavior, the new version from the db will exist for `dev`,
      // so we would want to re-run the spec below for `dev` images as well. However, there is an existing
      // bug(?) that would stop us from persisting that new spec since the ADV already exists for
      // (<actor def id>, 'dev').
      return newVersionFromDbOrRemote.get();
    }

    // specs are re-fetched from the container if the image tag has changed, or if the tag is "dev",
    // to allow for easier iteration of dev images
    final boolean specNeedsUpdate = !currentVersion.getDockerImageTag().equals(newDockerImageTag)
        || ServerConstants.DEV_IMAGE_TAG.equals(newDockerImageTag);
    final ConnectorSpecification spec = specNeedsUpdate
        ? getSpecForImage(currentVersion.getDockerRepository(), newDockerImageTag, isCustomConnector, null)
        : currentVersion.getSpec();
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
        .withNormalizationConfig(currentVersion.getNormalizationConfig())
        .withSupportsDbt(currentVersion.getSupportsDbt())
        .withAllowedHosts(currentVersion.getAllowedHosts());
  }

  private ConnectorSpecification getSpecForImage(final String dockerRepository,
                                                 final String imageTag,
                                                 final boolean isCustomConnector,
                                                 final @Nullable UUID workspaceId)
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

}
