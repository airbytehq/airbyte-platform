/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.model.generated.CustomDestinationDefinitionCreate;
import io.airbyte.api.model.generated.DestinationDefinitionCreate;
import io.airbyte.api.model.generated.DestinationDefinitionIdRequestBody;
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.DestinationDefinitionRead;
import io.airbyte.api.model.generated.DestinationDefinitionReadList;
import io.airbyte.api.model.generated.DestinationDefinitionUpdate;
import io.airbyte.api.model.generated.DestinationRead;
import io.airbyte.api.model.generated.PrivateDestinationDefinitionRead;
import io.airbyte.api.model.generated.PrivateDestinationDefinitionReadList;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.server.ServerConstants;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.converters.SpecFetcher;
import io.airbyte.commons.server.errors.IdNotFoundKnownException;
import io.airbyte.commons.server.errors.InternalServerKnownException;
import io.airbyte.commons.server.errors.UnsupportedProtocolVersionException;
import io.airbyte.commons.server.scheduler.SynchronousResponse;
import io.airbyte.commons.server.scheduler.SynchronousSchedulerClient;
import io.airbyte.commons.server.services.AirbyteRemoteOssCatalog;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionResourceRequirements;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.Configs;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.EnvConfigs;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.helpers.ConnectorRegistryConverters;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * DestinationDefinitionsHandler. Javadocs suppressed because api docs should be used as source of
 * truth.
 */
@SuppressWarnings({"PMD.AvoidCatchingNPE", "MissingJavadocMethod", "LineLength"})
@Singleton
public class DestinationDefinitionsHandler {

  private final ConfigRepository configRepository;
  private final Supplier<UUID> uuidSupplier;
  private final SynchronousSchedulerClient schedulerSynchronousClient;
  private final AirbyteRemoteOssCatalog remoteOssCatalog;
  private final DestinationHandler destinationHandler;
  private final AirbyteProtocolVersionRange protocolVersionRange;

  @VisibleForTesting
  public DestinationDefinitionsHandler(final ConfigRepository configRepository,
                                       final Supplier<UUID> uuidSupplier,
                                       final SynchronousSchedulerClient schedulerSynchronousClient,
                                       final AirbyteRemoteOssCatalog remoteOssCatalog,
                                       final DestinationHandler destinationHandler,
                                       final AirbyteProtocolVersionRange protocolVersionRange) {
    this.configRepository = configRepository;
    this.uuidSupplier = uuidSupplier;
    this.schedulerSynchronousClient = schedulerSynchronousClient;
    this.remoteOssCatalog = remoteOssCatalog;
    this.destinationHandler = destinationHandler;
    this.protocolVersionRange = protocolVersionRange;
  }

  // This should be deleted when cloud is migrated to micronaut
  @Deprecated(forRemoval = true)
  public DestinationDefinitionsHandler(final ConfigRepository configRepository,
                                       final SynchronousSchedulerClient schedulerSynchronousClient,
                                       final DestinationHandler destinationHandler) {
    this.configRepository = configRepository;
    this.uuidSupplier = UUID::randomUUID;
    this.schedulerSynchronousClient = schedulerSynchronousClient;
    this.remoteOssCatalog = new AirbyteRemoteOssCatalog();
    this.destinationHandler = destinationHandler;
    final Configs configs = new EnvConfigs();
    this.protocolVersionRange = new AirbyteProtocolVersionRange(configs.getAirbyteProtocolVersionMin(), configs.getAirbyteProtocolVersionMax());
  }

  @VisibleForTesting
  static DestinationDefinitionRead buildDestinationDefinitionRead(final StandardDestinationDefinition standardDestinationDefinition,
                                                                  final ActorDefinitionVersion destinationVersion) {
    try {

      return new DestinationDefinitionRead()
          .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
          .name(standardDestinationDefinition.getName())
          .dockerRepository(destinationVersion.getDockerRepository())
          .dockerImageTag(destinationVersion.getDockerImageTag())
          .documentationUrl(new URI(destinationVersion.getDocumentationUrl()))
          .icon(loadIcon(standardDestinationDefinition.getIcon()))
          .protocolVersion(destinationVersion.getProtocolVersion())
          .releaseStage(ApiPojoConverters.toApiReleaseStage(destinationVersion.getReleaseStage()))
          .releaseDate(ApiPojoConverters.toLocalDate(destinationVersion.getReleaseDate()))
          .supportsDbt(Objects.requireNonNullElse(destinationVersion.getSupportsDbt(), false))
          .normalizationConfig(
              ApiPojoConverters.normalizationDestinationDefinitionConfigToApi(destinationVersion.getNormalizationConfig()))
          .resourceRequirements(ApiPojoConverters.actorDefResourceReqsToApi(standardDestinationDefinition.getResourceRequirements()));
    } catch (final URISyntaxException | NullPointerException e) {
      throw new InternalServerKnownException("Unable to process retrieved latest destination definitions list", e);
    }
  }

  public DestinationDefinitionReadList listDestinationDefinitions() throws IOException {
    final List<StandardDestinationDefinition> standardDestinationDefinitions = configRepository.listStandardDestinationDefinitions(false);
    final Map<UUID, ActorDefinitionVersion> destinationDefinitionVersionMap = getVersionsForDestinationDefinitions(standardDestinationDefinitions);
    return toDestinationDefinitionReadList(standardDestinationDefinitions, destinationDefinitionVersionMap);
  }

  private static DestinationDefinitionReadList toDestinationDefinitionReadList(final List<StandardDestinationDefinition> defs,
                                                                               final Map<UUID, ActorDefinitionVersion> defIdToVersionMap) {
    final List<DestinationDefinitionRead> reads = defs.stream()
        .map(d -> buildDestinationDefinitionRead(d, defIdToVersionMap.get(d.getDestinationDefinitionId())))
        .collect(Collectors.toList());
    return new DestinationDefinitionReadList().destinationDefinitions(reads);
  }

  private Map<UUID, ActorDefinitionVersion> getVersionsForDestinationDefinitions(final List<StandardDestinationDefinition> destinationDefinitions)
      throws IOException {
    return configRepository.getActorDefinitionVersions(destinationDefinitions
        .stream()
        .map(StandardDestinationDefinition::getDefaultVersionId)
        .collect(Collectors.toList()))
        .stream().collect(Collectors.toMap(ActorDefinitionVersion::getActorDefinitionId, v -> v));
  }

  public DestinationDefinitionReadList listLatestDestinationDefinitions() {
    final List<ConnectorRegistryDestinationDefinition> latestDestinations = remoteOssCatalog.getDestinationDefinitions();
    final List<StandardDestinationDefinition> destinationDefs =
        latestDestinations.stream().map(ConnectorRegistryConverters::toStandardDestinationDefinition).toList();
    final Map<UUID, ActorDefinitionVersion> destinationDefVersions =
        latestDestinations.stream().collect(Collectors.toMap(
            ConnectorRegistryDestinationDefinition::getDestinationDefinitionId,
            ConnectorRegistryConverters::toActorDefinitionVersion));
    return toDestinationDefinitionReadList(destinationDefs, destinationDefVersions);
  }

  public DestinationDefinitionReadList listDestinationDefinitionsForWorkspace(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws IOException {
    final List<StandardDestinationDefinition> destinationDefs = Stream.concat(
        configRepository.listPublicDestinationDefinitions(false).stream(),
        configRepository.listGrantedDestinationDefinitions(workspaceIdRequestBody.getWorkspaceId(), false).stream()).toList();
    final Map<UUID, ActorDefinitionVersion> destinationDefVersionMap = getVersionsForDestinationDefinitions(destinationDefs);
    return toDestinationDefinitionReadList(destinationDefs, destinationDefVersionMap);
  }

  public PrivateDestinationDefinitionReadList listPrivateDestinationDefinitions(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws IOException {
    final List<Entry<StandardDestinationDefinition, Boolean>> standardDestinationDefinitionBooleanMap =
        configRepository.listGrantableDestinationDefinitions(workspaceIdRequestBody.getWorkspaceId(), false);
    final Map<UUID, ActorDefinitionVersion> destinationDefinitionVersionMap =
        getVersionsForDestinationDefinitions(standardDestinationDefinitionBooleanMap.stream().map(Entry::getKey).toList());
    return toPrivateDestinationDefinitionReadList(standardDestinationDefinitionBooleanMap, destinationDefinitionVersionMap);
  }

  private static PrivateDestinationDefinitionReadList toPrivateDestinationDefinitionReadList(
                                                                                             final List<Entry<StandardDestinationDefinition, Boolean>> defs,
                                                                                             final Map<UUID, ActorDefinitionVersion> defIdToVersionMap) {
    final List<PrivateDestinationDefinitionRead> reads = defs.stream()
        .map(entry -> new PrivateDestinationDefinitionRead()
            .destinationDefinition(buildDestinationDefinitionRead(entry.getKey(), defIdToVersionMap.get(entry.getKey().getDestinationDefinitionId())))
            .granted(entry.getValue()))
        .collect(Collectors.toList());
    return new PrivateDestinationDefinitionReadList().destinationDefinitions(reads);
  }

  public DestinationDefinitionRead getDestinationDefinition(final DestinationDefinitionIdRequestBody destinationDefinitionIdRequestBody)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardDestinationDefinition destinationDefinition =
        configRepository.getStandardDestinationDefinition(destinationDefinitionIdRequestBody.getDestinationDefinitionId());
    final ActorDefinitionVersion destinationVersion = configRepository.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId());
    return buildDestinationDefinitionRead(destinationDefinition, destinationVersion);
  }

  public DestinationDefinitionRead getDestinationDefinitionForWorkspace(
                                                                        final DestinationDefinitionIdWithWorkspaceId destinationDefinitionIdWithWorkspaceId)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final UUID definitionId = destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId();
    final UUID workspaceId = destinationDefinitionIdWithWorkspaceId.getWorkspaceId();
    if (!configRepository.workspaceCanUseDefinition(definitionId, workspaceId)) {
      throw new IdNotFoundKnownException("Cannot find the requested definition with given id for this workspace", definitionId.toString());
    }
    return getDestinationDefinition(new DestinationDefinitionIdRequestBody().destinationDefinitionId(definitionId));
  }

  public DestinationDefinitionRead createCustomDestinationDefinition(final CustomDestinationDefinitionCreate customDestinationDefinitionCreate)
      throws IOException {
    final UUID id = uuidSupplier.get();

    final DestinationDefinitionCreate destinationDefCreate = customDestinationDefinitionCreate.getDestinationDefinition();
    final ActorDefinitionVersion actorDefinitionVersion = defaultDefinitionVersionFromCreate(destinationDefCreate)
        .withActorDefinitionId(id);

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(id)
        .withName(destinationDefCreate.getName())
        .withIcon(destinationDefCreate.getIcon())
        .withDockerRepository(actorDefinitionVersion.getDockerRepository())
        .withDockerImageTag(actorDefinitionVersion.getDockerImageTag())
        .withDocumentationUrl(actorDefinitionVersion.getDocumentationUrl())
        .withSpec(actorDefinitionVersion.getSpec())
        .withProtocolVersion(actorDefinitionVersion.getProtocolVersion())
        .withReleaseStage(actorDefinitionVersion.getReleaseStage())
        .withTombstone(false)
        .withPublic(false)
        .withCustom(true)
        .withResourceRequirements(ApiPojoConverters.actorDefResourceReqsToInternal(destinationDefCreate.getResourceRequirements()));

    if (!protocolVersionRange.isSupported(new Version(actorDefinitionVersion.getProtocolVersion()))) {
      throw new UnsupportedProtocolVersionException(actorDefinitionVersion.getProtocolVersion(), protocolVersionRange.min(),
          protocolVersionRange.max());
    }

    configRepository.writeCustomDestinationDefinitionAndDefaultVersion(
        destinationDefinition,
        actorDefinitionVersion,
        customDestinationDefinitionCreate.getWorkspaceId());

    return buildDestinationDefinitionRead(destinationDefinition, actorDefinitionVersion);
  }

  private ActorDefinitionVersion defaultDefinitionVersionFromCreate(final DestinationDefinitionCreate destinationDefCreate) throws IOException {
    final ConnectorSpecification spec = getSpecForImage(
        destinationDefCreate.getDockerRepository(),
        destinationDefCreate.getDockerImageTag(),
        // Only custom connectors can be created via handlers.
        true);

    final Version airbyteProtocolVersion = AirbyteProtocolVersion.getWithDefault(spec.getProtocolVersion());

    return new ActorDefinitionVersion()
        .withDockerImageTag(destinationDefCreate.getDockerImageTag())
        .withDockerRepository(destinationDefCreate.getDockerRepository())
        .withSpec(spec)
        .withDocumentationUrl(destinationDefCreate.getDocumentationUrl().toString())
        .withProtocolVersion(airbyteProtocolVersion.serialize())
        .withReleaseStage(io.airbyte.config.ReleaseStage.CUSTOM);
  }

  public DestinationDefinitionRead updateDestinationDefinition(final DestinationDefinitionUpdate destinationDefinitionUpdate)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardDestinationDefinition currentDestination = configRepository
        .getStandardDestinationDefinition(destinationDefinitionUpdate.getDestinationDefinitionId());
    final ActorDefinitionVersion currentVersion = configRepository.getActorDefinitionVersion(currentDestination.getDefaultVersionId());

    // specs are re-fetched from the container if the image tag has changed, or if the tag is "dev",
    // to allow for easier iteration of dev images
    final boolean specNeedsUpdate = !currentVersion.getDockerImageTag().equals(destinationDefinitionUpdate.getDockerImageTag())
        || ServerConstants.DEV_IMAGE_TAG.equals(destinationDefinitionUpdate.getDockerImageTag());
    final ConnectorSpecification spec = specNeedsUpdate
        ? getSpecForImage(currentVersion.getDockerRepository(), destinationDefinitionUpdate.getDockerImageTag(), currentDestination.getCustom())
        : currentVersion.getSpec();
    final ActorDefinitionResourceRequirements updatedResourceReqs = destinationDefinitionUpdate.getResourceRequirements() != null
        ? ApiPojoConverters.actorDefResourceReqsToInternal(destinationDefinitionUpdate.getResourceRequirements())
        : currentDestination.getResourceRequirements();

    final Version airbyteProtocolVersion = AirbyteProtocolVersion.getWithDefault(spec.getProtocolVersion());
    if (!protocolVersionRange.isSupported(airbyteProtocolVersion)) {
      throw new UnsupportedProtocolVersionException(airbyteProtocolVersion, protocolVersionRange.min(), protocolVersionRange.max());
    }

    final ActorDefinitionVersion newVersion = new ActorDefinitionVersion()
        .withActorDefinitionId(currentVersion.getActorDefinitionId())
        .withDockerRepository(currentVersion.getDockerRepository())
        .withDockerImageTag(destinationDefinitionUpdate.getDockerImageTag())
        .withSpec(spec)
        .withDocumentationUrl(currentVersion.getDocumentationUrl())
        .withProtocolVersion(airbyteProtocolVersion.serialize())
        .withReleaseStage(currentVersion.getReleaseStage())
        .withReleaseDate(currentVersion.getReleaseDate())
        .withNormalizationConfig(currentVersion.getNormalizationConfig())
        .withSupportsDbt(currentVersion.getSupportsDbt())
        .withAllowedHosts(currentVersion.getAllowedHosts());

    final StandardDestinationDefinition newDestination = new StandardDestinationDefinition()
        .withDestinationDefinitionId(currentDestination.getDestinationDefinitionId())
        .withDockerImageTag(newVersion.getDockerImageTag())
        .withDockerRepository(newVersion.getDockerRepository())
        .withName(currentDestination.getName())
        .withDocumentationUrl(newVersion.getDocumentationUrl())
        .withIcon(currentDestination.getIcon())
        .withNormalizationConfig(newVersion.getNormalizationConfig())
        .withSupportsDbt(newVersion.getSupportsDbt())
        .withSpec(newVersion.getSpec())
        .withProtocolVersion(newVersion.getProtocolVersion())
        .withTombstone(currentDestination.getTombstone())
        .withPublic(currentDestination.getPublic())
        .withCustom(currentDestination.getCustom())
        .withReleaseStage(newVersion.getReleaseStage())
        .withReleaseDate(newVersion.getReleaseDate())
        .withResourceRequirements(updatedResourceReqs)
        .withAllowedHosts(newVersion.getAllowedHosts());

    configRepository.writeDestinationDefinitionAndDefaultVersion(newDestination, newVersion);
    configRepository.clearUnsupportedProtocolVersionFlag(newDestination.getDestinationDefinitionId(), ActorType.DESTINATION, protocolVersionRange);
    return buildDestinationDefinitionRead(newDestination, newVersion);
  }

  public void deleteDestinationDefinition(final DestinationDefinitionIdRequestBody destinationDefinitionIdRequestBody)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    // "delete" all destinations associated with the destination definition as well. This will cascade
    // to connections that depend on any deleted
    // destinations. Delete destinations first in case a failure occurs mid-operation.

    final StandardDestinationDefinition persistedDestinationDefinition =
        configRepository.getStandardDestinationDefinition(destinationDefinitionIdRequestBody.getDestinationDefinitionId());

    for (final DestinationRead destinationRead : destinationHandler.listDestinationsForDestinationDefinition(destinationDefinitionIdRequestBody)
        .getDestinations()) {
      destinationHandler.deleteDestination(destinationRead);
    }

    persistedDestinationDefinition.withTombstone(true);
    configRepository.writeStandardDestinationDefinition(persistedDestinationDefinition);
  }

  private ConnectorSpecification getSpecForImage(final String dockerRepository, final String imageTag, final boolean isCustomConnector)
      throws IOException {
    final String imageName = dockerRepository + ":" + imageTag;
    final SynchronousResponse<ConnectorSpecification> getSpecResponse = schedulerSynchronousClient.createGetSpecJob(imageName, isCustomConnector);
    return SpecFetcher.getSpecFromJob(getSpecResponse);
  }

  public static String loadIcon(final String name) {
    try {
      return name == null ? null : MoreResources.readResource("icons/" + name);
    } catch (final Exception e) {
      return null;
    }
  }

  public PrivateDestinationDefinitionRead grantDestinationDefinitionToWorkspace(
                                                                                final DestinationDefinitionIdWithWorkspaceId destinationDefinitionIdWithWorkspaceId)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardDestinationDefinition standardDestinationDefinition =
        configRepository.getStandardDestinationDefinition(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId());
    final ActorDefinitionVersion actorDefinitionVersion =
        configRepository.getActorDefinitionVersion(standardDestinationDefinition.getDefaultVersionId());
    configRepository.writeActorDefinitionWorkspaceGrant(
        destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId(),
        destinationDefinitionIdWithWorkspaceId.getWorkspaceId());
    return new PrivateDestinationDefinitionRead()
        .destinationDefinition(buildDestinationDefinitionRead(standardDestinationDefinition, actorDefinitionVersion))
        .granted(true);
  }

  public void revokeDestinationDefinitionFromWorkspace(final DestinationDefinitionIdWithWorkspaceId destinationDefinitionIdWithWorkspaceId)
      throws IOException {
    configRepository.deleteActorDefinitionWorkspaceGrant(
        destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId(),
        destinationDefinitionIdWithWorkspaceId.getWorkspaceId());
  }

}
