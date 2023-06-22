/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.model.generated.CustomSourceDefinitionCreate;
import io.airbyte.api.model.generated.PrivateSourceDefinitionRead;
import io.airbyte.api.model.generated.PrivateSourceDefinitionReadList;
import io.airbyte.api.model.generated.SourceDefinitionCreate;
import io.airbyte.api.model.generated.SourceDefinitionIdRequestBody;
import io.airbyte.api.model.generated.SourceDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.SourceDefinitionRead;
import io.airbyte.api.model.generated.SourceDefinitionRead.SourceTypeEnum;
import io.airbyte.api.model.generated.SourceDefinitionReadList;
import io.airbyte.api.model.generated.SourceDefinitionUpdate;
import io.airbyte.api.model.generated.SourceRead;
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
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.EnvConfigs;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.helpers.ConnectorRegistryConverters;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * OperationsHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@SuppressWarnings({"PMD.AvoidCatchingNPE", "MissingJavadocMethod"})
@Singleton
public class SourceDefinitionsHandler {

  private final ConfigRepository configRepository;
  private final Supplier<UUID> uuidSupplier;
  private final AirbyteRemoteOssCatalog remoteOssCatalog;
  private final SynchronousSchedulerClient schedulerSynchronousClient;
  private final SourceHandler sourceHandler;
  private final AirbyteProtocolVersionRange protocolVersionRange;

  @Inject
  public SourceDefinitionsHandler(final ConfigRepository configRepository,
                                  final Supplier<UUID> uuidSupplier,
                                  final SynchronousSchedulerClient schedulerSynchronousClient,
                                  final AirbyteRemoteOssCatalog remoteOssCatalog,
                                  final SourceHandler sourceHandler,
                                  final AirbyteProtocolVersionRange protocolVersionRange) {
    this.configRepository = configRepository;
    this.uuidSupplier = uuidSupplier;
    this.schedulerSynchronousClient = schedulerSynchronousClient;
    this.remoteOssCatalog = remoteOssCatalog;
    this.sourceHandler = sourceHandler;
    this.protocolVersionRange = protocolVersionRange;
  }

  // This should be deleted when cloud is migrated to micronaut
  @Deprecated(forRemoval = true)
  public SourceDefinitionsHandler(final ConfigRepository configRepository,
                                  final SynchronousSchedulerClient schedulerSynchronousClient,
                                  final SourceHandler sourceHandler) {
    this.configRepository = configRepository;
    this.uuidSupplier = UUID::randomUUID;
    this.schedulerSynchronousClient = schedulerSynchronousClient;
    this.remoteOssCatalog = new AirbyteRemoteOssCatalog();
    this.sourceHandler = sourceHandler;
    final Configs configs = new EnvConfigs();
    this.protocolVersionRange = new AirbyteProtocolVersionRange(configs.getAirbyteProtocolVersionMin(), configs.getAirbyteProtocolVersionMax());
  }

  @VisibleForTesting
  static SourceDefinitionRead buildSourceDefinitionRead(final StandardSourceDefinition standardSourceDefinition,
                                                        final ActorDefinitionVersion sourceVersion) {
    try {
      return new SourceDefinitionRead()
          .sourceDefinitionId(standardSourceDefinition.getSourceDefinitionId())
          .name(standardSourceDefinition.getName())
          .sourceType(getSourceType(standardSourceDefinition))
          .dockerRepository(sourceVersion.getDockerRepository())
          .dockerImageTag(sourceVersion.getDockerImageTag())
          .documentationUrl(new URI(sourceVersion.getDocumentationUrl()))
          .icon(loadIcon(standardSourceDefinition.getIcon()))
          .protocolVersion(sourceVersion.getProtocolVersion())
          .releaseStage(ApiPojoConverters.toApiReleaseStage(sourceVersion.getReleaseStage()))
          .releaseDate(ApiPojoConverters.toLocalDate(sourceVersion.getReleaseDate()))
          .resourceRequirements(ApiPojoConverters.actorDefResourceReqsToApi(standardSourceDefinition.getResourceRequirements()))
          .maxSecondsBetweenMessages(standardSourceDefinition.getMaxSecondsBetweenMessages());

    } catch (final URISyntaxException | NullPointerException e) {
      throw new InternalServerKnownException("Unable to process retrieved latest source definitions list", e);
    }
  }

  private static SourceTypeEnum getSourceType(final StandardSourceDefinition standardSourceDefinition) {
    if (standardSourceDefinition.getSourceType() == null) {
      return null;
    }
    return SourceTypeEnum.fromValue(standardSourceDefinition.getSourceType().value());
  }

  public SourceDefinitionReadList listSourceDefinitions() throws IOException {
    final List<StandardSourceDefinition> standardSourceDefinitions = configRepository.listStandardSourceDefinitions(false);
    final Map<UUID, ActorDefinitionVersion> sourceDefinitionVersionMap = getVersionsForSourceDefinitions(standardSourceDefinitions);
    return toSourceDefinitionReadList(standardSourceDefinitions, sourceDefinitionVersionMap);
  }

  private Map<UUID, ActorDefinitionVersion> getVersionsForSourceDefinitions(final List<StandardSourceDefinition> sourceDefinitions)
      throws IOException {
    return configRepository.getActorDefinitionVersions(sourceDefinitions
        .stream()
        .map(StandardSourceDefinition::getDefaultVersionId)
        .collect(Collectors.toList()))
        .stream().collect(Collectors.toMap(ActorDefinitionVersion::getActorDefinitionId, v -> v));
  }

  private static SourceDefinitionReadList toSourceDefinitionReadList(final List<StandardSourceDefinition> defs,
                                                                     final Map<UUID, ActorDefinitionVersion> defIdToVersionMap) {
    final List<SourceDefinitionRead> reads = defs.stream()
        .map(d -> buildSourceDefinitionRead(d, defIdToVersionMap.get(d.getSourceDefinitionId())))
        .collect(Collectors.toList());
    return new SourceDefinitionReadList().sourceDefinitions(reads);
  }

  public SourceDefinitionReadList listLatestSourceDefinitions() {
    final List<ConnectorRegistrySourceDefinition> latestSources = remoteOssCatalog.getSourceDefinitions();
    final List<StandardSourceDefinition> sourceDefs = latestSources.stream().map(ConnectorRegistryConverters::toStandardSourceDefinition).toList();
    final Map<UUID, ActorDefinitionVersion> sourceDefVersionMap = latestSources
        .stream().collect(Collectors.toMap(
            ConnectorRegistrySourceDefinition::getSourceDefinitionId,
            ConnectorRegistryConverters::toActorDefinitionVersion));
    return toSourceDefinitionReadList(sourceDefs, sourceDefVersionMap);
  }

  public SourceDefinitionReadList listSourceDefinitionsForWorkspace(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws IOException {
    final List<StandardSourceDefinition> sourceDefs = Stream.concat(
        configRepository.listPublicSourceDefinitions(false).stream(),
        configRepository.listGrantedSourceDefinitions(workspaceIdRequestBody.getWorkspaceId(), false).stream()).toList();
    final Map<UUID, ActorDefinitionVersion> sourceDefVersionMap = getVersionsForSourceDefinitions(sourceDefs);
    return toSourceDefinitionReadList(sourceDefs, sourceDefVersionMap);
  }

  public PrivateSourceDefinitionReadList listPrivateSourceDefinitions(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws IOException {
    final List<Entry<StandardSourceDefinition, Boolean>> standardSourceDefinitionBooleanMap =
        configRepository.listGrantableSourceDefinitions(workspaceIdRequestBody.getWorkspaceId(), false);
    final Map<UUID, ActorDefinitionVersion> sourceDefinitionVersionMap =
        getVersionsForSourceDefinitions(standardSourceDefinitionBooleanMap.stream().map(Entry::getKey).toList());
    return toPrivateSourceDefinitionReadList(standardSourceDefinitionBooleanMap, sourceDefinitionVersionMap);
  }

  private static PrivateSourceDefinitionReadList toPrivateSourceDefinitionReadList(final List<Entry<StandardSourceDefinition, Boolean>> defs,
                                                                                   final Map<UUID, ActorDefinitionVersion> defIdToVersionMap) {
    final List<PrivateSourceDefinitionRead> reads = defs.stream()
        .map(entry -> new PrivateSourceDefinitionRead()
            .sourceDefinition(buildSourceDefinitionRead(entry.getKey(), defIdToVersionMap.get(entry.getKey().getSourceDefinitionId())))
            .granted(entry.getValue()))
        .collect(Collectors.toList());
    return new PrivateSourceDefinitionReadList().sourceDefinitions(reads);
  }

  public SourceDefinitionRead getSourceDefinition(final SourceDefinitionIdRequestBody sourceDefinitionIdRequestBody)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardSourceDefinition sourceDefinition =
        configRepository.getStandardSourceDefinition(sourceDefinitionIdRequestBody.getSourceDefinitionId());
    final ActorDefinitionVersion sourceVersion = configRepository.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId());
    return buildSourceDefinitionRead(sourceDefinition, sourceVersion);
  }

  public SourceDefinitionRead getSourceDefinitionForWorkspace(final SourceDefinitionIdWithWorkspaceId sourceDefinitionIdWithWorkspaceId)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final UUID definitionId = sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId();
    final UUID workspaceId = sourceDefinitionIdWithWorkspaceId.getWorkspaceId();
    if (!configRepository.workspaceCanUseDefinition(definitionId, workspaceId)) {
      throw new IdNotFoundKnownException("Cannot find the requested definition with given id for this workspace", definitionId.toString());
    }
    return getSourceDefinition(new SourceDefinitionIdRequestBody().sourceDefinitionId(definitionId));
  }

  public SourceDefinitionRead createCustomSourceDefinition(final CustomSourceDefinitionCreate customSourceDefinitionCreate)
      throws IOException {
    final UUID id = uuidSupplier.get();
    final SourceDefinitionCreate sourceDefinitionCreate = customSourceDefinitionCreate.getSourceDefinition();
    final ActorDefinitionVersion actorDefinitionVersion = defaultDefinitionVersionFromCreate(sourceDefinitionCreate)
        .withActorDefinitionId(id);

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(id)
        .withName(sourceDefinitionCreate.getName())
        .withIcon(sourceDefinitionCreate.getIcon())
        .withDockerRepository(actorDefinitionVersion.getDockerRepository())
        .withDockerImageTag(actorDefinitionVersion.getDockerImageTag())
        .withDocumentationUrl(actorDefinitionVersion.getDocumentationUrl())
        .withSpec(actorDefinitionVersion.getSpec())
        .withProtocolVersion(actorDefinitionVersion.getProtocolVersion())
        .withReleaseStage(actorDefinitionVersion.getReleaseStage())
        .withTombstone(false)
        .withPublic(false)
        .withCustom(true)
        .withResourceRequirements(ApiPojoConverters.actorDefResourceReqsToInternal(sourceDefinitionCreate.getResourceRequirements()));

    if (!protocolVersionRange.isSupported(new Version(actorDefinitionVersion.getProtocolVersion()))) {
      throw new UnsupportedProtocolVersionException(actorDefinitionVersion.getProtocolVersion(), protocolVersionRange.min(),
          protocolVersionRange.max());
    }
    configRepository.writeCustomSourceDefinitionAndDefaultVersion(sourceDefinition, actorDefinitionVersion,
        customSourceDefinitionCreate.getWorkspaceId());

    return buildSourceDefinitionRead(sourceDefinition, actorDefinitionVersion);
  }

  private ActorDefinitionVersion defaultDefinitionVersionFromCreate(final SourceDefinitionCreate sourceDefinitionCreate) throws IOException {
    final ConnectorSpecification spec =
        getSpecForImage(
            sourceDefinitionCreate.getDockerRepository(),
            sourceDefinitionCreate.getDockerImageTag(),
            // Only custom connectors can be created via handlers.
            true);

    final Version airbyteProtocolVersion = AirbyteProtocolVersion.getWithDefault(spec.getProtocolVersion());
    return new ActorDefinitionVersion()
        .withDockerImageTag(sourceDefinitionCreate.getDockerImageTag())
        .withDockerRepository(sourceDefinitionCreate.getDockerRepository())
        .withSpec(spec)
        .withDocumentationUrl(sourceDefinitionCreate.getDocumentationUrl().toString())
        .withProtocolVersion(airbyteProtocolVersion.serialize())
        .withReleaseStage(io.airbyte.config.ReleaseStage.CUSTOM);
  }

  public SourceDefinitionRead updateSourceDefinition(final SourceDefinitionUpdate sourceDefinitionUpdate)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardSourceDefinition currentSourceDefinition =
        configRepository.getStandardSourceDefinition(sourceDefinitionUpdate.getSourceDefinitionId());
    final ActorDefinitionVersion currentVersion = configRepository.getActorDefinitionVersion(currentSourceDefinition.getDefaultVersionId());

    // specs are re-fetched from the container if the image tag has changed, or if the tag is "dev",
    // to allow for easier iteration of dev images
    final boolean specNeedsUpdate = !currentVersion.getDockerImageTag().equals(sourceDefinitionUpdate.getDockerImageTag())
        || ServerConstants.DEV_IMAGE_TAG.equals(sourceDefinitionUpdate.getDockerImageTag());
    final ConnectorSpecification spec = specNeedsUpdate
        ? getSpecForImage(currentVersion.getDockerRepository(), sourceDefinitionUpdate.getDockerImageTag(),
            currentSourceDefinition.getCustom())
        : currentVersion.getSpec();
    final ActorDefinitionResourceRequirements updatedResourceReqs = sourceDefinitionUpdate.getResourceRequirements() != null
        ? ApiPojoConverters.actorDefResourceReqsToInternal(sourceDefinitionUpdate.getResourceRequirements())
        : currentSourceDefinition.getResourceRequirements();

    final Version airbyteProtocolVersion = AirbyteProtocolVersion.getWithDefault(spec.getProtocolVersion());
    if (!protocolVersionRange.isSupported(airbyteProtocolVersion)) {
      throw new UnsupportedProtocolVersionException(airbyteProtocolVersion, protocolVersionRange.min(), protocolVersionRange.max());
    }

    final ActorDefinitionVersion newVersion = new ActorDefinitionVersion()
        .withActorDefinitionId(currentVersion.getActorDefinitionId())
        .withDockerRepository(currentVersion.getDockerRepository())
        .withDockerImageTag(sourceDefinitionUpdate.getDockerImageTag())
        .withSpec(spec)
        .withDocumentationUrl(currentVersion.getDocumentationUrl())
        .withProtocolVersion(airbyteProtocolVersion.serialize())
        .withReleaseStage(currentVersion.getReleaseStage())
        .withReleaseDate(currentVersion.getReleaseDate())
        .withSuggestedStreams(currentVersion.getSuggestedStreams())
        .withAllowedHosts(currentVersion.getAllowedHosts());

    final StandardSourceDefinition newSource = new StandardSourceDefinition()
        .withSourceDefinitionId(currentSourceDefinition.getSourceDefinitionId())
        .withDockerImageTag(sourceDefinitionUpdate.getDockerImageTag())
        .withDockerRepository(newVersion.getDockerRepository())
        .withDocumentationUrl(newVersion.getDocumentationUrl())
        .withName(currentSourceDefinition.getName())
        .withIcon(currentSourceDefinition.getIcon())
        .withSpec(newVersion.getSpec())
        .withProtocolVersion(newVersion.getProtocolVersion())
        .withTombstone(currentSourceDefinition.getTombstone())
        .withPublic(currentSourceDefinition.getPublic())
        .withCustom(currentSourceDefinition.getCustom())
        .withReleaseStage(newVersion.getReleaseStage())
        .withReleaseDate(newVersion.getReleaseDate())
        .withSuggestedStreams(newVersion.getSuggestedStreams())
        .withAllowedHosts(newVersion.getAllowedHosts())
        .withMaxSecondsBetweenMessages(currentSourceDefinition.getMaxSecondsBetweenMessages())
        .withResourceRequirements(updatedResourceReqs);

    configRepository.writeSourceDefinitionAndDefaultVersion(newSource, newVersion);
    configRepository.clearUnsupportedProtocolVersionFlag(newSource.getSourceDefinitionId(), ActorType.SOURCE, protocolVersionRange);

    return buildSourceDefinitionRead(newSource, newVersion);
  }

  public void deleteSourceDefinition(final SourceDefinitionIdRequestBody sourceDefinitionIdRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    // "delete" all sources associated with the source definition as well. This will cascade to
    // connections that depend on any deleted sources.
    // Delete sources first in case a failure occurs mid-operation.

    final StandardSourceDefinition persistedSourceDefinition =
        configRepository.getStandardSourceDefinition(sourceDefinitionIdRequestBody.getSourceDefinitionId());

    for (final SourceRead sourceRead : sourceHandler.listSourcesForSourceDefinition(sourceDefinitionIdRequestBody).getSources()) {
      sourceHandler.deleteSource(sourceRead);
    }

    persistedSourceDefinition.withTombstone(true);
    configRepository.writeStandardSourceDefinition(persistedSourceDefinition);
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

  public PrivateSourceDefinitionRead grantSourceDefinitionToWorkspace(
                                                                      final SourceDefinitionIdWithWorkspaceId sourceDefinitionIdWithWorkspaceId)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardSourceDefinition standardSourceDefinition =
        configRepository.getStandardSourceDefinition(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId());
    final ActorDefinitionVersion actorDefinitionVersion = configRepository.getActorDefinitionVersion(standardSourceDefinition.getDefaultVersionId());
    configRepository.writeActorDefinitionWorkspaceGrant(
        sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId(),
        sourceDefinitionIdWithWorkspaceId.getWorkspaceId());
    return new PrivateSourceDefinitionRead()
        .sourceDefinition(buildSourceDefinitionRead(standardSourceDefinition, actorDefinitionVersion))
        .granted(true);
  }

  public void revokeSourceDefinitionFromWorkspace(final SourceDefinitionIdWithWorkspaceId sourceDefinitionIdWithWorkspaceId)
      throws IOException {
    configRepository.deleteActorDefinitionWorkspaceGrant(
        sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId(),
        sourceDefinitionIdWithWorkspaceId.getWorkspaceId());
  }

}
