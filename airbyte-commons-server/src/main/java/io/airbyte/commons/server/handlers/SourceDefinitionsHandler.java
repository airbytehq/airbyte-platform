/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.featureflag.ContextKt.ANONYMOUS;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.model.generated.ActorDefinitionIdWithScope;
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
import io.airbyte.commons.lang.Exceptions;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.errors.IdNotFoundKnownException;
import io.airbyte.commons.server.errors.InternalServerKnownException;
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionResourceRequirements;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.ScopeType;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.helpers.ConnectorRegistryConverters;
import io.airbyte.config.init.SupportStateUpdater;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.specs.RemoteDefinitionsProvider;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HideActorDefinitionFromList;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.RunSupportStateUpdater;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.Workspace;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
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
@SuppressWarnings("PMD.AvoidCatchingNPE")
@Singleton
public class SourceDefinitionsHandler {

  private final ConfigRepository configRepository;
  private final Supplier<UUID> uuidSupplier;
  private final RemoteDefinitionsProvider remoteDefinitionsProvider;
  private final ActorDefinitionHandlerHelper actorDefinitionHandlerHelper;
  private final SourceHandler sourceHandler;
  private final SupportStateUpdater supportStateUpdater;
  private final FeatureFlagClient featureFlagClient;

  @Inject
  public SourceDefinitionsHandler(final ConfigRepository configRepository,
                                  @Named("uuidGenerator") final Supplier<UUID> uuidSupplier,
                                  final ActorDefinitionHandlerHelper actorDefinitionHandlerHelper,
                                  final RemoteDefinitionsProvider remoteDefinitionsProvider,
                                  final SourceHandler sourceHandler,
                                  final SupportStateUpdater supportStateUpdater,
                                  final FeatureFlagClient featureFlagClient) {
    this.configRepository = configRepository;
    this.uuidSupplier = uuidSupplier;
    this.actorDefinitionHandlerHelper = actorDefinitionHandlerHelper;
    this.remoteDefinitionsProvider = remoteDefinitionsProvider;
    this.sourceHandler = sourceHandler;
    this.supportStateUpdater = supportStateUpdater;
    this.featureFlagClient = featureFlagClient;
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
          .supportLevel(ApiPojoConverters.toApiSupportLevel(sourceVersion.getSupportLevel()))
          .releaseStage(ApiPojoConverters.toApiReleaseStage(sourceVersion.getReleaseStage()))
          .releaseDate(ApiPojoConverters.toLocalDate(sourceVersion.getReleaseDate()))
          .custom(standardSourceDefinition.getCustom())
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
    // Swallow exceptions when fetching registry, so we don't hard-fail for airgapped deployments.
    final List<ConnectorRegistrySourceDefinition> latestSources =
        Exceptions.swallowWithDefault(remoteDefinitionsProvider::getSourceDefinitions, Collections.emptyList());
    final List<StandardSourceDefinition> sourceDefs = latestSources.stream().map(ConnectorRegistryConverters::toStandardSourceDefinition).toList();

    final Map<UUID, ActorDefinitionVersion> sourceDefVersionMap =
        latestSources.stream().collect(Collectors.toMap(
            ConnectorRegistrySourceDefinition::getSourceDefinitionId,
            destination -> Exceptions.swallowWithDefault(
                () -> ConnectorRegistryConverters.toActorDefinitionVersion(destination), null)));

    // filter out any destination definitions with no corresponding version
    final List<StandardSourceDefinition> validSourceDefs = sourceDefs.stream()
        .filter(s -> sourceDefVersionMap.get(s.getSourceDefinitionId()) != null)
        .toList();

    return toSourceDefinitionReadList(validSourceDefs, sourceDefVersionMap);
  }

  public SourceDefinitionReadList listSourceDefinitionsForWorkspace(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws IOException {
    final List<StandardSourceDefinition> sourceDefs = Stream.concat(
        configRepository.listPublicSourceDefinitions(false).stream(),
        configRepository.listGrantedSourceDefinitions(workspaceIdRequestBody.getWorkspaceId(), false).stream()).toList();

    // Hide source definitions from the list via feature flag
    final List<StandardSourceDefinition> shownSourceDefs = sourceDefs.stream().filter((sourceDefinition) -> !featureFlagClient.boolVariation(
        HideActorDefinitionFromList.INSTANCE,
        new Multi(List.of(new SourceDefinition(sourceDefinition.getSourceDefinitionId()), new Workspace(workspaceIdRequestBody.getWorkspaceId())))))
        .toList();

    final Map<UUID, ActorDefinitionVersion> sourceDefVersionMap = getVersionsForSourceDefinitions(shownSourceDefs);
    return toSourceDefinitionReadList(shownSourceDefs, sourceDefVersionMap);
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

  public SourceDefinitionRead getSourceDefinitionForScope(final ActorDefinitionIdWithScope actorDefinitionIdWithScope)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final UUID definitionId = actorDefinitionIdWithScope.getActorDefinitionId();
    final UUID scopeId = actorDefinitionIdWithScope.getScopeId();
    final ScopeType scopeType = ScopeType.fromValue(actorDefinitionIdWithScope.getScopeType().toString());
    if (!configRepository.scopeCanUseDefinition(definitionId, scopeId, scopeType.value())) {
      final String message = String.format("Cannot find the requested definition with given id for this %s", scopeType);
      throw new IdNotFoundKnownException(message, definitionId.toString());
    }
    return getSourceDefinition(new SourceDefinitionIdRequestBody().sourceDefinitionId(definitionId));
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

  public SourceDefinitionRead createCustomSourceDefinition(final CustomSourceDefinitionCreate customSourceDefinitionCreate) throws IOException {
    final UUID id = uuidSupplier.get();
    final SourceDefinitionCreate sourceDefinitionCreate = customSourceDefinitionCreate.getSourceDefinition();
    final ActorDefinitionVersion actorDefinitionVersion =
        actorDefinitionHandlerHelper
            .defaultDefinitionVersionFromCreate(sourceDefinitionCreate.getDockerRepository(), sourceDefinitionCreate.getDockerImageTag(),
                sourceDefinitionCreate.getDocumentationUrl(), customSourceDefinitionCreate.getWorkspaceId())
            .withActorDefinitionId(id);

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(id)
        .withName(sourceDefinitionCreate.getName())
        .withIcon(sourceDefinitionCreate.getIcon())
        .withTombstone(false)
        .withPublic(false)
        .withCustom(true)
        .withResourceRequirements(ApiPojoConverters.actorDefResourceReqsToInternal(sourceDefinitionCreate.getResourceRequirements()));

    // legacy call; todo: remove once we drop workspace_id column
    if (customSourceDefinitionCreate.getWorkspaceId() != null) {
      configRepository.writeCustomConnectorMetadata(sourceDefinition, actorDefinitionVersion,
          customSourceDefinitionCreate.getWorkspaceId(), ScopeType.WORKSPACE);
    } else {
      configRepository.writeCustomConnectorMetadata(sourceDefinition, actorDefinitionVersion,
          customSourceDefinitionCreate.getScopeId(), ScopeType.fromValue(customSourceDefinitionCreate.getScopeType().toString()));
    }

    return buildSourceDefinitionRead(sourceDefinition, actorDefinitionVersion);
  }

  public SourceDefinitionRead updateSourceDefinition(final SourceDefinitionUpdate sourceDefinitionUpdate)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardSourceDefinition currentSourceDefinition =
        configRepository.getStandardSourceDefinition(sourceDefinitionUpdate.getSourceDefinitionId());
    final ActorDefinitionVersion currentVersion = configRepository.getActorDefinitionVersion(currentSourceDefinition.getDefaultVersionId());

    final ActorDefinitionResourceRequirements updatedResourceReqs = sourceDefinitionUpdate.getResourceRequirements() != null
        ? ApiPojoConverters.actorDefResourceReqsToInternal(sourceDefinitionUpdate.getResourceRequirements())
        : currentSourceDefinition.getResourceRequirements();

    final StandardSourceDefinition newSource = new StandardSourceDefinition()
        .withSourceDefinitionId(currentSourceDefinition.getSourceDefinitionId())
        .withName(currentSourceDefinition.getName())
        .withIcon(currentSourceDefinition.getIcon())
        .withTombstone(currentSourceDefinition.getTombstone())
        .withPublic(currentSourceDefinition.getPublic())
        .withCustom(currentSourceDefinition.getCustom())
        .withMaxSecondsBetweenMessages(currentSourceDefinition.getMaxSecondsBetweenMessages())
        .withResourceRequirements(updatedResourceReqs);

    final ActorDefinitionVersion newVersion = actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
        currentVersion, ActorType.SOURCE, sourceDefinitionUpdate.getDockerImageTag(), currentSourceDefinition.getCustom());

    final List<ActorDefinitionBreakingChange> breakingChangesForDef = actorDefinitionHandlerHelper.getBreakingChanges(newVersion, ActorType.SOURCE);
    configRepository.writeConnectorMetadata(newSource, newVersion, breakingChangesForDef);

    if (featureFlagClient.boolVariation(RunSupportStateUpdater.INSTANCE, new Workspace(ANONYMOUS))) {
      final StandardSourceDefinition updatedSourceDefinition = configRepository.getStandardSourceDefinition(newSource.getSourceDefinitionId());
      supportStateUpdater.updateSupportStatesForSourceDefinition(updatedSourceDefinition);
    }
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
    configRepository.updateStandardSourceDefinition(persistedSourceDefinition);
  }

  public static String loadIcon(final String name) {
    try {
      return name == null ? null : MoreResources.readResource("icons/" + name);
    } catch (final Exception e) {
      return null;
    }
  }

  public PrivateSourceDefinitionRead grantSourceDefinitionToWorkspaceOrOrganization(final ActorDefinitionIdWithScope actorDefinitionIdWithScope)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardSourceDefinition standardSourceDefinition =
        configRepository.getStandardSourceDefinition(actorDefinitionIdWithScope.getActorDefinitionId());
    final ActorDefinitionVersion actorDefinitionVersion = configRepository.getActorDefinitionVersion(standardSourceDefinition.getDefaultVersionId());
    configRepository.writeActorDefinitionWorkspaceGrant(
        actorDefinitionIdWithScope.getActorDefinitionId(),
        actorDefinitionIdWithScope.getScopeId(),
        ScopeType.fromValue(actorDefinitionIdWithScope.getScopeType().toString()));
    return new PrivateSourceDefinitionRead()
        .sourceDefinition(buildSourceDefinitionRead(standardSourceDefinition, actorDefinitionVersion))
        .granted(true);
  }

  public void revokeSourceDefinition(final ActorDefinitionIdWithScope actorDefinitionIdWithScope)
      throws IOException {
    configRepository.deleteActorDefinitionWorkspaceGrant(
        actorDefinitionIdWithScope.getActorDefinitionId(),
        actorDefinitionIdWithScope.getScopeId(),
        ScopeType.fromValue(actorDefinitionIdWithScope.getScopeType().toString()));
  }

}
