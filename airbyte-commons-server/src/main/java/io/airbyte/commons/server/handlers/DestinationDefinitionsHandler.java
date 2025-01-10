/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.model.generated.ActorDefinitionIdWithScope;
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
import io.airbyte.api.problems.model.generated.ProblemMessageData;
import io.airbyte.api.problems.throwable.generated.BadRequestProblem;
import io.airbyte.commons.lang.Exceptions;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.errors.IdNotFoundKnownException;
import io.airbyte.commons.server.errors.InternalServerKnownException;
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionResourceRequirements;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ScopeType;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.helpers.ConnectorRegistryConverters;
import io.airbyte.config.init.AirbyteCompatibleConnectorsValidator;
import io.airbyte.config.init.ConnectorPlatformCompatibilityValidationResult;
import io.airbyte.config.init.SupportStateUpdater;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.specs.RemoteDefinitionsProvider;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.featureflag.DestinationDefinition;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HideActorDefinitionFromList;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.Workspace;
import io.airbyte.validation.json.JsonValidationException;
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
 * DestinationDefinitionsHandler. Javadocs suppressed because api docs should be used as source of
 * truth.
 */
@SuppressWarnings({"PMD.AvoidCatchingNPE", "LineLength"})
@Singleton
public class DestinationDefinitionsHandler {

  private final ActorDefinitionService actorDefinitionService;
  private final Supplier<UUID> uuidSupplier;
  private final ActorDefinitionHandlerHelper actorDefinitionHandlerHelper;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final RemoteDefinitionsProvider remoteDefinitionsProvider;
  private final DestinationHandler destinationHandler;
  private final SupportStateUpdater supportStateUpdater;
  private final FeatureFlagClient featureFlagClient;
  private final AirbyteCompatibleConnectorsValidator airbyteCompatibleConnectorsValidator;

  private final DestinationService destinationService;
  private final WorkspaceService workspaceService;
  private final ApiPojoConverters apiPojoConverters;

  @VisibleForTesting
  public DestinationDefinitionsHandler(final ActorDefinitionService actorDefinitionService,
                                       @Named("uuidGenerator") final Supplier<UUID> uuidSupplier,
                                       final ActorDefinitionHandlerHelper actorDefinitionHandlerHelper,
                                       final RemoteDefinitionsProvider remoteDefinitionsProvider,
                                       final DestinationHandler destinationHandler,
                                       final SupportStateUpdater supportStateUpdater,
                                       final FeatureFlagClient featureFlagClient,
                                       final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                                       final AirbyteCompatibleConnectorsValidator airbyteCompatibleConnectorsValidator,
                                       final DestinationService destinationService,
                                       final WorkspaceService workspaceService,
                                       final ApiPojoConverters apiPojoConverters) {
    this.actorDefinitionService = actorDefinitionService;
    this.uuidSupplier = uuidSupplier;
    this.actorDefinitionHandlerHelper = actorDefinitionHandlerHelper;
    this.remoteDefinitionsProvider = remoteDefinitionsProvider;
    this.destinationHandler = destinationHandler;
    this.supportStateUpdater = supportStateUpdater;
    this.featureFlagClient = featureFlagClient;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.airbyteCompatibleConnectorsValidator = airbyteCompatibleConnectorsValidator;
    this.destinationService = destinationService;
    this.workspaceService = workspaceService;
    this.apiPojoConverters = apiPojoConverters;
  }

  public DestinationDefinitionRead buildDestinationDefinitionRead(final UUID destinationDefinitionId)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardDestinationDefinition destinationDefinition =
        destinationService.getStandardDestinationDefinition(destinationDefinitionId);
    final ActorDefinitionVersion destinationVersion = actorDefinitionService.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId());
    return buildDestinationDefinitionRead(destinationDefinition, destinationVersion);
  }

  @VisibleForTesting
  DestinationDefinitionRead buildDestinationDefinitionRead(final StandardDestinationDefinition standardDestinationDefinition,
                                                           final ActorDefinitionVersion destinationVersion) {
    try {
      return new DestinationDefinitionRead()
          .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
          .name(standardDestinationDefinition.getName())
          .dockerRepository(destinationVersion.getDockerRepository())
          .dockerImageTag(destinationVersion.getDockerImageTag())
          .documentationUrl(new URI(destinationVersion.getDocumentationUrl()))
          .icon(standardDestinationDefinition.getIconUrl())
          .protocolVersion(destinationVersion.getProtocolVersion())
          .supportLevel(apiPojoConverters.toApiSupportLevel(destinationVersion.getSupportLevel()))
          .releaseStage(apiPojoConverters.toApiReleaseStage(destinationVersion.getReleaseStage()))
          .releaseDate(apiPojoConverters.toLocalDate(destinationVersion.getReleaseDate()))
          .lastPublished(apiPojoConverters.toOffsetDateTime(destinationVersion.getLastPublished()))
          .cdkVersion(destinationVersion.getCdkVersion())
          .metrics(standardDestinationDefinition.getMetrics())
          .custom(standardDestinationDefinition.getCustom())
          .resourceRequirements(apiPojoConverters.actorDefResourceReqsToApi(standardDestinationDefinition.getResourceRequirements()))
          .language(destinationVersion.getLanguage());
    } catch (final URISyntaxException | NullPointerException e) {
      throw new InternalServerKnownException("Unable to process retrieved latest destination definitions list", e);
    }
  }

  public DestinationDefinitionReadList listDestinationDefinitions() throws IOException {
    final List<StandardDestinationDefinition> standardDestinationDefinitions = destinationService.listStandardDestinationDefinitions(false);
    final Map<UUID, ActorDefinitionVersion> destinationDefinitionVersionMap = getVersionsForDestinationDefinitions(standardDestinationDefinitions);
    return toDestinationDefinitionReadList(standardDestinationDefinitions, destinationDefinitionVersionMap);
  }

  private DestinationDefinitionReadList toDestinationDefinitionReadList(final List<StandardDestinationDefinition> defs,
                                                                        final Map<UUID, ActorDefinitionVersion> defIdToVersionMap) {
    final List<DestinationDefinitionRead> reads = defs.stream()
        .map(d -> buildDestinationDefinitionRead(d, defIdToVersionMap.get(d.getDestinationDefinitionId())))
        .collect(Collectors.toList());
    return new DestinationDefinitionReadList().destinationDefinitions(reads);
  }

  private Map<UUID, ActorDefinitionVersion> getVersionsForDestinationDefinitions(final List<StandardDestinationDefinition> destinationDefinitions)
      throws IOException {
    return actorDefinitionService.getActorDefinitionVersions(destinationDefinitions
        .stream()
        .map(StandardDestinationDefinition::getDefaultVersionId)
        .collect(Collectors.toList()))
        .stream().collect(Collectors.toMap(ActorDefinitionVersion::getActorDefinitionId, v -> v));
  }

  public DestinationDefinitionReadList listLatestDestinationDefinitions() {
    // Swallow exceptions when fetching registry, so we don't hard-fail for airgapped deployments.
    final List<ConnectorRegistryDestinationDefinition> latestDestinations =
        Exceptions.swallowWithDefault(remoteDefinitionsProvider::getDestinationDefinitions, Collections.emptyList());
    final List<StandardDestinationDefinition> destinationDefs =
        latestDestinations.stream().map(ConnectorRegistryConverters::toStandardDestinationDefinition).toList();

    final Map<UUID, ActorDefinitionVersion> destinationDefVersions =
        latestDestinations.stream().collect(Collectors.toMap(
            ConnectorRegistryDestinationDefinition::getDestinationDefinitionId,
            destination -> Exceptions.swallowWithDefault(
                () -> ConnectorRegistryConverters.toActorDefinitionVersion(destination), null)));

    // filter out any destination definitions with no corresponding version
    final List<StandardDestinationDefinition> validDestinationDefs = destinationDefs.stream()
        .filter(d -> destinationDefVersions.get(d.getDestinationDefinitionId()) != null)
        .toList();

    return toDestinationDefinitionReadList(validDestinationDefs, destinationDefVersions);
  }

  public DestinationDefinitionReadList listDestinationDefinitionsForWorkspace(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws IOException {
    final List<StandardDestinationDefinition> destinationDefs = Stream.concat(
        destinationService.listPublicDestinationDefinitions(false).stream(),
        destinationService.listGrantedDestinationDefinitions(workspaceIdRequestBody.getWorkspaceId(), false).stream()).toList();

    // Hide destination definitions from the list via feature flag
    final List<StandardDestinationDefinition> shownDestinationDefs = destinationDefs
        .stream().filter((destinationDefinition) -> !featureFlagClient.boolVariation(
            HideActorDefinitionFromList.INSTANCE,
            new Multi(List.of(new DestinationDefinition(destinationDefinition.getDestinationDefinitionId()),
                new Workspace(workspaceIdRequestBody.getWorkspaceId())))))
        .toList();

    final Map<UUID, ActorDefinitionVersion> sourceDefVersionMap =
        actorDefinitionVersionHelper.getDestinationVersions(shownDestinationDefs, workspaceIdRequestBody.getWorkspaceId());
    return toDestinationDefinitionReadList(shownDestinationDefs, sourceDefVersionMap);
  }

  public PrivateDestinationDefinitionReadList listPrivateDestinationDefinitions(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws IOException {
    final List<Entry<StandardDestinationDefinition, Boolean>> standardDestinationDefinitionBooleanMap =
        destinationService.listGrantableDestinationDefinitions(workspaceIdRequestBody.getWorkspaceId(), false);
    final Map<UUID, ActorDefinitionVersion> destinationDefinitionVersionMap =
        getVersionsForDestinationDefinitions(standardDestinationDefinitionBooleanMap.stream().map(Entry::getKey).toList());
    return toPrivateDestinationDefinitionReadList(standardDestinationDefinitionBooleanMap, destinationDefinitionVersionMap);
  }

  public DestinationDefinitionReadList listPublicDestinationDefinitions() throws IOException {
    final List<StandardDestinationDefinition> standardDestinationDefinitions = destinationService.listPublicDestinationDefinitions(false);
    final Map<UUID, ActorDefinitionVersion> destinationDefinitionVersionMap = getVersionsForDestinationDefinitions(standardDestinationDefinitions);
    return toDestinationDefinitionReadList(standardDestinationDefinitions, destinationDefinitionVersionMap);
  }

  private PrivateDestinationDefinitionReadList toPrivateDestinationDefinitionReadList(
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
    return buildDestinationDefinitionRead(destinationDefinitionIdRequestBody.getDestinationDefinitionId());
  }

  public DestinationDefinitionRead getDestinationDefinitionForWorkspace(
                                                                        final DestinationDefinitionIdWithWorkspaceId destinationDefinitionIdWithWorkspaceId)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final UUID definitionId = destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId();
    final UUID workspaceId = destinationDefinitionIdWithWorkspaceId.getWorkspaceId();
    if (!workspaceService.workspaceCanUseDefinition(definitionId, workspaceId)) {
      throw new IdNotFoundKnownException("Cannot find the requested definition with given id for this workspace", definitionId.toString());
    }
    return getDestinationDefinition(new DestinationDefinitionIdRequestBody().destinationDefinitionId(definitionId));
  }

  public DestinationDefinitionRead getDestinationDefinitionForScope(final ActorDefinitionIdWithScope actorDefinitionIdWithScope)
      throws ConfigNotFoundException, IOException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final UUID definitionId = actorDefinitionIdWithScope.getActorDefinitionId();
    final UUID scopeId = actorDefinitionIdWithScope.getScopeId();
    final ScopeType scopeType = ScopeType.fromValue(actorDefinitionIdWithScope.getScopeType().toString());
    if (!actorDefinitionService.scopeCanUseDefinition(definitionId, scopeId, scopeType.value())) {
      final String message = String.format("Cannot find the requested definition with given id for this %s", scopeType);
      throw new IdNotFoundKnownException(message, definitionId.toString());
    }
    return getDestinationDefinition(new DestinationDefinitionIdRequestBody().destinationDefinitionId(definitionId));
  }

  public DestinationDefinitionRead createCustomDestinationDefinition(final CustomDestinationDefinitionCreate customDestinationDefinitionCreate)
      throws IOException {
    final UUID id = uuidSupplier.get();
    final DestinationDefinitionCreate destinationDefCreate = customDestinationDefinitionCreate.getDestinationDefinition();
    final ActorDefinitionVersion actorDefinitionVersion =
        actorDefinitionHandlerHelper
            .defaultDefinitionVersionFromCreate(destinationDefCreate.getDockerRepository(), destinationDefCreate.getDockerImageTag(),
                destinationDefCreate.getDocumentationUrl(), customDestinationDefinitionCreate.getWorkspaceId())
            .withActorDefinitionId(id);

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(id)
        .withName(destinationDefCreate.getName())
        .withIcon(destinationDefCreate.getIcon())
        .withTombstone(false)
        .withPublic(false)
        .withCustom(true)
        .withResourceRequirements(apiPojoConverters.actorDefResourceReqsToInternal(destinationDefCreate.getResourceRequirements()));

    // legacy call; todo: remove once we drop workspace_id column
    if (customDestinationDefinitionCreate.getWorkspaceId() != null) {
      destinationService.writeCustomConnectorMetadata(destinationDefinition, actorDefinitionVersion,
          customDestinationDefinitionCreate.getWorkspaceId(), ScopeType.WORKSPACE);
    } else {
      destinationService.writeCustomConnectorMetadata(destinationDefinition, actorDefinitionVersion,
          customDestinationDefinitionCreate.getScopeId(), ScopeType.fromValue(customDestinationDefinitionCreate.getScopeType().toString()));
    }

    return buildDestinationDefinitionRead(destinationDefinition, actorDefinitionVersion);
  }

  public DestinationDefinitionRead updateDestinationDefinition(final DestinationDefinitionUpdate destinationDefinitionUpdate)
      throws ConfigNotFoundException, IOException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final ConnectorPlatformCompatibilityValidationResult isNewConnectorVersionSupported =
        airbyteCompatibleConnectorsValidator.validate(destinationDefinitionUpdate.getDestinationDefinitionId().toString(),
            destinationDefinitionUpdate.getDockerImageTag());
    if (!isNewConnectorVersionSupported.isValid()) {
      final String message = isNewConnectorVersionSupported.getMessage() != null ? isNewConnectorVersionSupported.getMessage()
          : String.format("Destination %s can't be updated to version %s cause the version "
              + "is not supported by current platform version",
              destinationDefinitionUpdate.getDestinationDefinitionId().toString(),
              destinationDefinitionUpdate.getDockerImageTag());
      throw new BadRequestProblem(message, new ProblemMessageData().message(message));
    }

    final StandardDestinationDefinition currentDestination = destinationService
        .getStandardDestinationDefinition(destinationDefinitionUpdate.getDestinationDefinitionId());
    final ActorDefinitionVersion currentVersion = actorDefinitionService.getActorDefinitionVersion(currentDestination.getDefaultVersionId());

    final StandardDestinationDefinition newDestination = buildDestinationDefinitionUpdate(currentDestination, destinationDefinitionUpdate);

    final ActorDefinitionVersion newVersion = actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
        currentVersion, ActorType.DESTINATION, destinationDefinitionUpdate.getDockerImageTag(), currentDestination.getCustom());

    final List<ActorDefinitionBreakingChange> breakingChangesForDef =
        actorDefinitionHandlerHelper.getBreakingChanges(newVersion, ActorType.DESTINATION);
    destinationService.writeConnectorMetadata(newDestination, newVersion, breakingChangesForDef);

    final StandardDestinationDefinition updatedDestinationDefinition = destinationService
        .getStandardDestinationDefinition(destinationDefinitionUpdate.getDestinationDefinitionId());
    supportStateUpdater.updateSupportStatesForDestinationDefinition(updatedDestinationDefinition);
    return buildDestinationDefinitionRead(newDestination, newVersion);
  }

  @VisibleForTesting
  StandardDestinationDefinition buildDestinationDefinitionUpdate(final StandardDestinationDefinition currentDestination,
                                                                 final DestinationDefinitionUpdate destinationDefinitionUpdate) {
    final ActorDefinitionResourceRequirements updatedResourceReqs = destinationDefinitionUpdate.getResourceRequirements() != null
        ? apiPojoConverters.actorDefResourceReqsToInternal(destinationDefinitionUpdate.getResourceRequirements())
        : currentDestination.getResourceRequirements();

    final StandardDestinationDefinition newDestination = new StandardDestinationDefinition()
        .withDestinationDefinitionId(currentDestination.getDestinationDefinitionId())
        .withName(currentDestination.getName())
        .withIcon(currentDestination.getIcon())
        .withIconUrl(currentDestination.getIconUrl())
        .withTombstone(currentDestination.getTombstone())
        .withPublic(currentDestination.getPublic())
        .withCustom(currentDestination.getCustom())
        .withMetrics(currentDestination.getMetrics())
        .withResourceRequirements(updatedResourceReqs);

    if (destinationDefinitionUpdate.getName() != null && currentDestination.getCustom()) {
      newDestination.withName(destinationDefinitionUpdate.getName());
    }

    return newDestination;
  }

  public void deleteDestinationDefinition(final DestinationDefinitionIdRequestBody destinationDefinitionIdRequestBody)
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    // "delete" all destinations associated with the destination definition as well. This will cascade
    // to connections that depend on any deleted
    // destinations. Delete destinations first in case a failure occurs mid-operation.

    final StandardDestinationDefinition persistedDestinationDefinition =
        destinationService.getStandardDestinationDefinition(destinationDefinitionIdRequestBody.getDestinationDefinitionId());

    for (final DestinationRead destinationRead : destinationHandler.listDestinationsForDestinationDefinition(destinationDefinitionIdRequestBody)
        .getDestinations()) {
      destinationHandler.deleteDestination(destinationRead);
    }

    persistedDestinationDefinition.withTombstone(true);
    destinationService.updateStandardDestinationDefinition(persistedDestinationDefinition);
  }

  public PrivateDestinationDefinitionRead grantDestinationDefinitionToWorkspaceOrOrganization(final ActorDefinitionIdWithScope actorDefinitionIdWithScope)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardDestinationDefinition standardDestinationDefinition =
        destinationService.getStandardDestinationDefinition(actorDefinitionIdWithScope.getActorDefinitionId());
    final ActorDefinitionVersion actorDefinitionVersion =
        actorDefinitionService.getActorDefinitionVersion(standardDestinationDefinition.getDefaultVersionId());
    actorDefinitionService.writeActorDefinitionWorkspaceGrant(
        actorDefinitionIdWithScope.getActorDefinitionId(),
        actorDefinitionIdWithScope.getScopeId(),
        ScopeType.fromValue(actorDefinitionIdWithScope.getScopeType().toString()));
    return new PrivateDestinationDefinitionRead()
        .destinationDefinition(buildDestinationDefinitionRead(standardDestinationDefinition, actorDefinitionVersion))
        .granted(true);
  }

  public void revokeDestinationDefinition(final ActorDefinitionIdWithScope actorDefinitionIdWithScope)
      throws IOException {
    actorDefinitionService.deleteActorDefinitionWorkspaceGrant(
        actorDefinitionIdWithScope.getActorDefinitionId(),
        actorDefinitionIdWithScope.getScopeId(),
        ScopeType.fromValue(actorDefinitionIdWithScope.getScopeType().toString()));
  }

}
