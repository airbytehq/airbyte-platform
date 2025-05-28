/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.model.generated.ActorDefinitionIdWithScope;
import io.airbyte.api.model.generated.CustomSourceDefinitionCreate;
import io.airbyte.api.model.generated.PrivateSourceDefinitionRead;
import io.airbyte.api.model.generated.PrivateSourceDefinitionReadList;
import io.airbyte.api.model.generated.SourceDefinitionCreate;
import io.airbyte.api.model.generated.SourceDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.SourceDefinitionRead;
import io.airbyte.api.model.generated.SourceDefinitionRead.SourceTypeEnum;
import io.airbyte.api.model.generated.SourceDefinitionReadList;
import io.airbyte.api.model.generated.SourceDefinitionUpdate;
import io.airbyte.api.model.generated.SourceRead;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.api.problems.model.generated.ProblemMessageData;
import io.airbyte.api.problems.throwable.generated.BadRequestProblem;
import io.airbyte.api.problems.throwable.generated.UnprocessableEntityProblem;
import io.airbyte.commons.entitlements.Entitlement;
import io.airbyte.commons.entitlements.LicenseEntitlementChecker;
import io.airbyte.commons.lang.Exceptions;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.errors.IdNotFoundKnownException;
import io.airbyte.commons.server.errors.InternalServerKnownException;
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.ScopeType;
import io.airbyte.config.ScopedResourceRequirements;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.helpers.ConnectorRegistryConverters;
import io.airbyte.config.init.AirbyteCompatibleConnectorsValidator;
import io.airbyte.config.init.ConnectorPlatformCompatibilityValidationResult;
import io.airbyte.config.init.SupportStateUpdater;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.specs.RemoteDefinitionsProvider;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HideActorDefinitionFromList;
import io.airbyte.featureflag.Multi;
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

  private final Supplier<UUID> uuidSupplier;
  private final RemoteDefinitionsProvider remoteDefinitionsProvider;
  private final ActorDefinitionHandlerHelper actorDefinitionHandlerHelper;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final SourceHandler sourceHandler;
  private final SupportStateUpdater supportStateUpdater;
  private final FeatureFlagClient featureFlagClient;
  private final AirbyteCompatibleConnectorsValidator airbyteCompatibleConnectorsValidator;

  private final ActorDefinitionService actorDefinitionService;
  private final SourceService sourceService;
  private final WorkspaceService workspaceService;
  private final LicenseEntitlementChecker licenseEntitlementChecker;
  private final ApiPojoConverters apiPojoConverters;

  @Inject
  public SourceDefinitionsHandler(final ActorDefinitionService actorDefinitionService,
                                  @Named("uuidGenerator") final Supplier<UUID> uuidSupplier,
                                  final ActorDefinitionHandlerHelper actorDefinitionHandlerHelper,
                                  final RemoteDefinitionsProvider remoteDefinitionsProvider,
                                  final SourceHandler sourceHandler,
                                  final SupportStateUpdater supportStateUpdater,
                                  final FeatureFlagClient featureFlagClient,
                                  final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                                  final AirbyteCompatibleConnectorsValidator airbyteCompatibleConnectorsValidator,
                                  final SourceService sourceService,
                                  final WorkspaceService workspaceService,
                                  final LicenseEntitlementChecker licenseEntitlementChecker,
                                  final ApiPojoConverters apiPojoConverters) {
    this.actorDefinitionService = actorDefinitionService;
    this.uuidSupplier = uuidSupplier;
    this.actorDefinitionHandlerHelper = actorDefinitionHandlerHelper;
    this.remoteDefinitionsProvider = remoteDefinitionsProvider;
    this.sourceHandler = sourceHandler;
    this.supportStateUpdater = supportStateUpdater;
    this.featureFlagClient = featureFlagClient;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.airbyteCompatibleConnectorsValidator = airbyteCompatibleConnectorsValidator;
    this.sourceService = sourceService;
    this.workspaceService = workspaceService;
    this.licenseEntitlementChecker = licenseEntitlementChecker;
    this.apiPojoConverters = apiPojoConverters;
  }

  public SourceDefinitionRead buildSourceDefinitionRead(final UUID sourceDefinitionId, final boolean includeTombstone)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardSourceDefinition sourceDefinition = sourceService.getStandardSourceDefinition(sourceDefinitionId, includeTombstone);
    final ActorDefinitionVersion sourceVersion = actorDefinitionService.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId());
    return buildSourceDefinitionRead(sourceDefinition, sourceVersion);
  }

  @VisibleForTesting
  SourceDefinitionRead buildSourceDefinitionRead(final StandardSourceDefinition standardSourceDefinition,
                                                 final ActorDefinitionVersion sourceVersion) {

    try {
      return new SourceDefinitionRead()
          .sourceDefinitionId(standardSourceDefinition.getSourceDefinitionId())
          .name(standardSourceDefinition.getName())
          .sourceType(getSourceType(standardSourceDefinition))
          .dockerRepository(sourceVersion.getDockerRepository())
          .dockerImageTag(sourceVersion.getDockerImageTag())
          .documentationUrl(new URI(sourceVersion.getDocumentationUrl()))
          .icon(standardSourceDefinition.getIconUrl())
          .protocolVersion(sourceVersion.getProtocolVersion())
          .supportLevel(apiPojoConverters.toApiSupportLevel(sourceVersion.getSupportLevel()))
          .releaseStage(apiPojoConverters.toApiReleaseStage(sourceVersion.getReleaseStage()))
          .releaseDate(apiPojoConverters.toLocalDate(sourceVersion.getReleaseDate()))
          .lastPublished(apiPojoConverters.toOffsetDateTime(sourceVersion.getLastPublished()))
          .cdkVersion(sourceVersion.getCdkVersion())
          .metrics(standardSourceDefinition.getMetrics())
          .custom(standardSourceDefinition.getCustom())
          .enterprise(standardSourceDefinition.getEnterprise())
          .resourceRequirements(apiPojoConverters.scopedResourceReqsToApi(standardSourceDefinition.getResourceRequirements()))
          .maxSecondsBetweenMessages(standardSourceDefinition.getMaxSecondsBetweenMessages())
          .language(sourceVersion.getLanguage());

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
    final List<StandardSourceDefinition> standardSourceDefinitions = sourceService.listStandardSourceDefinitions(false);
    final Map<UUID, ActorDefinitionVersion> sourceDefinitionVersionMap = getVersionsForSourceDefinitions(standardSourceDefinitions);
    return toSourceDefinitionReadList(standardSourceDefinitions, sourceDefinitionVersionMap);
  }

  private Map<UUID, ActorDefinitionVersion> getVersionsForSourceDefinitions(final List<StandardSourceDefinition> sourceDefinitions)
      throws IOException {
    return actorDefinitionService.getActorDefinitionVersions(sourceDefinitions
        .stream()
        .map(StandardSourceDefinition::getDefaultVersionId)
        .collect(Collectors.toList()))
        .stream().collect(Collectors.toMap(ActorDefinitionVersion::getActorDefinitionId, v -> v));
  }

  private SourceDefinitionReadList toSourceDefinitionReadList(final List<StandardSourceDefinition> defs,
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
      throws IOException, JsonValidationException, ConfigNotFoundException {

    final List<StandardSourceDefinition> publicSourceDefs = sourceService.listPublicSourceDefinitions(false);

    final StandardWorkspace workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceIdRequestBody.getWorkspaceId(), true);
    final Map<UUID, Boolean> publicSourceEntitlements = licenseEntitlementChecker.checkEntitlements(
        workspace.getOrganizationId(),
        Entitlement.SOURCE_CONNECTOR,
        publicSourceDefs.stream().map(StandardSourceDefinition::getSourceDefinitionId).toList());

    final Stream<StandardSourceDefinition> entitledPublicSourceDefs = publicSourceDefs.stream()
        .filter(s -> publicSourceEntitlements.get(s.getSourceDefinitionId()));

    final List<StandardSourceDefinition> sourceDefs = Stream.concat(
        entitledPublicSourceDefs,
        sourceService.listGrantedSourceDefinitions(workspaceIdRequestBody.getWorkspaceId(), false).stream()).toList();

    // Hide source definitions from the list via feature flag
    final List<StandardSourceDefinition> shownSourceDefs = sourceDefs.stream().filter((sourceDefinition) -> !featureFlagClient.boolVariation(
        HideActorDefinitionFromList.INSTANCE,
        new Multi(List.of(new SourceDefinition(sourceDefinition.getSourceDefinitionId()), new Workspace(workspaceIdRequestBody.getWorkspaceId())))))
        .toList();

    final Map<UUID, ActorDefinitionVersion> sourceDefVersionMap =
        actorDefinitionVersionHelper.getSourceVersions(shownSourceDefs, workspaceIdRequestBody.getWorkspaceId());
    return toSourceDefinitionReadList(shownSourceDefs, sourceDefVersionMap);
  }

  public PrivateSourceDefinitionReadList listPrivateSourceDefinitions(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws IOException {
    final List<Entry<StandardSourceDefinition, Boolean>> standardSourceDefinitionBooleanMap =
        sourceService.listGrantableSourceDefinitions(workspaceIdRequestBody.getWorkspaceId(), false);
    final Map<UUID, ActorDefinitionVersion> sourceDefinitionVersionMap =
        getVersionsForSourceDefinitions(standardSourceDefinitionBooleanMap.stream().map(Entry::getKey).toList());
    return toPrivateSourceDefinitionReadList(standardSourceDefinitionBooleanMap, sourceDefinitionVersionMap);
  }

  public SourceDefinitionReadList listPublicSourceDefinitions() throws IOException {
    final List<StandardSourceDefinition> standardSourceDefinitions = sourceService.listPublicSourceDefinitions(false);
    final Map<UUID, ActorDefinitionVersion> sourceDefinitionVersionMap = getVersionsForSourceDefinitions(standardSourceDefinitions);
    return toSourceDefinitionReadList(standardSourceDefinitions, sourceDefinitionVersionMap);
  }

  private PrivateSourceDefinitionReadList toPrivateSourceDefinitionReadList(final List<Entry<StandardSourceDefinition, Boolean>> defs,
                                                                            final Map<UUID, ActorDefinitionVersion> defIdToVersionMap) {
    final List<PrivateSourceDefinitionRead> reads = defs.stream()
        .map(entry -> new PrivateSourceDefinitionRead()
            .sourceDefinition(buildSourceDefinitionRead(entry.getKey(), defIdToVersionMap.get(entry.getKey().getSourceDefinitionId())))
            .granted(entry.getValue()))
        .collect(Collectors.toList());
    return new PrivateSourceDefinitionReadList().sourceDefinitions(reads);
  }

  public SourceDefinitionRead getSourceDefinition(final UUID sourceDefinitionId, final boolean includeTombstone)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    return buildSourceDefinitionRead(sourceDefinitionId, includeTombstone);
  }

  public SourceDefinitionRead getSourceDefinitionForScope(final ActorDefinitionIdWithScope actorDefinitionIdWithScope)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final UUID definitionId = actorDefinitionIdWithScope.getActorDefinitionId();
    final UUID scopeId = actorDefinitionIdWithScope.getScopeId();
    final ScopeType scopeType = ScopeType.fromValue(actorDefinitionIdWithScope.getScopeType().toString());
    if (!actorDefinitionService.scopeCanUseDefinition(definitionId, scopeId, scopeType.value())) {
      final String message = String.format("Cannot find the requested definition with given id for this %s", scopeType);
      throw new IdNotFoundKnownException(message, definitionId.toString());
    }
    return getSourceDefinition(definitionId, true);
  }

  public SourceDefinitionRead getSourceDefinitionForWorkspace(final SourceDefinitionIdWithWorkspaceId sourceDefinitionIdWithWorkspaceId)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final UUID definitionId = sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId();
    final UUID workspaceId = sourceDefinitionIdWithWorkspaceId.getWorkspaceId();
    if (!workspaceService.workspaceCanUseDefinition(definitionId, workspaceId)) {
      throw new IdNotFoundKnownException("Cannot find the requested definition with given id for this workspace", definitionId.toString());
    }
    return getSourceDefinition(definitionId, true);
  }

  public SourceDefinitionRead createCustomSourceDefinition(final CustomSourceDefinitionCreate customSourceDefinitionCreate) throws IOException {
    final UUID id = uuidSupplier.get();
    final SourceDefinitionCreate sourceDefinitionCreate = customSourceDefinitionCreate.getSourceDefinition();
    final UUID workspaceId = resolveWorkspaceId(customSourceDefinitionCreate);
    final ActorDefinitionVersion actorDefinitionVersion =
        actorDefinitionHandlerHelper
            .defaultDefinitionVersionFromCreate(sourceDefinitionCreate.getDockerRepository(), sourceDefinitionCreate.getDockerImageTag(),
                sourceDefinitionCreate.getDocumentationUrl(), workspaceId)
            .withActorDefinitionId(id);

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(id)
        .withName(sourceDefinitionCreate.getName())
        .withIcon(sourceDefinitionCreate.getIcon())
        .withTombstone(false)
        .withPublic(false)
        .withCustom(true)
        .withResourceRequirements(apiPojoConverters.scopedResourceReqsToInternal(sourceDefinitionCreate.getResourceRequirements()));

    // legacy call; todo: remove once we drop workspace_id column
    if (customSourceDefinitionCreate.getWorkspaceId() != null) {
      sourceService.writeCustomConnectorMetadata(sourceDefinition, actorDefinitionVersion,
          customSourceDefinitionCreate.getWorkspaceId(), ScopeType.WORKSPACE);
    } else {
      sourceService.writeCustomConnectorMetadata(sourceDefinition, actorDefinitionVersion,
          customSourceDefinitionCreate.getScopeId(), ScopeType.fromValue(customSourceDefinitionCreate.getScopeType().toString()));
    }

    return buildSourceDefinitionRead(sourceDefinition, actorDefinitionVersion);
  }

  private UUID resolveWorkspaceId(final CustomSourceDefinitionCreate customSourceDefinitionCreate) {
    if (customSourceDefinitionCreate.getWorkspaceId() != null) {
      return customSourceDefinitionCreate.getWorkspaceId();
    }
    if (ScopeType.fromValue(customSourceDefinitionCreate.getScopeType().toString()).equals(ScopeType.WORKSPACE)) {
      return customSourceDefinitionCreate.getScopeId();
    }
    throw new UnprocessableEntityProblem(new ProblemMessageData()
        .message(String.format("Cannot determine workspace ID for custom source definition creation: %s", customSourceDefinitionCreate)));
  }

  public SourceDefinitionRead updateSourceDefinition(final SourceDefinitionUpdate sourceDefinitionUpdate)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final ConnectorPlatformCompatibilityValidationResult isNewConnectorVersionSupported =
        airbyteCompatibleConnectorsValidator.validate(sourceDefinitionUpdate.getSourceDefinitionId().toString(),
            sourceDefinitionUpdate.getDockerImageTag());
    if (!isNewConnectorVersionSupported.isValid()) {
      final String message = isNewConnectorVersionSupported.getMessage() != null ? isNewConnectorVersionSupported.getMessage()
          : String.format("Destination %s can't be updated to version %s cause the version "
              + "is not supported by current platform version",
              sourceDefinitionUpdate.getSourceDefinitionId().toString(),
              sourceDefinitionUpdate.getDockerImageTag());
      throw new BadRequestProblem(message, new ProblemMessageData().message(message));
    }
    final StandardSourceDefinition currentSourceDefinition =
        sourceService.getStandardSourceDefinition(sourceDefinitionUpdate.getSourceDefinitionId());
    final ActorDefinitionVersion currentVersion = actorDefinitionService.getActorDefinitionVersion(currentSourceDefinition.getDefaultVersionId());

    final StandardSourceDefinition newSource = buildSourceDefinitionUpdate(currentSourceDefinition, sourceDefinitionUpdate);

    final ActorDefinitionVersion newVersion = actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
        currentVersion, ActorType.SOURCE, sourceDefinitionUpdate.getDockerImageTag(), currentSourceDefinition.getCustom(),
        sourceDefinitionUpdate.getWorkspaceId());

    final List<ActorDefinitionBreakingChange> breakingChangesForDef = actorDefinitionHandlerHelper.getBreakingChanges(newVersion, ActorType.SOURCE);
    sourceService.writeConnectorMetadata(newSource, newVersion, breakingChangesForDef);

    final StandardSourceDefinition updatedSourceDefinition = sourceService.getStandardSourceDefinition(newSource.getSourceDefinitionId());
    supportStateUpdater.updateSupportStatesForSourceDefinition(updatedSourceDefinition);

    return buildSourceDefinitionRead(newSource, newVersion);
  }

  @VisibleForTesting
  StandardSourceDefinition buildSourceDefinitionUpdate(final StandardSourceDefinition currentSourceDefinition,
                                                       final SourceDefinitionUpdate sourceDefinitionUpdate) {
    final ScopedResourceRequirements updatedResourceReqs = sourceDefinitionUpdate.getResourceRequirements() != null
        ? apiPojoConverters.scopedResourceReqsToInternal(sourceDefinitionUpdate.getResourceRequirements())
        : currentSourceDefinition.getResourceRequirements();

    final StandardSourceDefinition newSource = new StandardSourceDefinition()
        .withSourceDefinitionId(currentSourceDefinition.getSourceDefinitionId())
        .withName(currentSourceDefinition.getName())
        .withIcon(currentSourceDefinition.getIcon())
        .withIconUrl(currentSourceDefinition.getIconUrl())
        .withTombstone(currentSourceDefinition.getTombstone())
        .withPublic(currentSourceDefinition.getPublic())
        .withCustom(currentSourceDefinition.getCustom())
        .withMetrics(currentSourceDefinition.getMetrics())
        .withMaxSecondsBetweenMessages(currentSourceDefinition.getMaxSecondsBetweenMessages())
        .withResourceRequirements(updatedResourceReqs);

    if (sourceDefinitionUpdate.getName() != null && currentSourceDefinition.getCustom()) {
      newSource.withName(sourceDefinitionUpdate.getName());
    }

    return newSource;
  }

  public void deleteSourceDefinition(final UUID sourceDefinitionId)
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
    // "delete" all sources associated with the source definition as well. This will cascade to
    // connections that depend on any deleted sources.
    // Delete sources first in case a failure occurs mid-operation.

    final StandardSourceDefinition persistedSourceDefinition =
        sourceService.getStandardSourceDefinition(sourceDefinitionId);

    for (final SourceRead sourceRead : sourceHandler.listSourcesForSourceDefinition(sourceDefinitionId).getSources()) {
      sourceHandler.deleteSource(sourceRead);
    }

    persistedSourceDefinition.withTombstone(true);
    sourceService.updateStandardSourceDefinition(persistedSourceDefinition);
  }

  public PrivateSourceDefinitionRead grantSourceDefinitionToWorkspaceOrOrganization(final ActorDefinitionIdWithScope actorDefinitionIdWithScope)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardSourceDefinition standardSourceDefinition =
        sourceService.getStandardSourceDefinition(actorDefinitionIdWithScope.getActorDefinitionId());
    final ActorDefinitionVersion actorDefinitionVersion =
        actorDefinitionService.getActorDefinitionVersion(standardSourceDefinition.getDefaultVersionId());
    actorDefinitionService.writeActorDefinitionWorkspaceGrant(
        actorDefinitionIdWithScope.getActorDefinitionId(),
        actorDefinitionIdWithScope.getScopeId(),
        ScopeType.fromValue(actorDefinitionIdWithScope.getScopeType().toString()));
    return new PrivateSourceDefinitionRead()
        .sourceDefinition(buildSourceDefinitionRead(standardSourceDefinition, actorDefinitionVersion))
        .granted(true);
  }

  public void revokeSourceDefinition(final ActorDefinitionIdWithScope actorDefinitionIdWithScope)
      throws IOException {
    actorDefinitionService.deleteActorDefinitionWorkspaceGrant(
        actorDefinitionIdWithScope.getActorDefinitionId(),
        actorDefinitionIdWithScope.getScopeId(),
        ScopeType.fromValue(actorDefinitionIdWithScope.getScopeType().toString()));
  }

}
