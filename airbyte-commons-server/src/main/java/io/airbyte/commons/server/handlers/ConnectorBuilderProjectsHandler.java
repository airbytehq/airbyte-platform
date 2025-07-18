/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.constants.AirbyteCatalogConstants.AIRBYTE_SOURCE_DECLARATIVE_MANIFEST_IMAGE;
import static io.airbyte.commons.version.AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTOR_BUILDER_PROJECT_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.WORKSPACE_ID_KEY;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.api.model.generated.BaseActorDefinitionVersionInfo;
import io.airbyte.api.model.generated.BuilderProjectForDefinitionRequestBody;
import io.airbyte.api.model.generated.BuilderProjectForDefinitionResponse;
import io.airbyte.api.model.generated.BuilderProjectOauthConsentRequest;
import io.airbyte.api.model.generated.CompleteConnectorBuilderProjectOauthRequest;
import io.airbyte.api.model.generated.CompleteOAuthResponse;
import io.airbyte.api.model.generated.ConnectorBuilderAuxiliaryRequest;
import io.airbyte.api.model.generated.ConnectorBuilderHttpRequest;
import io.airbyte.api.model.generated.ConnectorBuilderHttpResponse;
import io.airbyte.api.model.generated.ConnectorBuilderProjectDetails;
import io.airbyte.api.model.generated.ConnectorBuilderProjectDetailsRead;
import io.airbyte.api.model.generated.ConnectorBuilderProjectForkRequestBody;
import io.airbyte.api.model.generated.ConnectorBuilderProjectFullResolveRequestBody;
import io.airbyte.api.model.generated.ConnectorBuilderProjectFullResolveResponse;
import io.airbyte.api.model.generated.ConnectorBuilderProjectIdWithWorkspaceId;
import io.airbyte.api.model.generated.ConnectorBuilderProjectRead;
import io.airbyte.api.model.generated.ConnectorBuilderProjectReadList;
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamRead;
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadLogsInner;
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadRequestBody;
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadSlicesInner;
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadSlicesInnerPagesInner;
import io.airbyte.api.model.generated.ConnectorBuilderProjectTestingValuesUpdate;
import io.airbyte.api.model.generated.ConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.api.model.generated.ConnectorBuilderPublishRequestBody;
import io.airbyte.api.model.generated.ContributionInfo;
import io.airbyte.api.model.generated.DeclarativeManifestBaseImageRead;
import io.airbyte.api.model.generated.DeclarativeManifestRead;
import io.airbyte.api.model.generated.DeclarativeManifestRequestBody;
import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.api.model.generated.OAuthConsentRead;
import io.airbyte.api.model.generated.SourceDefinitionIdBody;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.errors.NotFoundException;
import io.airbyte.commons.server.handlers.helpers.BuilderProjectUpdater;
import io.airbyte.commons.server.handlers.helpers.DeclarativeSourceManifestInjector;
import io.airbyte.commons.server.handlers.helpers.OAuthHelper;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionConfigInjection;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ConfigNotFoundType;
import io.airbyte.config.ConnectorBuilderProject;
import io.airbyte.config.ConnectorBuilderProjectVersionedManifest;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.ScopeType;
import io.airbyte.config.SecretPersistenceConfig;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSourceDefinition.SourceType;
import io.airbyte.config.SupportLevel;
import io.airbyte.config.secrets.JsonSecretsProcessor;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence;
import io.airbyte.config.specs.RemoteDefinitionsProvider;
import io.airbyte.connectorbuilderserver.api.client.generated.ConnectorBuilderServerApi;
import io.airbyte.connectorbuilderserver.api.client.model.generated.AuxiliaryRequest;
import io.airbyte.connectorbuilderserver.api.client.model.generated.FullResolveManifestRequestBody;
import io.airbyte.connectorbuilderserver.api.client.model.generated.HttpRequest;
import io.airbyte.connectorbuilderserver.api.client.model.generated.HttpResponse;
import io.airbyte.connectorbuilderserver.api.client.model.generated.ResolveManifest;
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamRead;
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadRequestBody;
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadSlicesInner;
import io.airbyte.data.ConfigNotFoundException;
import io.airbyte.data.repositories.entities.DeclarativeManifestImageVersion;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.ConnectorBuilderService;
import io.airbyte.data.services.DeclarativeManifestImageVersionService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Organization;
import io.airbyte.featureflag.UseRuntimeSecretPersistence;
import io.airbyte.metrics.MetricClient;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.oauth.OAuthImplementationFactory;
import io.airbyte.oauth.declarative.DeclarativeOAuthFlow;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import io.airbyte.protocol.models.v0.OAuthConfigSpecification;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.core.util.CollectionUtils;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConnectorBuilderProjectsHandler. Javadocs suppressed because api docs should be used as source of
 * truth.
 */
@Singleton
@SuppressWarnings("PMD.PreserveStackTrace")
public class ConnectorBuilderProjectsHandler {

  private static final Logger log = LoggerFactory.getLogger(ConnectorBuilderProjectsHandler.class);
  private final DeclarativeManifestImageVersionService declarativeManifestImageVersionService;
  private final ConnectorBuilderService connectorBuilderService;

  private final BuilderProjectUpdater buildProjectUpdater;
  private final Supplier<UUID> uuidSupplier;
  private final DeclarativeSourceManifestInjector manifestInjector;
  private final WorkspaceService workspaceService;
  private final FeatureFlagClient featureFlagClient;
  private final SecretsRepositoryReader secretsRepositoryReader;
  private final SecretsRepositoryWriter secretsRepositoryWriter;
  private final SecretPersistenceConfigService secretPersistenceConfigService;
  private final SourceService sourceService;
  private final JsonSecretsProcessor secretsProcessor;
  private final ConnectorBuilderServerApi connectorBuilderServerApiClient;
  private final ActorDefinitionService actorDefinitionService;
  private final RemoteDefinitionsProvider remoteDefinitionsProvider;
  private final OAuthImplementationFactory oAuthImplementationFactory;
  private final MetricClient metricClient;

  public static final String SPEC_FIELD = "spec";
  public static final String CONNECTION_SPECIFICATION_FIELD = "connection_specification";

  @Inject
  public ConnectorBuilderProjectsHandler(final DeclarativeManifestImageVersionService declarativeManifestImageVersionService,
                                         final ConnectorBuilderService connectorBuilderService,
                                         final BuilderProjectUpdater builderProjectUpdater,
                                         @Named("uuidGenerator") final Supplier<UUID> uuidSupplier,
                                         final DeclarativeSourceManifestInjector manifestInjector,
                                         final WorkspaceService workspaceService,
                                         final FeatureFlagClient featureFlagClient,
                                         final SecretsRepositoryReader secretsRepositoryReader,
                                         final SecretsRepositoryWriter secretsRepositoryWriter,
                                         final SecretPersistenceConfigService secretPersistenceConfigService,
                                         final SourceService sourceService,
                                         @Named("jsonSecretsProcessorWithCopy") final JsonSecretsProcessor secretsProcessor,
                                         final ConnectorBuilderServerApi connectorBuilderServerApiClient,
                                         final ActorDefinitionService actorDefinitionService,
                                         final RemoteDefinitionsProvider remoteDefinitionsProvider,
                                         @Named("oauthImplementationFactory") final OAuthImplementationFactory oauthImplementationFactory,
                                         final MetricClient metricClient) {
    this.declarativeManifestImageVersionService = declarativeManifestImageVersionService;
    this.connectorBuilderService = connectorBuilderService;
    this.buildProjectUpdater = builderProjectUpdater;
    this.uuidSupplier = uuidSupplier;
    this.manifestInjector = manifestInjector;
    this.workspaceService = workspaceService;
    this.featureFlagClient = featureFlagClient;
    this.secretsRepositoryReader = secretsRepositoryReader;
    this.secretsRepositoryWriter = secretsRepositoryWriter;
    this.secretPersistenceConfigService = secretPersistenceConfigService;
    this.sourceService = sourceService;
    this.secretsProcessor = secretsProcessor;
    this.connectorBuilderServerApiClient = connectorBuilderServerApiClient;
    this.actorDefinitionService = actorDefinitionService;
    this.remoteDefinitionsProvider = remoteDefinitionsProvider;
    this.oAuthImplementationFactory = oauthImplementationFactory;
    this.metricClient = metricClient;
  }

  private ConnectorBuilderProjectDetailsRead getProjectDetailsWithoutBaseAdvInfo(final ConnectorBuilderProject project) {
    final ConnectorBuilderProjectDetailsRead detailsRead = new ConnectorBuilderProjectDetailsRead()
        .name(project.getName())
        .updatedAt(project.getUpdatedAt())
        .builderProjectId(project.getBuilderProjectId())
        .sourceDefinitionId(project.getActorDefinitionId())
        .activeDeclarativeManifestVersion(
            project.getActiveDeclarativeManifestVersion())
        .hasDraft(project.getHasDraft())
        .componentsFileContent(project.getComponentsFileContent());

    if (project.getContributionPullRequestUrl() != null) {
      detailsRead.setContributionInfo(new ContributionInfo().pullRequestUrl(project.getContributionPullRequestUrl())
          .actorDefinitionId(project.getContributionActorDefinitionId()));
    }
    return detailsRead;
  }

  private BaseActorDefinitionVersionInfo buildBaseActorDefinitionVersionInfo(final ActorDefinitionVersion actorDefinitionVersion,
                                                                             final StandardSourceDefinition sourceDefinition) {
    return new BaseActorDefinitionVersionInfo()
        .name(sourceDefinition.getName())
        .dockerRepository(actorDefinitionVersion.getDockerRepository())
        .dockerImageTag(actorDefinitionVersion.getDockerImageTag())
        .actorDefinitionId(actorDefinitionVersion.getActorDefinitionId())
        .icon(sourceDefinition.getIconUrl())
        .documentationUrl(actorDefinitionVersion.getDocumentationUrl());
  }

  private ConnectorBuilderProjectDetailsRead builderProjectToDetailsRead(final ConnectorBuilderProject project)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final ConnectorBuilderProjectDetailsRead detailsRead = getProjectDetailsWithoutBaseAdvInfo(project);
    if (project.getBaseActorDefinitionVersionId() != null) {
      final ActorDefinitionVersion baseActorDefinitionVersion =
          actorDefinitionService.getActorDefinitionVersion(project.getBaseActorDefinitionVersionId());
      detailsRead.baseActorDefinitionVersionInfo(
          buildBaseActorDefinitionVersionInfo(
              baseActorDefinitionVersion,
              sourceService.getStandardSourceDefinition(baseActorDefinitionVersion.getActorDefinitionId())));
    }
    return detailsRead;
  }

  private List<ConnectorBuilderProjectDetailsRead> builderProjectsToDetailsReads(final List<ConnectorBuilderProject> projects)
      throws IOException {

    final List<UUID> baseActorDefinitionVersionIds = projects
        .stream()
        .map(ConnectorBuilderProject::getBaseActorDefinitionVersionId)
        .distinct()
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    final List<ActorDefinitionVersion> baseActorDefinitionVersions = actorDefinitionService.getActorDefinitionVersions(baseActorDefinitionVersionIds);

    final Map<UUID, ActorDefinitionVersion> baseAdvIdToAdvMap =
        baseActorDefinitionVersions.stream().collect(Collectors.toMap(ActorDefinitionVersion::getVersionId, Function.identity()));
    final Map<UUID, StandardSourceDefinition> standardSourceDefinitionsMap = sourceService.listStandardSourceDefinitions(false).stream()
        .collect(Collectors.toMap(StandardSourceDefinition::getSourceDefinitionId, Function.identity()));

    final Map<UUID, StandardSourceDefinition> baseAdvIdToAssociatedSourceDefMap = baseActorDefinitionVersions.stream()
        .collect(Collectors.toMap(
            ActorDefinitionVersion::getVersionId,
            actorDefinitionVersion -> standardSourceDefinitionsMap.get(actorDefinitionVersion.getActorDefinitionId())));

    return projects.stream().map(project -> {
      final ConnectorBuilderProjectDetailsRead detailsRead = getProjectDetailsWithoutBaseAdvInfo(project);
      final UUID baseAdvId = project.getBaseActorDefinitionVersionId();
      if (baseAdvId != null) {
        detailsRead
            .baseActorDefinitionVersionInfo(buildBaseActorDefinitionVersionInfo(
                baseAdvIdToAdvMap.get(baseAdvId),
                baseAdvIdToAssociatedSourceDefMap.get(baseAdvId)));
      }
      return detailsRead;
    }).collect(Collectors.toList());
  }

  private ConnectorBuilderProjectIdWithWorkspaceId buildIdResponseFromId(final UUID projectId, final UUID workspaceId) {
    return new ConnectorBuilderProjectIdWithWorkspaceId().workspaceId(workspaceId).builderProjectId(projectId);
  }

  private void validateProjectUnderRightWorkspace(final UUID projectId, final UUID workspaceId) throws ConfigNotFoundException, IOException {
    final ConnectorBuilderProject project = connectorBuilderService.getConnectorBuilderProject(projectId, false);
    validateProjectUnderRightWorkspace(project, workspaceId);
  }

  private void validateProjectUnderRightWorkspace(final ConnectorBuilderProject project, final UUID workspaceId) throws ConfigNotFoundException {
    final UUID actualWorkspaceId = project.getWorkspaceId();
    if (!actualWorkspaceId.equals(workspaceId)) {
      throw new ConfigNotFoundException(ConfigNotFoundType.CONNECTOR_BUILDER_PROJECT, project.getBuilderProjectId().toString());
    }
  }

  public ConnectorBuilderProjectIdWithWorkspaceId createConnectorBuilderProject(final ConnectorBuilderProjectWithWorkspaceId projectCreate)
      throws IOException {
    final UUID id = uuidSupplier.get();

    connectorBuilderService.writeBuilderProjectDraft(id,
        projectCreate.getWorkspaceId(),
        projectCreate.getBuilderProject().getName(),
        new ObjectMapper().valueToTree(projectCreate.getBuilderProject().getDraftManifest()),
        projectCreate.getBuilderProject().getComponentsFileContent(),
        projectCreate.getBuilderProject().getBaseActorDefinitionVersionId(),
        projectCreate.getBuilderProject().getContributionPullRequestUrl(),
        projectCreate.getBuilderProject().getContributionActorDefinitionId());

    return buildIdResponseFromId(id, projectCreate.getWorkspaceId());
  }

  /**
   * Apply defaults from the persisted project to the update. These fields will only be passed when we
   * are trying to set them (patch-style), and cannot be currently un-set. Therefore, grab the
   * persisted value if the update does not contain it, since all fields are passed into the update
   * method.
   */
  private ConnectorBuilderProjectDetails applyPatchDefaultsFromDb(final ConnectorBuilderProjectDetails projectDetailsUpdate,
                                                                  final ConnectorBuilderProject persistedProject) {
    if (projectDetailsUpdate.getBaseActorDefinitionVersionId() == null) {
      projectDetailsUpdate.setBaseActorDefinitionVersionId(persistedProject.getBaseActorDefinitionVersionId());
    }
    if (projectDetailsUpdate.getContributionPullRequestUrl() == null) {
      projectDetailsUpdate.setContributionPullRequestUrl(persistedProject.getContributionPullRequestUrl());
    }
    if (projectDetailsUpdate.getContributionActorDefinitionId() == null) {
      projectDetailsUpdate.setContributionActorDefinitionId(persistedProject.getContributionActorDefinitionId());
    }
    return projectDetailsUpdate;
  }

  public void updateConnectorBuilderProject(final ExistingConnectorBuilderProjectWithWorkspaceId projectUpdate)
      throws ConfigNotFoundException, IOException {

    final ConnectorBuilderProject connectorBuilderProject =
        connectorBuilderService.getConnectorBuilderProject(projectUpdate.getBuilderProjectId(), false);
    validateProjectUnderRightWorkspace(connectorBuilderProject, projectUpdate.getWorkspaceId());

    final ConnectorBuilderProjectDetails projectDetailsUpdate = applyPatchDefaultsFromDb(projectUpdate.getBuilderProject(), connectorBuilderProject);

    buildProjectUpdater.persistBuilderProjectUpdate(projectUpdate.builderProject(projectDetailsUpdate));
  }

  public void deleteConnectorBuilderProject(final ConnectorBuilderProjectIdWithWorkspaceId projectDelete)
      throws IOException, ConfigNotFoundException {
    validateProjectUnderRightWorkspace(projectDelete.getBuilderProjectId(), projectDelete.getWorkspaceId());
    connectorBuilderService.deleteBuilderProject(projectDelete.getBuilderProjectId());
  }

  public ConnectorBuilderProjectRead getConnectorBuilderProjectWithManifest(final ConnectorBuilderProjectIdWithWorkspaceId request)
      throws IOException, ConfigNotFoundException, JsonValidationException {

    if (request.getVersion() != null) {
      validateProjectUnderRightWorkspace(request.getBuilderProjectId(), request.getWorkspaceId());
      return buildConnectorBuilderProjectVersionManifestRead(
          connectorBuilderService.getVersionedConnectorBuilderProject(request.getBuilderProjectId(), request.getVersion()));
    }

    return getWithManifestWithoutVersion(request);
  }

  private ConnectorBuilderProjectRead getWithManifestWithoutVersion(final ConnectorBuilderProjectIdWithWorkspaceId request)
      throws IOException, ConfigNotFoundException, JsonValidationException {
    final ConnectorBuilderProject project = connectorBuilderService.getConnectorBuilderProject(request.getBuilderProjectId(), true);
    validateProjectUnderRightWorkspace(project, request.getWorkspaceId());
    final ConnectorBuilderProjectRead response = new ConnectorBuilderProjectRead().builderProject(builderProjectToDetailsRead(project));

    if (project.getManifestDraft() != null) {
      response.setDeclarativeManifest(new DeclarativeManifestRead()
          .manifest(project.getManifestDraft())
          .isDraft(true));
      response.setTestingValues(maskSecrets(project.getTestingValues(), project.getManifestDraft()));
    } else if (project.getActorDefinitionId() != null) {
      final DeclarativeManifest declarativeManifest = connectorBuilderService.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(
          project.getActorDefinitionId());
      response.setDeclarativeManifest(new DeclarativeManifestRead()
          .isDraft(false)
          .manifest(declarativeManifest.getManifest())
          .version(declarativeManifest.getVersion())
          .description(declarativeManifest.getDescription()));
      response.getBuilderProject().setComponentsFileContent(declarativeManifest.getComponentsFileContent());
      response.setTestingValues(maskSecrets(project.getTestingValues(), declarativeManifest.getManifest()));
    }
    return response;
  }

  private ConnectorBuilderProjectRead buildConnectorBuilderProjectVersionManifestRead(final ConnectorBuilderProjectVersionedManifest project) {
    return new ConnectorBuilderProjectRead()
        .builderProject(new ConnectorBuilderProjectDetailsRead()
            .builderProjectId(project.getBuilderProjectId())
            .name(project.getName())
            .hasDraft(project.getHasDraft())
            .activeDeclarativeManifestVersion(project.getActiveDeclarativeManifestVersion())
            .sourceDefinitionId(project.getSourceDefinitionId())
            .componentsFileContent(project.getComponentsFileContent()))
        .declarativeManifest(new DeclarativeManifestRead()
            .isDraft(false)
            .manifest(project.getManifest())
            .version(project.getManifestVersion())
            .description(project.getManifestDescription()))
        .testingValues(maskSecrets(project.getTestingValues(), project.getManifest()));
  }

  private JsonNode maskSecrets(final JsonNode testingValues, final JsonNode manifest) {
    final JsonNode spec = manifest.get(SPEC_FIELD);
    if (spec != null) {
      final JsonNode connectionSpecification = spec.get(CONNECTION_SPECIFICATION_FIELD);
      if (connectionSpecification != null && testingValues != null) {
        return secretsProcessor.prepareSecretsForOutput(testingValues, connectionSpecification);
      }
    }
    return testingValues;
  }

  public ConnectorBuilderProjectReadList listConnectorBuilderProjects(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws IOException {

    final Stream<ConnectorBuilderProject> projects =
        connectorBuilderService.getConnectorBuilderProjectsByWorkspace(workspaceIdRequestBody.getWorkspaceId());

    return new ConnectorBuilderProjectReadList().projects(builderProjectsToDetailsReads(projects.toList()));
  }

  public SourceDefinitionIdBody publishConnectorBuilderProject(final ConnectorBuilderPublishRequestBody connectorBuilderPublishRequestBody)
      throws IOException, ConfigNotFoundException {
    validateProjectUnderRightWorkspace(connectorBuilderPublishRequestBody.getBuilderProjectId(), connectorBuilderPublishRequestBody.getWorkspaceId());
    final JsonNode manifest = connectorBuilderPublishRequestBody.getInitialDeclarativeManifest().getManifest();
    final JsonNode spec = connectorBuilderPublishRequestBody.getInitialDeclarativeManifest().getSpec();
    final String componentsFileContent = connectorBuilderPublishRequestBody.getComponentsFileContent();

    manifestInjector.addInjectedDeclarativeManifest(spec);
    final UUID actorDefinitionId = createActorDefinition(connectorBuilderPublishRequestBody.getName(),
        connectorBuilderPublishRequestBody.getWorkspaceId(),
        manifest,
        spec,
        componentsFileContent);

    final DeclarativeManifest declarativeManifest = new DeclarativeManifest()
        .withActorDefinitionId(actorDefinitionId)
        .withVersion(connectorBuilderPublishRequestBody.getInitialDeclarativeManifest().getVersion())
        .withDescription(connectorBuilderPublishRequestBody.getInitialDeclarativeManifest().getDescription())
        .withManifest(manifest)
        .withSpec(spec)
        .withComponentsFileContent(componentsFileContent);

    connectorBuilderService.insertActiveDeclarativeManifest(declarativeManifest);
    connectorBuilderService.assignActorDefinitionToConnectorBuilderProject(connectorBuilderPublishRequestBody.getBuilderProjectId(),
        actorDefinitionId);
    connectorBuilderService.deleteBuilderProjectDraft(connectorBuilderPublishRequestBody.getBuilderProjectId());

    return new SourceDefinitionIdBody().sourceDefinitionId(actorDefinitionId);
  }

  private UUID createActorDefinition(final String name,
                                     final UUID workspaceId,
                                     final JsonNode manifest,
                                     final JsonNode spec,
                                     final String componentFileContent)
      throws IOException {
    final ConnectorSpecification connectorSpecification = manifestInjector.createDeclarativeManifestConnectorSpecification(spec);
    final UUID actorDefinitionId = uuidSupplier.get();
    final StandardSourceDefinition source = new StandardSourceDefinition()
        .withSourceDefinitionId(actorDefinitionId)
        .withName(name)
        .withSourceType(SourceType.CUSTOM)
        .withTombstone(false)
        .withPublic(false)
        .withCustom(true);

    final ActorDefinitionVersion defaultVersion = new ActorDefinitionVersion()
        .withActorDefinitionId(actorDefinitionId)
        .withDockerImageTag(getImageVersionForManifest(manifest).getImageVersion())
        .withDockerRepository(AIRBYTE_SOURCE_DECLARATIVE_MANIFEST_IMAGE)
        .withSpec(connectorSpecification)
        .withProtocolVersion(DEFAULT_AIRBYTE_PROTOCOL_VERSION.serialize())
        .withReleaseStage(ReleaseStage.CUSTOM)
        .withSupportLevel(SupportLevel.NONE)
        .withInternalSupportLevel(100L)
        .withDocumentationUrl(connectorSpecification.getDocumentationUrl().toString());

    // Scope connector to the organization if present, otherwise scope to the workspace.
    final Optional<UUID> organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId);
    final UUID scopeId = organizationId.orElse(workspaceId);
    final ScopeType scopeType = organizationId.isPresent() ? ScopeType.ORGANIZATION : ScopeType.WORKSPACE;
    sourceService.writeCustomConnectorMetadata(source, defaultVersion, scopeId, scopeType);

    final List<ActorDefinitionConfigInjection> configInjectionsToCreate =
        manifestInjector.getManifestConnectorInjections(source.getSourceDefinitionId(), manifest, componentFileContent);
    connectorBuilderService.writeActorDefinitionConfigInjectionsForPath(configInjectionsToCreate);

    return source.getSourceDefinitionId();
  }

  public JsonNode updateConnectorBuilderProjectTestingValues(final ConnectorBuilderProjectTestingValuesUpdate testingValuesUpdate)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    try {
      validateProjectUnderRightWorkspace(testingValuesUpdate.getBuilderProjectId(), testingValuesUpdate.getWorkspaceId());
      final ConnectorBuilderProject project = connectorBuilderService.getConnectorBuilderProject(testingValuesUpdate.getBuilderProjectId(), false);
      final Optional<JsonNode> existingTestingValues = Optional.ofNullable(project.getTestingValues());

      final Optional<SecretPersistenceConfig> secretPersistenceConfig = getSecretPersistenceConfig(project.getWorkspaceId());
      final Optional<JsonNode> existingHydratedTestingValues = getHydratedTestingValues(project, secretPersistenceConfig.orElse(null));

      final JsonNode updatedTestingValues = existingHydratedTestingValues.isPresent()
          ? secretsProcessor.copySecrets(existingHydratedTestingValues.get(),
              testingValuesUpdate.getTestingValues(),
              testingValuesUpdate.getSpec())
          : testingValuesUpdate.getTestingValues();

      final JsonNode updatedTestingValuesWithSecretCoordinates = writeSecretsToSecretPersistence(existingTestingValues, updatedTestingValues,
          testingValuesUpdate.getSpec(), project.getWorkspaceId(), secretPersistenceConfig);

      connectorBuilderService.updateBuilderProjectTestingValues(testingValuesUpdate.getBuilderProjectId(), updatedTestingValuesWithSecretCoordinates);
      return secretsProcessor.prepareSecretsForOutput(updatedTestingValuesWithSecretCoordinates, testingValuesUpdate.getSpec());
    } catch (final ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.type, e.configId);
    }
  }

  public ConnectorBuilderProjectStreamRead readConnectorBuilderProjectStream(final ConnectorBuilderProjectStreamReadRequestBody requestBody)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    try {
      final ConnectorBuilderProject project = connectorBuilderService.getConnectorBuilderProject(requestBody.getBuilderProjectId(), false);
      final Optional<SecretPersistenceConfig> secretPersistenceConfig = getSecretPersistenceConfig(project.getWorkspaceId());
      final JsonNode existingHydratedTestingValues =
          getHydratedTestingValues(project, secretPersistenceConfig.orElse(null)).orElse(Jsons.emptyObject());

      final StreamReadRequestBody streamReadRequestBody =
          new StreamReadRequestBody(existingHydratedTestingValues,
              requestBody.getManifest(),
              requestBody.getStreamName(),
              requestBody.getCustomComponentsCode(),
              requestBody.getFormGeneratedManifest(),
              requestBody.getBuilderProjectId().toString(),
              requestBody.getRecordLimit(),
              requestBody.getPageLimit(),
              requestBody.getSliceLimit(),
              requestBody.getState(),
              requestBody.getWorkspaceId().toString());
      final StreamRead streamRead = connectorBuilderServerApiClient.readStream(streamReadRequestBody);

      final ConnectorBuilderProjectStreamRead builderProjectStreamRead = convertStreamRead(streamRead);

      if (streamRead.getLatestConfigUpdate() != null) {
        final JsonNode spec = requestBody.getManifest().get(SPEC_FIELD).get(CONNECTION_SPECIFICATION_FIELD);
        final JsonNode updatedTestingValuesWithSecretCoordinates = writeSecretsToSecretPersistence(Optional.ofNullable(project.getTestingValues()),
            Jsons.convertValue(streamRead.getLatestConfigUpdate(), JsonNode.class), spec, project.getWorkspaceId(), secretPersistenceConfig);
        connectorBuilderService.updateBuilderProjectTestingValues(project.getBuilderProjectId(), updatedTestingValuesWithSecretCoordinates);
        final JsonNode updatedTestingValuesWithObfuscatedSecrets =
            secretsProcessor.prepareSecretsForOutput(updatedTestingValuesWithSecretCoordinates, spec);
        builderProjectStreamRead.setLatestConfigUpdate(updatedTestingValuesWithObfuscatedSecrets);
      }

      return builderProjectStreamRead;
    } catch (final ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.type, e.configId);
    }
  }

  public ConnectorBuilderProjectFullResolveResponse fullResolveManifestBuilderProject(final ConnectorBuilderProjectFullResolveRequestBody requestBody)
      throws ConfigNotFoundException, IOException {
    final ConnectorBuilderProject project = connectorBuilderService.getConnectorBuilderProject(requestBody.getBuilderProjectId(), false);
    final Optional<SecretPersistenceConfig> secretPersistenceConfig = getSecretPersistenceConfig(project.getWorkspaceId());
    final JsonNode existingHydratedTestingValues =
        getHydratedTestingValues(project, secretPersistenceConfig.orElse(null)).orElse(Jsons.emptyObject());

    final FullResolveManifestRequestBody fullResolveManifestRequestBody =
        new FullResolveManifestRequestBody(existingHydratedTestingValues,
            requestBody.getManifest(),
            requestBody.getStreamLimit(),
            requestBody.getBuilderProjectId().toString(),
            requestBody.getWorkspaceId().toString());
    final ResolveManifest resolveManifest = connectorBuilderServerApiClient.fullResolveManifest(fullResolveManifestRequestBody);
    final ConnectorBuilderProjectFullResolveResponse builderProjectResolveManifest = convertResolveManifest(resolveManifest);
    return builderProjectResolveManifest;
  }

  public BuilderProjectForDefinitionResponse getConnectorBuilderProjectForDefinitionId(final BuilderProjectForDefinitionRequestBody requestBody)
      throws IOException, ConfigNotFoundException {
    final Optional<UUID> builderProjectId =
        connectorBuilderService.getConnectorBuilderProjectIdForActorDefinitionId(requestBody.getActorDefinitionId());

    Optional<UUID> workspaceId = Optional.empty();
    if (builderProjectId.isPresent()) {
      workspaceId = Optional.ofNullable(connectorBuilderService
          .getConnectorBuilderProject(builderProjectId.get(), false))
          .map(ConnectorBuilderProject::getWorkspaceId);
    }

    return new BuilderProjectForDefinitionResponse()
        .builderProjectId(builderProjectId.orElse(null))
        .workspaceId(workspaceId.orElse(null));
  }

  /**
   * Converts the provided {@code StreamRead} object into a {@code ConnectorBuilderProjectStreamRead}
   * object.
   * <p>
   * This method maps various properties from the {@code streamRead} instance including logs, slices,
   * auxiliary requests, inferred schema, and inferred datetime formats. It also transfers the test
   * read limit flag.
   *
   * @param streamRead the {@code StreamRead} instance containing the original data to be converted.
   * @return a new {@code ConnectorBuilderProjectStreamRead} instance populated with the mapped values
   *         from {@code streamRead}.
   */
  private ConnectorBuilderProjectStreamRead convertStreamRead(final StreamRead streamRead) {
    return new ConnectorBuilderProjectStreamRead()
        .logs(mapStreamReadLogs(streamRead))
        .slices(mapStreamReadSlices(streamRead))
        .testReadLimitReached(streamRead.getTestReadLimitReached())
        .auxiliaryRequests(mapGlobalAuxiliaryRequests(streamRead))
        .inferredSchema(streamRead.getInferredSchema())
        .inferredDatetimeFormats(streamRead.getInferredDatetimeFormats());
  }

  /**
   * Converts the provided {@code ResolveManifest} object into a
   * {@code ConnectorBuilderProjectFullResolveResponse} object.
   * <p>
   * This method maps manifest property from the {@code resolveManifest} instance.
   *
   * @param resolveManifest the {@code ResolveManifest} instance containing the original data to be
   *        converted.
   * @return a new {@code ConnectorBuilderProjectFullResolveResponse} instance populated with the
   *         mapped value from {@code resolveManifest}.
   */
  private ConnectorBuilderProjectFullResolveResponse convertResolveManifest(final ResolveManifest resolveManifest) {
    return new ConnectorBuilderProjectFullResolveResponse()
        .manifest(resolveManifest.getManifest());
  }

  /**
   * Converts the logs contained within a {@code StreamRead} object into a list of
   * {@code ConnectorBuilderProjectStreamReadLogsInner} instances.
   *
   * <p>
   * Each log entry in the provided {@code streamRead} is transformed by mapping its message, level
   * (converted via {@code LevelEnum.fromString}), internal message, and stacktrace.
   * </p>
   *
   * @param streamRead the {@code StreamRead} instance containing the logs to be mapped
   * @return a list of {@code ConnectorBuilderProjectStreamReadLogsInner} objects representing the
   *         mapped log entries
   */
  private List<ConnectorBuilderProjectStreamReadLogsInner> mapStreamReadLogs(final StreamRead streamRead) {
    return streamRead.getLogs().stream().map(log -> new ConnectorBuilderProjectStreamReadLogsInner()
        .message(log.getMessage())
        .level(ConnectorBuilderProjectStreamReadLogsInner.LevelEnum.fromString(log.getLevel().getValue()))
        .internalMessage(log.getInternalMessage())
        .stacktrace(log.getStacktrace()))
        .toList();
  }

  /**
   * Maps the slices from the given StreamRead instance into a list of
   * ConnectorBuilderProjectStreamReadSlicesInner objects.
   *
   * <p>
   * This method iterates through each slice in the provided StreamRead, creating a corresponding
   * ConnectorBuilderProjectStreamReadSlicesInner for each slice. For each slice, it:
   * <ul>
   * <li>Sets the slice descriptor.</li>
   * <li>Ensures the state list is not empty; otherwise, it uses an empty list.</li>
   * <li>Converts each page by mapping records and transforming both the HTTP request and
   * response.</li>
   * <li>Processes auxiliary requests specific to the slice.</li>
   * </ul>
   *
   * @param streamRead the StreamRead instance containing slices to be mapped.
   * @return a list of ConnectorBuilderProjectStreamReadSlicesInner objects constructed from the
   *         StreamRead slices.
   */
  private List<ConnectorBuilderProjectStreamReadSlicesInner> mapStreamReadSlices(final StreamRead streamRead) {
    return streamRead.getSlices().stream().map(slice -> new ConnectorBuilderProjectStreamReadSlicesInner()
        .sliceDescriptor(slice.getSliceDescriptor())
        .state(CollectionUtils.isNotEmpty(slice.getState()) ? slice.getState() : List.of())
        .pages(slice.getPages().stream()
            .map(page -> new ConnectorBuilderProjectStreamReadSlicesInnerPagesInner()
                .records(page.getRecords())
                .request(convertHttpRequest(page.getRequest()))
                .response(convertHttpResponse(page.getResponse())))
            .toList())
        .auxiliaryRequests(mapSliceAuxiliaryRequests(slice)))
        .toList();
  }

  /**
   * Converts a list of AuxiliaryRequest objects into a list of ConnectorBuilderAuxiliaryRequest
   * objects.
   *
   * <p>
   * This method first checks if the provided list is not empty. If it is not empty, it streams over
   * the list, mapping each AuxiliaryRequest into a ConnectorBuilderAuxiliaryRequest by copying the
   * description, title, and converting the HTTP request and response fields as well as mapping the
   * type using the TypeEnum. If the input list is empty, it returns an empty list.
   *
   * @param auxiliaryRequests the list of AuxiliaryRequest objects to be converted
   * @return a list of converted ConnectorBuilderAuxiliaryRequest objects, or an empty list if the
   *         input is empty
   */
  private List<ConnectorBuilderAuxiliaryRequest> mapAuxiliaryRequests(List<AuxiliaryRequest> auxiliaryRequests) {
    return CollectionUtils.isNotEmpty(auxiliaryRequests)
        ? auxiliaryRequests.stream()
            .map(auxRequest -> new ConnectorBuilderAuxiliaryRequest()
                .description(auxRequest.getDescription())
                .request(convertHttpRequest(auxRequest.getRequest()))
                .response(convertHttpResponse(auxRequest.getResponse()))
                .title(auxRequest.getTitle())
                .type(ConnectorBuilderAuxiliaryRequest.TypeEnum.fromString(auxRequest.getType().getValue())))
            .toList()
        : List.of();
  }

  /**
   * Maps the global auxiliary requests from the provided StreamRead instance.
   *
   * <p>
   * This method retrieves the auxiliary requests from the StreamRead object and delegates the mapping
   * to the mapAuxiliaryRequests method.
   *
   * @param streamRead the StreamRead instance that contains the auxiliary requests
   * @return a list of ConnectorBuilderAuxiliaryRequest objects generated from the auxiliary requests
   */
  private List<ConnectorBuilderAuxiliaryRequest> mapGlobalAuxiliaryRequests(final StreamRead streamRead) {
    return mapAuxiliaryRequests(streamRead.getAuxiliaryRequests());
  }

  /**
   * Maps the auxiliary requests from the given slice into a list of ConnectorBuilderAuxiliaryRequest
   * instances.
   *
   * @param slice the input slice containing auxiliary request data
   * @return a list of ConnectorBuilderAuxiliaryRequest objects mapped from the slice's auxiliary
   *         requests
   */
  private List<ConnectorBuilderAuxiliaryRequest> mapSliceAuxiliaryRequests(final StreamReadSlicesInner slice) {
    return mapAuxiliaryRequests(slice.getAuxiliaryRequests());
  }

  private ConnectorBuilderHttpRequest convertHttpRequest(@Nullable final HttpRequest request) {
    return request != null
        ? new ConnectorBuilderHttpRequest()
            .url(request.getUrl())
            .httpMethod(ConnectorBuilderHttpRequest.HttpMethodEnum.fromString(request.getHttpMethod().getValue()))
            .body(request.getBody())
            .headers(request.getHeaders())
        : null;
  }

  private ConnectorBuilderHttpResponse convertHttpResponse(final HttpResponse response) {
    return response != null
        ? new ConnectorBuilderHttpResponse()
            .status(response.getStatus())
            .body(response.getBody())
            .headers(response.getHeaders())
        : null;
  }

  private JsonNode writeSecretsToSecretPersistence(final Optional<JsonNode> existingTestingValues,
                                                   final JsonNode updatedTestingValues,
                                                   final JsonNode spec,
                                                   final UUID workspaceId,
                                                   final Optional<SecretPersistenceConfig> secretPersistenceConfig)
      throws JsonValidationException {

    final var secretPersistence = secretPersistenceConfig.map(c -> new RuntimeSecretPersistence(c, metricClient)).orElse(null);
    if (existingTestingValues.isPresent()) {
      return secretsRepositoryWriter.updateFromConfigLegacy(workspaceId, existingTestingValues.get(), updatedTestingValues, spec, secretPersistence);
    }
    return secretsRepositoryWriter.createFromConfigLegacy(workspaceId, updatedTestingValues, spec, secretPersistence);
  }

  private Optional<SecretPersistenceConfig> getSecretPersistenceConfig(final UUID workspaceId) throws IOException, ConfigNotFoundException {
    try {
      final Optional<UUID> organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId);
      return organizationId.isPresent()
          && featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(organizationId.get()))
              ? Optional.of(secretPersistenceConfigService.get(ScopeType.ORGANIZATION, organizationId.get()))
              : Optional.empty();
    } catch (final ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.type, e.configId);
    }
  }

  private Optional<JsonNode> getHydratedTestingValues(final ConnectorBuilderProject project,
                                                      @Nullable final SecretPersistenceConfig secretPersistenceConfig) {
    final Optional<JsonNode> testingValues = Optional.ofNullable(project.getTestingValues());
    final Optional<SecretPersistenceConfig> secretPersistenceConfigOptional = Optional.ofNullable(secretPersistenceConfig);

    return testingValues.isPresent()
        ? secretPersistenceConfigOptional.isPresent()
            ? Optional.ofNullable(secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(testingValues.get(),
                new RuntimeSecretPersistence(secretPersistenceConfigOptional.get(), metricClient)))
            : Optional.ofNullable(secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(project.getTestingValues()))
        : Optional.empty();
  }

  public DeclarativeManifestBaseImageRead getDeclarativeManifestBaseImage(final DeclarativeManifestRequestBody declarativeManifestRequestBody) {
    final JsonNode declarativeManifest = declarativeManifestRequestBody.getManifest();
    final DeclarativeManifestImageVersion declarativeManifestImageVersion = getImageVersionForManifest(declarativeManifest);
    final String baseImage =
        String.format("docker.io/%s:%s@%s", AIRBYTE_SOURCE_DECLARATIVE_MANIFEST_IMAGE, declarativeManifestImageVersion.getImageVersion(),
            declarativeManifestImageVersion.getImageSha());
    return new DeclarativeManifestBaseImageRead().baseImage(baseImage);
  }

  private DeclarativeManifestImageVersion getImageVersionForManifest(final JsonNode declarativeManifest) {
    final Version manifestVersion = manifestInjector.getCdkVersion(declarativeManifest);
    return declarativeManifestImageVersionService
        .getDeclarativeManifestImageVersionByMajorVersion(Integer.parseInt(manifestVersion.getMajorVersion()));
  }

  public ConnectorBuilderProjectIdWithWorkspaceId createForkedConnectorBuilderProject(final ConnectorBuilderProjectForkRequestBody requestBody)
      throws IOException, ConfigNotFoundException, JsonValidationException {
    final StandardSourceDefinition sourceDefinition = sourceService.getStandardSourceDefinition(requestBody.getBaseActorDefinitionId());
    final ActorDefinitionVersion defaultVersion = actorDefinitionService.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId());
    final JsonNode manifest = remoteDefinitionsProvider.getConnectorManifest(defaultVersion.getDockerRepository(), defaultVersion.getDockerImageTag())
        .orElseGet(() -> {
          final String errorMessage = "Could not fork connector: no manifest file available for %s:%s".formatted(defaultVersion.getDockerRepository(),
              defaultVersion.getDockerImageTag());
          log.error(errorMessage);
          throw new NotFoundException(errorMessage);
        });

    final String customComponentsContent =
        remoteDefinitionsProvider.getConnectorCustomComponents(defaultVersion.getDockerRepository(), defaultVersion.getDockerImageTag())
            .orElse(null);

    final ConnectorBuilderProjectDetails projectDetails = new ConnectorBuilderProjectDetails().name(sourceDefinition.getName())
        .baseActorDefinitionVersionId(defaultVersion.getVersionId()).draftManifest(manifest).componentsFileContent(customComponentsContent);
    return createConnectorBuilderProject(
        new ConnectorBuilderProjectWithWorkspaceId().workspaceId(requestBody.getWorkspaceId()).builderProject(projectDetails));
  }

  public OAuthConsentRead getConnectorBuilderProjectOAuthConsent(final BuilderProjectOauthConsentRequest requestBody)
      throws JsonValidationException, ConfigNotFoundException, IOException {

    final ConnectorBuilderProject project = connectorBuilderService.getConnectorBuilderProject(requestBody.getBuilderProjectId(), true);
    final ConnectorSpecification spec = Jsons.object(project.getManifestDraft().get("spec"), ConnectorSpecification.class);

    final Optional<SecretPersistenceConfig> secretPersistenceConfig = getSecretPersistenceConfig(project.getWorkspaceId());
    final JsonNode existingHydratedTestingValues =
        getHydratedTestingValues(project, secretPersistenceConfig.orElse(null)).orElse(Jsons.emptyObject());

    final Map<String, Object> traceTags = Map.of(WORKSPACE_ID_KEY, requestBody.getWorkspaceId(), CONNECTOR_BUILDER_PROJECT_ID_KEY,
        requestBody.getBuilderProjectId());
    ApmTraceUtils.addTagsToTrace(traceTags);
    ApmTraceUtils.addTagsToRootSpan(traceTags);

    final OAuthConfigSpecification oauthConfigSpecification = spec.getAdvancedAuth().getOauthConfigSpecification();
    OAuthHelper.updateOauthConfigToAcceptAdditionalUserInputProperties(oauthConfigSpecification);

    final DeclarativeOAuthFlow oAuthFlowImplementation = oAuthImplementationFactory.createDeclarativeOAuthImplementation(spec);
    return new OAuthConsentRead().consentUrl(oAuthFlowImplementation.getSourceConsentUrl(
        requestBody.getWorkspaceId(),
        null,
        requestBody.getRedirectUrl(),
        existingHydratedTestingValues,
        oauthConfigSpecification,
        existingHydratedTestingValues));
  }

  public CompleteOAuthResponse completeConnectorBuilderProjectOAuth(final CompleteConnectorBuilderProjectOauthRequest requestBody)
      throws JsonValidationException, ConfigNotFoundException, IOException {

    final ConnectorBuilderProject project = connectorBuilderService.getConnectorBuilderProject(requestBody.getBuilderProjectId(), true);
    final ConnectorSpecification spec = Jsons.object(project.getManifestDraft().get("spec"), ConnectorSpecification.class);

    final Optional<SecretPersistenceConfig> secretPersistenceConfig = getSecretPersistenceConfig(project.getWorkspaceId());
    final JsonNode existingHydratedTestingValues =
        getHydratedTestingValues(project, secretPersistenceConfig.orElse(null)).orElse(Jsons.emptyObject());

    final Map<String, Object> traceTags = Map.of(WORKSPACE_ID_KEY, requestBody.getWorkspaceId(), CONNECTOR_BUILDER_PROJECT_ID_KEY,
        requestBody.getBuilderProjectId());
    ApmTraceUtils.addTagsToTrace(traceTags);
    ApmTraceUtils.addTagsToRootSpan(traceTags);

    final OAuthConfigSpecification oauthConfigSpecification = spec.getAdvancedAuth().getOauthConfigSpecification();
    OAuthHelper.updateOauthConfigToAcceptAdditionalUserInputProperties(oauthConfigSpecification);

    final DeclarativeOAuthFlow oAuthFlowImplementation = oAuthImplementationFactory.createDeclarativeOAuthImplementation(spec);
    final Map<String, Object> result = oAuthFlowImplementation.completeSourceOAuth(
        requestBody.getWorkspaceId(),
        null,
        requestBody.getQueryParams(),
        requestBody.getRedirectUrl(),
        existingHydratedTestingValues,
        oauthConfigSpecification,
        existingHydratedTestingValues);

    return OAuthHelper.mapToCompleteOAuthResponse(result);
  }

}
