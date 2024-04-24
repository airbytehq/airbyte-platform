/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.version.AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.api.model.generated.ConnectorBuilderHttpRequest;
import io.airbyte.api.model.generated.ConnectorBuilderHttpResponse;
import io.airbyte.api.model.generated.ConnectorBuilderProjectDetailsRead;
import io.airbyte.api.model.generated.ConnectorBuilderProjectIdWithWorkspaceId;
import io.airbyte.api.model.generated.ConnectorBuilderProjectRead;
import io.airbyte.api.model.generated.ConnectorBuilderProjectReadList;
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamRead;
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadAuxiliaryRequestsInner;
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadRequestBody;
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadSlicesInner;
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadSlicesInnerPagesInner;
import io.airbyte.api.model.generated.ConnectorBuilderProjectTestingValuesUpdate;
import io.airbyte.api.model.generated.ConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.api.model.generated.ConnectorBuilderPublishRequestBody;
import io.airbyte.api.model.generated.DeclarativeManifestRead;
import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.api.model.generated.SourceDefinitionIdBody;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.handlers.helpers.BuilderProjectUpdater;
import io.airbyte.commons.server.handlers.helpers.DeclarativeSourceManifestInjector;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.ConnectorBuilderProject;
import io.airbyte.config.ConnectorBuilderProjectVersionedManifest;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.ScopeType;
import io.airbyte.config.SecretPersistenceConfig;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSourceDefinition.SourceType;
import io.airbyte.config.SupportLevel;
import io.airbyte.config.init.CdkVersionProvider;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.secrets.JsonSecretsProcessor;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence;
import io.airbyte.connectorbuilderserver.api.client.generated.ConnectorBuilderServerApi;
import io.airbyte.connectorbuilderserver.api.client.model.generated.HttpRequest;
import io.airbyte.connectorbuilderserver.api.client.model.generated.HttpResponse;
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamRead;
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadRequestBody;
import io.airbyte.data.services.ConnectorBuilderService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Organization;
import io.airbyte.featureflag.UseRuntimeSecretPersistence;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * ConnectorBuilderProjectsHandler. Javadocs suppressed because api docs should be used as source of
 * truth.
 */
@Singleton
public class ConnectorBuilderProjectsHandler {

  private final ConfigRepository configRepository;

  private final BuilderProjectUpdater buildProjectUpdater;
  private final Supplier<UUID> uuidSupplier;
  private final DeclarativeSourceManifestInjector manifestInjector;
  private final CdkVersionProvider cdkVersionProvider;
  private final WorkspaceService workspaceService;
  private final FeatureFlagClient featureFlagClient;
  private final SecretsRepositoryReader secretsRepositoryReader;
  private final SecretsRepositoryWriter secretsRepositoryWriter;
  private final SecretPersistenceConfigService secretPersistenceConfigService;
  private final ConnectorBuilderService connectorBuilderService;
  private final JsonSecretsProcessor secretsProcessor;
  private final ConnectorBuilderServerApi connectorBuilderServerApiClient;

  public static final String SPEC_FIELD = "spec";
  public static final String CONNECTION_SPECIFICATION_FIELD = "connection_specification";

  @Inject
  public ConnectorBuilderProjectsHandler(final ConfigRepository configRepository,
                                         final BuilderProjectUpdater builderProjectUpdater,
                                         final CdkVersionProvider cdkVersionProvider,
                                         @Named("uuidGenerator") final Supplier<UUID> uuidSupplier,
                                         final DeclarativeSourceManifestInjector manifestInjector,
                                         final WorkspaceService workspaceService,
                                         final FeatureFlagClient featureFlagClient,
                                         final SecretsRepositoryReader secretsRepositoryReader,
                                         final SecretsRepositoryWriter secretsRepositoryWriter,
                                         final SecretPersistenceConfigService secretPersistenceConfigService,
                                         final ConnectorBuilderService connectorBuilderService,
                                         @Named("jsonSecretsProcessorWithCopy") final JsonSecretsProcessor secretsProcessor,
                                         final ConnectorBuilderServerApi connectorBuilderServerApiClient) {
    this.configRepository = configRepository;
    this.buildProjectUpdater = builderProjectUpdater;
    this.cdkVersionProvider = cdkVersionProvider;
    this.uuidSupplier = uuidSupplier;
    this.manifestInjector = manifestInjector;
    this.workspaceService = workspaceService;
    this.featureFlagClient = featureFlagClient;
    this.secretsRepositoryReader = secretsRepositoryReader;
    this.secretsRepositoryWriter = secretsRepositoryWriter;
    this.secretPersistenceConfigService = secretPersistenceConfigService;
    this.connectorBuilderService = connectorBuilderService;
    this.secretsProcessor = secretsProcessor;
    this.connectorBuilderServerApiClient = connectorBuilderServerApiClient;
  }

  private static ConnectorBuilderProjectDetailsRead builderProjectToDetailsRead(final ConnectorBuilderProject project) {
    return new ConnectorBuilderProjectDetailsRead().name(project.getName()).builderProjectId(project.getBuilderProjectId())
        .sourceDefinitionId(project.getActorDefinitionId())
        .activeDeclarativeManifestVersion(
            project.getActiveDeclarativeManifestVersion())
        .hasDraft(project.getHasDraft());
  }

  private ConnectorBuilderProjectIdWithWorkspaceId buildIdResponseFromId(final UUID projectId, final UUID workspaceId) {
    return new ConnectorBuilderProjectIdWithWorkspaceId().workspaceId(workspaceId).builderProjectId(projectId);
  }

  private void validateProjectUnderRightWorkspace(final UUID projectId, final UUID workspaceId) throws ConfigNotFoundException, IOException {
    final ConnectorBuilderProject project = configRepository.getConnectorBuilderProject(projectId, false);
    validateProjectUnderRightWorkspace(project, workspaceId);
  }

  private void validateProjectUnderRightWorkspace(final ConnectorBuilderProject project, final UUID workspaceId) throws ConfigNotFoundException {
    final UUID actualWorkspaceId = project.getWorkspaceId();
    if (!actualWorkspaceId.equals(workspaceId)) {
      throw new ConfigNotFoundException(ConfigSchema.CONNECTOR_BUILDER_PROJECT, project.getBuilderProjectId().toString());
    }
  }

  public ConnectorBuilderProjectIdWithWorkspaceId createConnectorBuilderProject(final ConnectorBuilderProjectWithWorkspaceId projectCreate)
      throws IOException {
    final UUID id = uuidSupplier.get();

    configRepository.writeBuilderProjectDraft(id, projectCreate.getWorkspaceId(), projectCreate.getBuilderProject().getName(),
        new ObjectMapper().valueToTree(projectCreate.getBuilderProject().getDraftManifest()));

    return buildIdResponseFromId(id, projectCreate.getWorkspaceId());
  }

  public void updateConnectorBuilderProject(final ExistingConnectorBuilderProjectWithWorkspaceId projectUpdate)
      throws ConfigNotFoundException, IOException {

    final ConnectorBuilderProject connectorBuilderProject = configRepository.getConnectorBuilderProject(projectUpdate.getBuilderProjectId(), false);
    validateProjectUnderRightWorkspace(connectorBuilderProject, projectUpdate.getWorkspaceId());

    buildProjectUpdater.persistBuilderProjectUpdate(projectUpdate);
  }

  public void deleteConnectorBuilderProject(final ConnectorBuilderProjectIdWithWorkspaceId projectDelete)
      throws IOException, ConfigNotFoundException {
    validateProjectUnderRightWorkspace(projectDelete.getBuilderProjectId(), projectDelete.getWorkspaceId());
    configRepository.deleteBuilderProject(projectDelete.getBuilderProjectId());
  }

  public ConnectorBuilderProjectRead getConnectorBuilderProjectWithManifest(final ConnectorBuilderProjectIdWithWorkspaceId request)
      throws IOException, ConfigNotFoundException {

    if (request.getVersion() != null) {
      validateProjectUnderRightWorkspace(request.getBuilderProjectId(), request.getWorkspaceId());
      return buildConnectorBuilderProjectVersionManifestRead(
          configRepository.getVersionedConnectorBuilderProject(request.getBuilderProjectId(), request.getVersion()));
    }

    return getWithManifestWithoutVersion(request);
  }

  private ConnectorBuilderProjectRead getWithManifestWithoutVersion(final ConnectorBuilderProjectIdWithWorkspaceId request)
      throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject project = configRepository.getConnectorBuilderProject(request.getBuilderProjectId(), true);
    validateProjectUnderRightWorkspace(project, request.getWorkspaceId());
    final ConnectorBuilderProjectRead response = new ConnectorBuilderProjectRead().builderProject(builderProjectToDetailsRead(project));

    if (project.getManifestDraft() != null) {
      response.setDeclarativeManifest(new DeclarativeManifestRead()
          .manifest(project.getManifestDraft())
          .isDraft(true));
      response.setTestingValues(maskSecrets(project.getTestingValues(), project.getManifestDraft()));
    } else if (project.getActorDefinitionId() != null) {
      final DeclarativeManifest declarativeManifest = configRepository.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(
          project.getActorDefinitionId());
      response.setDeclarativeManifest(new DeclarativeManifestRead()
          .isDraft(false)
          .manifest(declarativeManifest.getManifest())
          .version(declarativeManifest.getVersion())
          .description(declarativeManifest.getDescription()));
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
            .sourceDefinitionId(project.getSourceDefinitionId()))
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

    final Stream<ConnectorBuilderProject> projects = configRepository.getConnectorBuilderProjectsByWorkspace(workspaceIdRequestBody.getWorkspaceId());

    return new ConnectorBuilderProjectReadList().projects(projects.map(ConnectorBuilderProjectsHandler::builderProjectToDetailsRead).toList());
  }

  public SourceDefinitionIdBody publishConnectorBuilderProject(final ConnectorBuilderPublishRequestBody connectorBuilderPublishRequestBody)
      throws IOException {
    final JsonNode manifest = connectorBuilderPublishRequestBody.getInitialDeclarativeManifest().getManifest();
    final JsonNode spec = connectorBuilderPublishRequestBody.getInitialDeclarativeManifest().getSpec();
    manifestInjector.addInjectedDeclarativeManifest(spec);
    final UUID actorDefinitionId = createActorDefinition(connectorBuilderPublishRequestBody.getName(),
        connectorBuilderPublishRequestBody.getWorkspaceId(),
        manifest,
        spec);

    final DeclarativeManifest declarativeManifest = new DeclarativeManifest()
        .withActorDefinitionId(actorDefinitionId)
        .withVersion(connectorBuilderPublishRequestBody.getInitialDeclarativeManifest().getVersion())
        .withDescription(connectorBuilderPublishRequestBody.getInitialDeclarativeManifest().getDescription())
        .withManifest(manifest)
        .withSpec(spec);
    configRepository.insertActiveDeclarativeManifest(declarativeManifest);
    configRepository.assignActorDefinitionToConnectorBuilderProject(connectorBuilderPublishRequestBody.getBuilderProjectId(), actorDefinitionId);
    configRepository.deleteBuilderProjectDraft(connectorBuilderPublishRequestBody.getBuilderProjectId());

    return new SourceDefinitionIdBody().sourceDefinitionId(actorDefinitionId);
  }

  private UUID createActorDefinition(final String name, final UUID workspaceId, final JsonNode manifest, final JsonNode spec) throws IOException {
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
        .withDockerImageTag(cdkVersionProvider.getCdkVersion())
        .withDockerRepository("airbyte/source-declarative-manifest")
        .withSpec(connectorSpecification)
        .withProtocolVersion(DEFAULT_AIRBYTE_PROTOCOL_VERSION.serialize())
        .withReleaseStage(ReleaseStage.CUSTOM)
        .withSupportLevel(SupportLevel.NONE)
        .withDocumentationUrl(connectorSpecification.getDocumentationUrl().toString());

    configRepository.writeCustomConnectorMetadata(source, defaultVersion, workspaceId, ScopeType.WORKSPACE);
    configRepository.writeActorDefinitionConfigInjectionForPath(manifestInjector.createConfigInjection(source.getSourceDefinitionId(), manifest));

    return source.getSourceDefinitionId();
  }

  @SuppressWarnings("PMD.PreserveStackTrace")
  public JsonNode updateConnectorBuilderProjectTestingValues(final ConnectorBuilderProjectTestingValuesUpdate testingValuesUpdate)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    try {
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
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
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
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  private ConnectorBuilderProjectStreamRead convertStreamRead(final StreamRead streamRead) {
    return new ConnectorBuilderProjectStreamRead()
        .logs(streamRead.getLogs())
        .slices(streamRead.getSlices().stream().map(slice -> new ConnectorBuilderProjectStreamReadSlicesInner()
            .sliceDescriptor(slice.getSliceDescriptor())
            .state(slice.getState())
            .pages(slice.getPages().stream().map(page -> new ConnectorBuilderProjectStreamReadSlicesInnerPagesInner()
                .records(page.getRecords())
                .request(convertHttpRequest(page.getRequest()))
                .response(convertHttpResponse(page.getResponse()))).toList()))
            .toList())
        .testReadLimitReached(streamRead.getTestReadLimitReached())
        .auxiliaryRequests(streamRead.getAuxiliaryRequests() != null
            ? streamRead.getAuxiliaryRequests().stream().map(auxRequest -> new ConnectorBuilderProjectStreamReadAuxiliaryRequestsInner()
                .description(auxRequest.getDescription())
                .request(convertHttpRequest(auxRequest.getRequest()))
                .response(convertHttpResponse(auxRequest.getResponse()))
                .title(auxRequest.getTitle())).toList()
            : null)
        .inferredSchema(streamRead.getInferredSchema())
        .inferredDatetimeFormats(streamRead.getInferredDatetimeFormats());
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
    return secretPersistenceConfig.isPresent()
        ? secretsRepositoryWriter.statefulUpdateSecretsToRuntimeSecretPersistence(
            workspaceId,
            existingTestingValues,
            updatedTestingValues,
            spec,
            true,
            new RuntimeSecretPersistence(secretPersistenceConfig.get()))
        : secretsRepositoryWriter.statefulUpdateSecretsToDefaultSecretPersistence(
            workspaceId,
            existingTestingValues,
            updatedTestingValues,
            spec,
            true);
  }

  private Optional<SecretPersistenceConfig> getSecretPersistenceConfig(final UUID workspaceId) throws IOException, ConfigNotFoundException {
    try {
      final Optional<UUID> organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId);
      return organizationId.isPresent()
          && featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(organizationId.get()))
              ? Optional.of(secretPersistenceConfigService.getSecretPersistenceConfig(ScopeType.ORGANIZATION, organizationId.get()))
              : Optional.empty();
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  private Optional<JsonNode> getHydratedTestingValues(final ConnectorBuilderProject project,
                                                      @Nullable final SecretPersistenceConfig secretPersistenceConfig) {
    final Optional<JsonNode> testingValues = Optional.ofNullable(project.getTestingValues());
    final Optional<SecretPersistenceConfig> secretPersistenceConfigOptional = Optional.ofNullable(secretPersistenceConfig);

    return testingValues.isPresent()
        ? secretPersistenceConfigOptional.isPresent()
            ? Optional.ofNullable(secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(testingValues.get(),
                new RuntimeSecretPersistence(secretPersistenceConfigOptional.get())))
            : Optional.ofNullable(secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(project.getTestingValues()))
        : Optional.empty();
  }

}
