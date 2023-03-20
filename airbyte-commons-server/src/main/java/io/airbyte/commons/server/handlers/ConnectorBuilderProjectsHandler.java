/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.version.AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.api.model.generated.ConnectorBuilderProjectDetailsRead;
import io.airbyte.api.model.generated.ConnectorBuilderProjectIdWithWorkspaceId;
import io.airbyte.api.model.generated.ConnectorBuilderProjectRead;
import io.airbyte.api.model.generated.ConnectorBuilderProjectReadList;
import io.airbyte.api.model.generated.ConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.api.model.generated.ConnectorBuilderPublishRequestBody;
import io.airbyte.api.model.generated.DeclarativeManifestRead;
import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.api.model.generated.SourceDefinitionIdBody;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.server.handlers.helpers.ConnectorBuilderSpecAdapter;
import io.airbyte.config.ActorDefinitionConfigInjection;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.ConnectorBuilderProject;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSourceDefinition.ReleaseStage;
import io.airbyte.config.StandardSourceDefinition.SourceType;
import io.airbyte.config.init.CdkVersionProvider;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.protocol.models.ConnectorSpecification;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * ConnectorBuilderProjectsHandler. Javadocs suppressed because api docs should be used as source of
 * truth.
 */
@SuppressWarnings({"MissingJavadocMethod"})
@Singleton
public class ConnectorBuilderProjectsHandler {

  private final ConfigRepository configRepository;
  private final Supplier<UUID> uuidSupplier;
  private final ConnectorBuilderSpecAdapter specAdapter;
  private final CdkVersionProvider cdkVersionProvider;

  @Inject
  public ConnectorBuilderProjectsHandler(final ConfigRepository configRepository,
                                         final CdkVersionProvider cdkVersionProvider,
                                         final Supplier<UUID> uuidSupplier,
                                         final ConnectorBuilderSpecAdapter specAdapter) {
    this.configRepository = configRepository;
    this.cdkVersionProvider = cdkVersionProvider;
    this.uuidSupplier = uuidSupplier;
    this.specAdapter = specAdapter;
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

  private void validateWorkspace(final UUID projectId, final UUID workspaceId) throws ConfigNotFoundException, IOException {
    final ConnectorBuilderProject project = configRepository.getConnectorBuilderProject(projectId, false);
    final UUID actualWorkspaceId = project.getWorkspaceId();
    if (!actualWorkspaceId.equals(workspaceId)) {
      throw new ConfigNotFoundException(ConfigSchema.CONNECTOR_BUILDER_PROJECT, projectId.toString());
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
      throws IOException, ConfigNotFoundException {
    validateWorkspace(projectUpdate.getBuilderProjectId(), projectUpdate.getWorkspaceId());

    configRepository.writeBuilderProjectDraft(
        projectUpdate.getBuilderProjectId(),
        projectUpdate.getWorkspaceId(),
        projectUpdate.getBuilderProject().getName(),
        projectUpdate.getBuilderProject().getDraftManifest());
  }

  public void deleteConnectorBuilderProject(final ConnectorBuilderProjectIdWithWorkspaceId projectDelete)
      throws IOException, ConfigNotFoundException {
    validateWorkspace(projectDelete.getBuilderProjectId(), projectDelete.getWorkspaceId());
    configRepository.deleteBuilderProject(projectDelete.getBuilderProjectId());
  }

  public ConnectorBuilderProjectRead getConnectorBuilderProjectWithManifest(final ConnectorBuilderProjectIdWithWorkspaceId request)
      throws IOException, ConfigNotFoundException {
    validateWorkspace(request.getBuilderProjectId(), request.getWorkspaceId());
    final ConnectorBuilderProject project = configRepository.getConnectorBuilderProject(request.getBuilderProjectId(), true);
    final ConnectorBuilderProjectRead response = new ConnectorBuilderProjectRead().builderProject(builderProjectToDetailsRead(project));
    if (project.getManifestDraft() != null) {
      final DeclarativeManifestRead manifest = new DeclarativeManifestRead()
          .manifest(project.getManifestDraft()).isDraft(true);
      response.setDeclarativeManifest(manifest);
    }

    return response;
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
    final UUID actorDefinitionId = createActorDefinition(connectorBuilderPublishRequestBody.getName(),
        connectorBuilderPublishRequestBody.getWorkspaceId(),
        manifest,
        spec);

    final DeclarativeManifest declarativeManifest = new DeclarativeManifest()
        .withActorDefinitionId(actorDefinitionId)
        .withVersion(connectorBuilderPublishRequestBody.getInitialDeclarativeManifest().getVersion().longValue())
        .withDescription(connectorBuilderPublishRequestBody.getInitialDeclarativeManifest().getDescription())
        .withManifest(manifest)
        .withSpec(spec);
    configRepository.insertActiveDeclarativeManifest(declarativeManifest);
    configRepository.assignActorDefinitionToConnectorBuilderProject(connectorBuilderPublishRequestBody.getBuilderProjectId(), actorDefinitionId);

    return new SourceDefinitionIdBody().sourceDefinitionId(actorDefinitionId);
  }

  private UUID createActorDefinition(final String name, final UUID workspaceId, final JsonNode manifest, final JsonNode spec) throws IOException {
    final ConnectorSpecification connectorSpecification = specAdapter.adapt(spec);
    final StandardSourceDefinition source = new StandardSourceDefinition()
        .withSourceDefinitionId(uuidSupplier.get())
        .withName(name)
        .withDockerImageTag(cdkVersionProvider.getCdkVersion())
        .withDockerRepository("airbyte/source-declarative-manifest")
        .withSourceType(SourceType.CUSTOM)
        .withSpec(connectorSpecification)
        .withTombstone(false)
        .withProtocolVersion(DEFAULT_AIRBYTE_PROTOCOL_VERSION.serialize())
        .withPublic(false)
        .withCustom(true)
        .withReleaseStage(ReleaseStage.CUSTOM)
        .withDocumentationUrl(connectorSpecification.getDocumentationUrl().toString());
    configRepository.writeCustomSourceDefinition(source, workspaceId);

    configRepository.writeActorDefinitionConfigInjectionForPath(
        new ActorDefinitionConfigInjection()
            .withActorDefinitionId(source.getSourceDefinitionId())
            .withInjectionPath("__injected_declarative_manifest")
            .withJsonToInject(manifest));
    return source.getSourceDefinitionId();
  }

}
