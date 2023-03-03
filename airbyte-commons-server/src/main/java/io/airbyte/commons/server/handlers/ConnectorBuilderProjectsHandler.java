/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.api.model.generated.ConnectorBuilderProjectDetailsRead;
import io.airbyte.api.model.generated.ConnectorBuilderProjectIdWithWorkspaceId;
import io.airbyte.api.model.generated.ConnectorBuilderProjectRead;
import io.airbyte.api.model.generated.ConnectorBuilderProjectReadList;
import io.airbyte.api.model.generated.ConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.api.model.generated.DeclarativeManifestRead;
import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.ConnectorBuilderProject;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * DestinationDefinitionsHandler. Javadocs suppressed because api docs should be used as source of
 * truth.
 */
@SuppressWarnings({"MissingJavadocMethod"})
@Singleton
public class ConnectorBuilderProjectsHandler {

  private final ConfigRepository configRepository;
  private final Supplier<UUID> uuidSupplier;

  @Inject
  public ConnectorBuilderProjectsHandler(final ConfigRepository configRepository,
                                         final Supplier<UUID> uuidSupplier) {
    this.configRepository = configRepository;
    this.uuidSupplier = uuidSupplier;
  }

  // This should be deleted when cloud is migrated to micronaut
  @Deprecated(forRemoval = true)
  public ConnectorBuilderProjectsHandler(final ConfigRepository configRepository) {
    this.configRepository = configRepository;
    this.uuidSupplier = UUID::randomUUID;
  }

  private ConnectorBuilderProject builderProjectFromUpdate(final ExistingConnectorBuilderProjectWithWorkspaceId projectCreate) {
    return new ConnectorBuilderProject().withBuilderProjectId(projectCreate.getBuilderProjectId()).withWorkspaceId(projectCreate.getWorkspaceId())
        .withName(projectCreate.getBuilderProject().getName())
        .withManifestDraft(projectCreate.getBuilderProject().getDraftManifest() == null ? null
            : new ObjectMapper().valueToTree(projectCreate.getBuilderProject().getDraftManifest()));
  }

  private static ConnectorBuilderProjectDetailsRead builderProjectToDetailsRead(final ConnectorBuilderProject project) {
    return new ConnectorBuilderProjectDetailsRead().name(project.getName()).builderProjectId(project.getBuilderProjectId())
        .hasDraft(project.getHasDraft());
  }

  private ConnectorBuilderProject builderProjectFromCreate(final ConnectorBuilderProjectWithWorkspaceId projectCreate) {
    final UUID id = uuidSupplier.get();

    return new ConnectorBuilderProject().withBuilderProjectId(id).withWorkspaceId(projectCreate.getWorkspaceId())
        .withName(projectCreate.getBuilderProject().getName())
        .withManifestDraft(new ObjectMapper().valueToTree(projectCreate.getBuilderProject().getDraftManifest()));
  }

  private ConnectorBuilderProjectIdWithWorkspaceId idResponseFromBuilderProject(final ConnectorBuilderProject project) {
    return new ConnectorBuilderProjectIdWithWorkspaceId().workspaceId(project.getWorkspaceId()).builderProjectId(project.getBuilderProjectId());
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
    final ConnectorBuilderProject project = builderProjectFromCreate(projectCreate);

    configRepository.writeBuilderProject(project);

    return idResponseFromBuilderProject(project);
  }

  public void updateConnectorBuilderProject(final ExistingConnectorBuilderProjectWithWorkspaceId projectUpdate)
      throws IOException, ConfigNotFoundException {
    validateWorkspace(projectUpdate.getBuilderProjectId(), projectUpdate.getWorkspaceId());

    final ConnectorBuilderProject project = builderProjectFromUpdate(projectUpdate);
    configRepository.writeBuilderProject(project);
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

}
