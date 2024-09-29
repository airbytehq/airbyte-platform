/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.ActorDefinitionVersionRead;
import io.airbyte.api.model.generated.ActorStatus;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.ConnectionReadList;
import io.airbyte.api.model.generated.ConnectionStatus;
import io.airbyte.api.model.generated.DestinationRead;
import io.airbyte.api.model.generated.DestinationReadList;
import io.airbyte.api.model.generated.SourceRead;
import io.airbyte.api.model.generated.SourceReadList;
import io.airbyte.api.model.generated.SupportState;
import io.airbyte.api.model.generated.WorkspaceRead;
import io.airbyte.api.model.generated.WorkspaceReadList;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.validation.json.JsonValidationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * DiagnosticToolHandlerTest.
 */
@SuppressWarnings("PMD")
class DiagnosticToolHandlerTest {

  private DiagnosticToolHandler diagnosticToolHandler;
  private WorkspacesHandler workspacesHandler;
  private ConnectionsHandler connectionsHandler;
  private SourceHandler sourceHandler;
  private DestinationHandler destinationHandler;
  private ActorDefinitionVersionHandler actorDefinitionVersionHandler;

  @BeforeEach
  void beforeEach() throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException {
    workspacesHandler = mock(WorkspacesHandler.class);
    connectionsHandler = mock(ConnectionsHandler.class);
    sourceHandler = mock(SourceHandler.class);
    destinationHandler = mock(DestinationHandler.class);
    actorDefinitionVersionHandler = mock(ActorDefinitionVersionHandler.class);
    diagnosticToolHandler =
        new DiagnosticToolHandler(workspacesHandler, connectionsHandler, sourceHandler, destinationHandler, actorDefinitionVersionHandler);

    when(workspacesHandler.listWorkspaces()).thenReturn(new WorkspaceReadList().addWorkspacesItem(
        new WorkspaceRead()
            .name("workspace1")
            .workspaceId(UUID.randomUUID())));
    when(connectionsHandler.listConnectionsForWorkspace(any())).thenReturn(new ConnectionReadList().addConnectionsItem(
        new ConnectionRead()
            .connectionId(UUID.randomUUID())
            .name("connection1")
            .status(ConnectionStatus.ACTIVE)
            .sourceId(UUID.randomUUID())
            .destinationId(UUID.randomUUID())));
    when(sourceHandler.listSourcesForWorkspace(any())).thenReturn(new SourceReadList().addSourcesItem(
        new SourceRead()
            .sourceId(UUID.randomUUID())
            .name("source1")
            .sourceDefinitionId(UUID.randomUUID())
            .status(ActorStatus.ACTIVE)));
    when(destinationHandler.listDestinationsForWorkspace(any())).thenReturn(new DestinationReadList().addDestinationsItem(
        new DestinationRead()
            .destinationId(UUID.randomUUID())
            .name("destination1")
            .destinationDefinitionId(UUID.randomUUID())
            .status(ActorStatus.ACTIVE)));

    final ActorDefinitionVersionRead actorDefinitionVersion = new ActorDefinitionVersionRead()
        .dockerImageTag("tag")
        .isVersionOverrideApplied(true)
        .supportState(SupportState.SUPPORTED);

    when(actorDefinitionVersionHandler.getActorDefinitionVersionForSourceId(any())).thenReturn(actorDefinitionVersion);
    when(actorDefinitionVersionHandler.getActorDefinitionVersionForDestinationId(any())).thenReturn(actorDefinitionVersion);
  }

  @Test
  void testGenerateDiagnosticReport() throws IOException {
    final File zipFile = diagnosticToolHandler.generateDiagnosticReport();
    Assertions.assertTrue(zipFile.exists());
    // Check the content of the zip file
    try (FileInputStream fis = new FileInputStream(zipFile);
        ZipInputStream zis = new ZipInputStream(fis)) {

      ZipEntry entry;
      boolean foundApplicationYaml = false;
      boolean foundDeploymentYaml = false;

      // Iterate through the entries in the zip
      while ((entry = zis.getNextEntry()) != null) {
        if (entry.getName().equals(DiagnosticToolHandler.APPLICATION_YAML)) {
          foundApplicationYaml = true;

          // Check the content of application.yaml
          byte[] buffer = new byte[1024];
          int bytesRead;
          StringBuilder content = new StringBuilder();
          while ((bytesRead = zis.read(buffer)) != -1) {
            content.append(new String(buffer, 0, bytesRead));
          }
          Assertions.assertTrue(content.toString().contains("workspaces"));
          Assertions.assertTrue(content.toString().contains("connections"));
          Assertions.assertTrue(content.toString().contains("connectors"));
        } else if (entry.getName().equals(DiagnosticToolHandler.DEPLOYMENT_YAML)) {
          foundDeploymentYaml = true;

          // Check the content of deployment.yaml
          byte[] buffer = new byte[1024];
          int bytesRead;
          StringBuilder content = new StringBuilder();
          while ((bytesRead = zis.read(buffer)) != -1) {
            content.append(new String(buffer, 0, bytesRead));
          }
          Assertions.assertTrue(content.toString().contains("k8s"));
        }
      }

      // Ensure both files are present in the zip
      Assertions.assertTrue(foundApplicationYaml);
      Assertions.assertTrue(foundDeploymentYaml);
    }
  }

}
