/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.api.model.generated.ActorDefinitionVersionRead;
import io.airbyte.api.model.generated.ActorStatus;
import io.airbyte.api.model.generated.ActorType;
import io.airbyte.api.model.generated.ConnectionReadList;
import io.airbyte.api.model.generated.DestinationIdRequestBody;
import io.airbyte.api.model.generated.DestinationReadList;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.api.model.generated.SourceReadList;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.api.model.generated.WorkspaceReadList;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Singleton;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

/**
 * DiagnosticToolHandler.
 */
@Singleton
@SuppressWarnings({"PMD.AvoidDuplicateLiterals"})
public class DiagnosticToolHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobInputHandler.class);
  public static final String APPLICATION_YAML = "application.yaml";
  public static final String DEPLOYMENT_YAML = "deployment.yaml";
  public static final String DIAGNOSTIC_REPORT_FILE_NAME = "diagnostic_report";
  public static final String DIAGNOSTIC_REPORT_FILE_FORMAT = ".zip";

  private final WorkspacesHandler workspacesHandler;
  private final ConnectionsHandler connectionsHandler;
  private final SourceHandler sourceHandler;
  private final DestinationHandler destinationHandler;
  private final ActorDefinitionVersionHandler actorDefinitionVersionHandler;

  public DiagnosticToolHandler(WorkspacesHandler workspacesHandler,
                               ConnectionsHandler connectionsHandler,
                               SourceHandler sourceHandler,
                               DestinationHandler destinationHandler,
                               ActorDefinitionVersionHandler actorDefinitionVersionHandler) {
    this.workspacesHandler = workspacesHandler;
    this.connectionsHandler = connectionsHandler;
    this.sourceHandler = sourceHandler;
    this.destinationHandler = destinationHandler;
    this.actorDefinitionVersionHandler = actorDefinitionVersionHandler;

  }

  /**
   * Generate diagnostic report by collecting relevant data and zipping them into a single file.
   *
   * @return File - generated zip file as a diagnostic report
   */
  public File generateDiagnosticReport() {
    try {
      // Generate the zip file content in memory as byte[]
      byte[] zipFileContent = generateZipInMemory();

      // Write the byte[] to a temporary file
      File tempFile = File.createTempFile(DIAGNOSTIC_REPORT_FILE_NAME, DIAGNOSTIC_REPORT_FILE_FORMAT);
      try (FileOutputStream fos = new FileOutputStream(tempFile)) {
        fos.write(zipFileContent);
      }

      // Return the temporary file
      return tempFile;

    } catch (IOException e) {
      LOGGER.error("Error generating diagnostic report", e);
      return null;
    }
  }

  private byte[] generateZipInMemory() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ZipOutputStream zipOut = new ZipOutputStream(byteArrayOutputStream);
    try {
      addApplicationYaml(zipOut);
    } catch (IOException | JsonValidationException e) {
      LOGGER.error("Error in writing application data.", e);
    }
    try {
      addDeploymentYaml(zipOut);
    } catch (IOException e) {
      LOGGER.error("Error in writing deployment data.", e);
    }
    zipOut.finish();
    return byteArrayOutputStream.toByteArray();
  }

  private void addApplicationYaml(ZipOutputStream zipOut) throws IOException, JsonValidationException {

    // In-memory construct an entry (application.yaml file) in the final zip output.
    ZipEntry applicationYaml = new ZipEntry(APPLICATION_YAML);
    zipOut.putNextEntry(applicationYaml);

    // Write application information to the zip entry: application.yaml.
    String applicationYamlContent = generateApplicationYaml();
    zipOut.write(applicationYamlContent.getBytes());
    zipOut.closeEntry();
  }

  private String generateApplicationYaml() {
    Map<String, Object> applicationYamlData = new HashMap<>();
    // Collect workspace information and write it to `application.yaml`.
    applicationYamlData.put("workspaces", collectWorkspaceInfo());
    // Collect other information here, e.g. application logs, etc.
    DumperOptions options = new DumperOptions();
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
    Yaml yaml = new Yaml(options);
    return yaml.dump(applicationYamlData);
  }

  private List<Map<String, Object>> collectWorkspaceInfo() {
    try {
      // get all workspaces
      final WorkspaceReadList workspaces = workspacesHandler.listWorkspaces();
      List<Map<String, Object>> workspaceList = workspaces.getWorkspaces().stream().map(workspace -> {
        Map<String, Object> workspaceInfo = new HashMap<>();
        workspaceInfo.put("name", workspace.getName());
        workspaceInfo.put("id", workspace.getWorkspaceId().toString());
        workspaceInfo.put("connections", collectConnectionInfo(workspace.getWorkspaceId()));
        workspaceInfo.put("connectors", collectConnectorInfo(workspace.getWorkspaceId()));
        return workspaceInfo;
      }).toList();
      return workspaceList;
    } catch (JsonValidationException | IOException e) {
      LOGGER.error("Error collecting workspace information", e);
      return null;
    }
  }

  private List<Map<String, Object>> collectConnectionInfo(final UUID workspaceId) {
    try {
      // get all connections by workspaceId
      final ConnectionReadList connections = connectionsHandler.listConnectionsForWorkspace(new WorkspaceIdRequestBody().workspaceId(workspaceId));
      List<Map<String, Object>> connectionList = connections.getConnections().stream().map(connection -> {
        Map<String, Object> connectionInfo = new HashMap<>();
        connectionInfo.put("name", connection.getName());
        connectionInfo.put("id", connection.getConnectionId().toString());
        connectionInfo.put("status", connection.getStatus().toString());
        connectionInfo.put("sourceId", connection.getSourceId().toString());
        connectionInfo.put("destinationId", connection.getDestinationId().toString());
        return connectionInfo;
      }).toList();
      return connectionList;
    } catch (JsonValidationException | ConfigNotFoundException | IOException e) {
      LOGGER.error("Error collecting connection information", e);
      return null;
    }
  }

  private List<Map<String, Object>> collectConnectorInfo(final UUID workspaceId) {
    try {
      // get all sources by workspaceId (only include active ones in the report)
      final SourceReadList sources = sourceHandler.listSourcesForWorkspace(new WorkspaceIdRequestBody().workspaceId(workspaceId));
      List<Map<String, Object>> sourceList = sources.getSources().stream().filter(source -> ActorStatus.ACTIVE.equals(source.getStatus()))
          .map(source -> {
            Map<String, Object> connectionInfo = new HashMap<>();
            connectionInfo.put("name", source.getName());
            connectionInfo.put("id", source.getSourceId().toString());
            connectionInfo.put("type", ActorType.SOURCE.toString());
            connectionInfo.put("connectorDefinitionId", source.getSourceDefinitionId().toString());
            try {
              final ActorDefinitionVersionRead sourceDefinitionVersion =
                  actorDefinitionVersionHandler.getActorDefinitionVersionForSourceId(new SourceIdRequestBody().sourceId(source.getSourceId()));
              connectionInfo.put("connectorDockerImageTag", sourceDefinitionVersion.getDockerImageTag());
              connectionInfo.put("connectorVersionOverrideApplied", sourceDefinitionVersion.getIsVersionOverrideApplied());
              connectionInfo.put("connectorSupportState", sourceDefinitionVersion.getSupportState().toString());
            } catch (JsonValidationException | IOException | io.airbyte.data.exceptions.ConfigNotFoundException | ConfigNotFoundException e) {
              LOGGER.error("Error collecting source version information", e);
            }
            return connectionInfo;
          }).toList();

      // get all destinations by workspaceId (only include active ones in the report)
      final DestinationReadList destinations = destinationHandler.listDestinationsForWorkspace(new WorkspaceIdRequestBody().workspaceId(workspaceId));
      List<Map<String, Object>> destinationList =
          destinations.getDestinations().stream().filter(destination -> ActorStatus.ACTIVE.equals(destination.getStatus()))
              .map(destination -> {
                Map<String, Object> connectionInfo = new HashMap<>();
                connectionInfo.put("name", destination.getName());
                connectionInfo.put("id", destination.getDestinationId().toString());
                connectionInfo.put("type", ActorType.DESTINATION.toString());
                connectionInfo.put("connectorDefinitionId", destination.getDestinationId().toString());
                try {
                  final ActorDefinitionVersionRead destinationDefinitionVersion = actorDefinitionVersionHandler
                      .getActorDefinitionVersionForDestinationId(new DestinationIdRequestBody().destinationId(destination.getDestinationId()));
                  connectionInfo.put("connectorDockerImageTag", destinationDefinitionVersion.getDockerImageTag());
                  connectionInfo.put("connectorVersionOverrideApplied", destinationDefinitionVersion.getIsVersionOverrideApplied());
                  connectionInfo.put("connectorSupportState", destinationDefinitionVersion.getSupportState().toString());
                } catch (JsonValidationException | IOException | io.airbyte.data.exceptions.ConfigNotFoundException | ConfigNotFoundException e) {
                  LOGGER.error("Error collecting destination version information", e);
                }
                return connectionInfo;
              }).toList();
      // merge the two lists
      List<Map<String, Object>> allConnectors = new ArrayList<>(sourceList);
      allConnectors.addAll(destinationList);
      return allConnectors;
    } catch (JsonValidationException | ConfigNotFoundException | IOException e) {
      LOGGER.error("Error collecting connectors information", e);
      return null;
    }
  }

  private void addDeploymentYaml(ZipOutputStream zipOut) throws IOException {
    // In-memory construct an entry (deployment.yaml file) in the final zip output.
    ZipEntry zipEntry = new ZipEntry(DEPLOYMENT_YAML);
    zipOut.putNextEntry(zipEntry);
    String deploymentYamlContent = generateDeploymentYaml();
    zipOut.write(deploymentYamlContent.getBytes());
    zipOut.closeEntry();
  }

  /**
   * Collect Kubernetes information and write it to deployment_info.yaml.
   */
  private String generateDeploymentYaml() {
    LOGGER.info("Collecting k8s data...");
    // TODO: implement this
    return "k8s: \n";
  }

}
