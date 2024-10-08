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
import io.airbyte.api.model.generated.LicenseInfoResponse;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.api.model.generated.SourceReadList;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.api.model.generated.WorkspaceReadList;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.validation.json.JsonValidationException;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeCondition;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.inject.Singleton;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  public static final String AIRBYTE_INSTANCE_YAML = "airbyte_instance.yaml";
  public static final String AIRBYTE_DEPLOYMENT_YAML = "airbyte_deployment.yaml";
  public static final String DIAGNOSTIC_REPORT_FILE_NAME = "diagnostic_report";
  public static final String DIAGNOSTIC_REPORT_FILE_FORMAT = ".zip";

  private final WorkspacesHandler workspacesHandler;
  private final ConnectionsHandler connectionsHandler;
  private final SourceHandler sourceHandler;
  private final DestinationHandler destinationHandler;
  private final ActorDefinitionVersionHandler actorDefinitionVersionHandler;
  private final InstanceConfigurationHandler instanceConfigurationHandler;
  private final Optional<KubernetesClient> kubernetesClient;
  private final DumperOptions yamlDumperOptions;

  public DiagnosticToolHandler(final WorkspacesHandler workspacesHandler,
                               final ConnectionsHandler connectionsHandler,
                               final SourceHandler sourceHandler,
                               final DestinationHandler destinationHandler,
                               final ActorDefinitionVersionHandler actorDefinitionVersionHandler,
                               final InstanceConfigurationHandler instanceConfigurationHandler,
                               final Optional<KubernetesClient> kubernetesClient) {
    this.workspacesHandler = workspacesHandler;
    this.connectionsHandler = connectionsHandler;
    this.sourceHandler = sourceHandler;
    this.destinationHandler = destinationHandler;
    this.actorDefinitionVersionHandler = actorDefinitionVersionHandler;
    this.instanceConfigurationHandler = instanceConfigurationHandler;
    this.kubernetesClient = kubernetesClient;
    this.yamlDumperOptions = new DumperOptions();
    this.yamlDumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
  }

  /**
   * Generate diagnostic report by collecting relevant data and zipping them into a single file.
   *
   * @return File - generated zip file as a diagnostic report
   */
  public File generateDiagnosticReport() {
    try {
      // Generate the zip file content in memory as byte[]
      final byte[] zipFileContent = generateZipInMemory();

      // Write the byte[] to a temporary file
      final File tempFile = File.createTempFile(DIAGNOSTIC_REPORT_FILE_NAME, DIAGNOSTIC_REPORT_FILE_FORMAT);
      try (final FileOutputStream fos = new FileOutputStream(tempFile)) {
        fos.write(zipFileContent);
      }

      // Return the temporary file
      return tempFile;

    } catch (final IOException e) {
      LOGGER.error("Error generating diagnostic report", e);
      return null;
    }
  }

  private byte[] generateZipInMemory() throws IOException {
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final ZipOutputStream zipOut = new ZipOutputStream(byteArrayOutputStream);
    try {
      addAirbyteInstanceYaml(zipOut);
    } catch (final IOException | JsonValidationException e) {
      LOGGER.error("Error in writing airbyte instance yaml.", e);
    }
    try {
      addAirbyteDeploymentYaml(zipOut);
    } catch (final IOException e) {
      LOGGER.error("Error in writing deployment yaml.", e);
    }
    zipOut.finish();
    return byteArrayOutputStream.toByteArray();
  }

  private void addAirbyteInstanceYaml(final ZipOutputStream zipOut) throws IOException, JsonValidationException {
    // In-memory construct an entry (yaml file) in the final zip output.
    final ZipEntry airbyteInstanceYaml = new ZipEntry(AIRBYTE_INSTANCE_YAML);
    zipOut.putNextEntry(airbyteInstanceYaml);

    // Write instance information to the zip entry
    final String airbyteInstanceYamlContent = generateAirbyteInstanceYaml();
    zipOut.write(airbyteInstanceYamlContent.getBytes());
    zipOut.closeEntry();
  }

  private String generateAirbyteInstanceYaml() {
    final Map<String, Object> airbyteInstanceYamlData = new HashMap<>();
    // Collect workspace information
    airbyteInstanceYamlData.put("workspaces", collectWorkspaceInfo());
    // Collect license information
    airbyteInstanceYamlData.put("license", collectLicenseInfo());
    // TODO: Collect other information here, e.g: application logs, etc.
    final Yaml yaml = new Yaml(yamlDumperOptions);
    return yaml.dump(airbyteInstanceYamlData);
  }

  private List<Map<String, Object>> collectWorkspaceInfo() {
    try {
      // get all workspaces
      LOGGER.info("Collecting workspaces data...");
      final WorkspaceReadList workspaces = workspacesHandler.listWorkspaces();
      final List<Map<String, Object>> workspaceList = workspaces.getWorkspaces().stream().map(workspace -> {
        final Map<String, Object> workspaceInfo = new HashMap<>();
        workspaceInfo.put("name", workspace.getName());
        workspaceInfo.put("id", workspace.getWorkspaceId().toString());
        workspaceInfo.put("connections", collectConnectionInfo(workspace.getWorkspaceId()));
        workspaceInfo.put("connectors", collectConnectorInfo(workspace.getWorkspaceId()));
        return workspaceInfo;
      }).toList();
      return workspaceList;
    } catch (final JsonValidationException | IOException e) {
      LOGGER.error("Error collecting workspace information", e);
      return null;
    }
  }

  private List<Map<String, Object>> collectConnectionInfo(final UUID workspaceId) {
    try {
      LOGGER.info("Collecting connections data...");
      // get all connections by workspaceId
      final ConnectionReadList connections = connectionsHandler.listConnectionsForWorkspace(new WorkspaceIdRequestBody().workspaceId(workspaceId));
      final List<Map<String, Object>> connectionList = connections.getConnections().stream().map(connection -> {
        final Map<String, Object> connectionInfo = new HashMap<>();
        connectionInfo.put("name", connection.getName());
        connectionInfo.put("id", connection.getConnectionId().toString());
        connectionInfo.put("status", connection.getStatus().toString());
        connectionInfo.put("sourceId", connection.getSourceId().toString());
        connectionInfo.put("destinationId", connection.getDestinationId().toString());
        return connectionInfo;
      }).toList();
      return connectionList;
    } catch (final JsonValidationException | ConfigNotFoundException | IOException e) {
      LOGGER.error("Error collecting connection information", e);
      return null;
    }
  }

  private List<Map<String, Object>> collectConnectorInfo(final UUID workspaceId) {
    try {
      LOGGER.info("Collecting connectors data...");
      // get all sources by workspaceId (only include active ones in the report)
      final SourceReadList sources = sourceHandler.listSourcesForWorkspace(new WorkspaceIdRequestBody().workspaceId(workspaceId));
      final List<Map<String, Object>> sourceList = sources.getSources().stream().filter(source -> ActorStatus.ACTIVE.equals(source.getStatus()))
          .map(source -> {
            final Map<String, Object> connectionInfo = new HashMap<>();
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
            } catch (final JsonValidationException | IOException | ConfigNotFoundException
                | io.airbyte.config.persistence.ConfigNotFoundException e) {
              LOGGER.error("Error collecting source version information", e);
            }
            return connectionInfo;
          }).toList();

      // get all destinations by workspaceId (only include active ones in the report)
      final DestinationReadList destinations = destinationHandler.listDestinationsForWorkspace(new WorkspaceIdRequestBody().workspaceId(workspaceId));
      final List<Map<String, Object>> destinationList =
          destinations.getDestinations().stream().filter(destination -> ActorStatus.ACTIVE.equals(destination.getStatus()))
              .map(destination -> {
                final Map<String, Object> connectionInfo = new HashMap<>();
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
                } catch (final JsonValidationException | IOException | ConfigNotFoundException
                    | io.airbyte.config.persistence.ConfigNotFoundException e) {
                  LOGGER.error("Error collecting destination version information", e);
                }
                return connectionInfo;
              }).toList();
      // merge the two lists
      final List<Map<String, Object>> allConnectors = new ArrayList<>(sourceList);
      allConnectors.addAll(destinationList);
      return allConnectors;
    } catch (final JsonValidationException | IOException | io.airbyte.data.exceptions.ConfigNotFoundException e) {
      LOGGER.error("Error collecting connectors information", e);
      return null;
    }
  }

  private Map<String, Object> collectLicenseInfo() {
    LOGGER.info("Collecting license data...");
    final LicenseInfoResponse license = instanceConfigurationHandler.licenseInfo();
    if (license == null) {
      LOGGER.error("Error collecting license information");
      return null;
    }
    final Map<String, Object> licenseInfo = new HashMap<>();
    licenseInfo.put("edition", license.getEdition());
    licenseInfo.put("status", license.getLicenseStatus().toString());
    licenseInfo.put("expiryDate", Instant.ofEpochSecond(license.getExpirationDate()).toString());
    licenseInfo.put("maxEditors", license.getMaxEditors());
    licenseInfo.put("maxNodes", license.getMaxNodes());
    licenseInfo.put("usedEditors", license.getUsedEditors());
    licenseInfo.put("usedNodes", license.getUsedNodes());
    return licenseInfo;
  }

  private void addAirbyteDeploymentYaml(final ZipOutputStream zipOut) throws IOException {
    final ZipEntry zipEntry = new ZipEntry(AIRBYTE_DEPLOYMENT_YAML);
    zipOut.putNextEntry(zipEntry);
    final String deploymentYamlContent = generateDeploymentYaml();
    zipOut.write(deploymentYamlContent.getBytes());
    zipOut.closeEntry();
  }

  private String generateDeploymentYaml() {
    final Map<String, Object> deploymentYamlData = new HashMap<>();
    // Collect cluster information
    deploymentYamlData.put("k8s", collectK8sInfo());
    final Yaml yaml = new Yaml(yamlDumperOptions);
    return yaml.dump(deploymentYamlData);
  }

  private Map<String, Object> collectK8sInfo() {
    if (kubernetesClient.isEmpty()) {
      LOGGER.error("Kubernetes client not available...");
      return null;
    }
    LOGGER.info("Collecting k8s data...");
    final KubernetesClient client = kubernetesClient.get();
    final Map<String, Object> kubernetesInfo = new HashMap<>();
    kubernetesInfo.put("nodes", collectNodeInfo(client));
    kubernetesInfo.put("pods", collectPodInfo(client));
    return kubernetesInfo;
  }

  private List<Map<String, Object>> collectNodeInfo(final KubernetesClient client) {
    LOGGER.info("Collecting nodes data...");
    final List<Map<String, Object>> nodeList = new ArrayList<>();
    final List<Node> nodes = client.nodes().list().getItems();
    for (final Node node : nodes) {
      final Map<String, Object> nodeInfo = new HashMap<>();
      nodeInfo.put("name", node.getMetadata().getName());
      nodeInfo.put("readyStatus", node.getStatus().getConditions().stream()
          .filter(nodeCondition -> "Ready".equals(nodeCondition.getType()))
          .map(NodeCondition::getStatus)
          .findFirst()
          .orElse("Unknown"));
      nodeInfo.put("cpu", node.getStatus().getAllocatable().get("cpu").toString());
      nodeInfo.put("memory", node.getStatus().getAllocatable().get("memory").toString());
      nodeList.add(nodeInfo);
    }
    return nodeList;
  }

  private List<Map<String, Object>> collectPodInfo(final KubernetesClient client) {
    LOGGER.info("Collecting pods data...");
    final List<Map<String, Object>> podList = new ArrayList<>();
    final List<Pod> pods = client.pods().inNamespace("ab").list().getItems();
    for (final Pod pod : pods) {
      final Map<String, Object> podInfo = new HashMap<>();
      podInfo.put("name", pod.getMetadata().getName());
      podInfo.put("status", pod.getStatus().getPhase());
      final List<Map<String, Object>> containerLimits = new ArrayList<>();
      pod.getSpec().getContainers().forEach(container -> {
        final Map<String, Object> containerLimit = new HashMap<>();
        containerLimit.put("containerName", container.getName());
        final Map<String, Quantity> limit = getContainerResourceLimit(container);
        containerLimit.put("cpu", limit != null ? limit.get("cpu") : null);
        containerLimit.put("memory", limit != null ? limit.get("memory") : null);
        containerLimits.add(containerLimit);
      });
      podInfo.put("limits", containerLimits);
      podList.add(podInfo);
    }
    return podList;
  }

  private Map<String, Quantity> getContainerResourceLimit(final Container container) {
    if (container.getResources() == null || container.getResources().getLimits() == null) {
      return null;
    }
    return container.getResources().getLimits();
  }

}
