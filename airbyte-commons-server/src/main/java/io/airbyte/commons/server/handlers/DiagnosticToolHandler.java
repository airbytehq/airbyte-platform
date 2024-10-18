/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.api.model.generated.ActorType;
import io.airbyte.api.model.generated.LicenseInfoResponse;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
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

  private final WorkspaceService workspaceService;
  private final ConnectionService connectionService;
  private final SourceService sourceService;
  private final DestinationService destinationService;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final InstanceConfigurationHandler instanceConfigurationHandler;
  private final Optional<KubernetesClient> kubernetesClient;
  private final DumperOptions yamlDumperOptions;

  public DiagnosticToolHandler(final WorkspaceService workspaceService,
                               final ConnectionService connectionService,
                               final SourceService sourceService,
                               final DestinationService destinationService,
                               final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                               final InstanceConfigurationHandler instanceConfigurationHandler,
                               final Optional<KubernetesClient> kubernetesClient) {
    this.workspaceService = workspaceService;
    this.connectionService = connectionService;
    this.sourceService = sourceService;
    this.destinationService = destinationService;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
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
      return workspaceService
          .listStandardWorkspaces(false)
          .stream()
          .map(workspace -> Map.of(
              "name", workspace.getName(),
              "id", workspace.getWorkspaceId(),
              "connections", Objects.requireNonNull(collectConnectionInfo(workspace.getWorkspaceId())),
              "connectors", Objects.requireNonNull(collectConnectorInfo(workspace.getWorkspaceId()))))
          .toList();
    } catch (final IOException e) {
      LOGGER.error("Error collecting workspace information", e);
      return null;
    }
  }

  private List<Map<String, String>> collectConnectionInfo(final UUID workspaceId) {
    try {
      LOGGER.info("Collecting connections data...");
      // get all connections by workspaceId
      return connectionService
          .listWorkspaceStandardSyncs(workspaceId, false)
          .stream()
          .map(connection -> Map.of(
              "name", connection.getName(),
              "id", connection.getConnectionId().toString(),
              "status", connection.getStatus().toString(),
              "sourceId", connection.getSourceId().toString(),
              "destinationId", connection.getDestinationId().toString()))
          .toList();
    } catch (final IOException e) {
      LOGGER.error("Error collecting connection information", e);
      return Collections.emptyList();
    }
  }

  private List<Map<String, String>> collectConnectorInfo(final UUID workspaceId) {
    try {
      LOGGER.info("Collecting connectors data...");
      // get all sources by workspaceId (only include active ones in the report)
      final List<Map<String, String>> sources = sourceService
          .listWorkspaceSourceConnection(workspaceId)
          .stream()
          .filter(source -> {
            // TODO: isSourceActive feels like it could not throw and just return false if the config is not
            // found.
            try {
              return sourceService.isSourceActive(source.getSourceId());
            } catch (IOException e) {
              return false;
            }
          })
          .map(source -> {
            ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus sourceDefinitionVersion = null;
            try {
              sourceDefinitionVersion = actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(
                  sourceService.getSourceDefinitionFromSource(source.getSourceDefinitionId()),
                  workspaceId,
                  source.getSourceId());
            } catch (IOException | JsonValidationException | ConfigNotFoundException e) {
              LOGGER.error("Error collecting source version information", e);
            }

            return Map.of(
                "name", source.getName(),
                "id", source.getSourceId().toString(),
                "type", ActorType.SOURCE.toString(),
                "connectorDefinitionId", source.getSourceDefinitionId().toString(),
                "connectorDockerImageTag",
                sourceDefinitionVersion != null ? sourceDefinitionVersion.actorDefinitionVersion().getDockerImageTag() : "",
                "connectorVersionOverrideApplied", Boolean.toString(Objects.requireNonNull(sourceDefinitionVersion).isOverrideApplied()),
                "connectorSupportState", sourceDefinitionVersion.actorDefinitionVersion().getSupportState().toString());

          })
          .toList();

      // get all destinations by workspaceId (only include active ones in the report)
      final List<Map<String, String>> destinations = destinationService
          .listWorkspaceDestinationConnection(workspaceId)
          .stream()
          .filter(destination -> {
            // TODO: isDestinationActive feels like it could not throw and just return false if the config is
            // not found.
            try {
              return destinationService.isDestinationActive(destination.getDestinationId());
            } catch (IOException e) {
              return false;
            }
          })
          .map(destination -> {
            ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus destinationDefinitionVersion = null;
            try {
              destinationDefinitionVersion = actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(
                  destinationService.getStandardDestinationDefinition(destination.getDestinationId()),
                  workspaceId,
                  destination.getDestinationId());
            } catch (IOException | JsonValidationException | ConfigNotFoundException e) {
              LOGGER.error("Error collecting destination version information", e);
            }

            return Map.of(
                "name", destination.getName(),
                "id", destination.getDestinationId().toString(),
                "type", ActorType.DESTINATION.toString(),
                "connectorDefinitionId", destination.getDestinationId().toString(),
                "connectorDockerImageTag",
                destinationDefinitionVersion != null ? destinationDefinitionVersion.actorDefinitionVersion().getDockerImageTag() : "",
                "connectorVersionOverrideApplied", Boolean.toString(Objects.requireNonNull(destinationDefinitionVersion).isOverrideApplied()),
                "connectorSupportState", destinationDefinitionVersion.actorDefinitionVersion().getSupportState().toString());
          }).toList();
      // merge the two lists
      return Stream.concat(sources.stream(), destinations.stream()).toList();
    } catch (final IOException e) {
      LOGGER.error("Error collecting connectors information", e);
      return Collections.emptyList();
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
