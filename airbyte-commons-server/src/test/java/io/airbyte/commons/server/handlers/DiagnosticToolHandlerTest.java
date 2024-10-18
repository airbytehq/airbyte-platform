/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.LicenseInfoResponse;
import io.airbyte.api.model.generated.LicenseStatus;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.validation.json.JsonValidationException;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeCondition;
import io.fabric8.kubernetes.api.model.NodeList;
import io.fabric8.kubernetes.api.model.NodeStatus;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * DiagnosticToolHandlerTest.
 */
@SuppressWarnings("PMD")
@ExtendWith(MockitoExtension.class)
class DiagnosticToolHandlerTest {

  private DiagnosticToolHandler diagnosticToolHandler;
  private WorkspaceService workspaceService;
  private ConnectionService connectionService;
  private SourceService sourceService;
  private DestinationService destinationService;
  private InstanceConfigurationHandler instanceConfigurationHandler;
  private ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private KubernetesClient kubernetesClient;

  @BeforeEach
  void beforeEach() throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException {
    workspaceService = mock(WorkspaceService.class);
    connectionService = mock(ConnectionService.class);
    sourceService = mock(SourceService.class);
    destinationService = mock(DestinationService.class);
    actorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);
    instanceConfigurationHandler = mock(InstanceConfigurationHandler.class);
    kubernetesClient = mock(KubernetesClient.class);
    diagnosticToolHandler = new DiagnosticToolHandler(
        workspaceService,
        connectionService,
        sourceService,
        destinationService,
        actorDefinitionVersionHelper,
        instanceConfigurationHandler,
        Optional.of(kubernetesClient));

    // Mock workspace API responses
    final var workspace = getStandardWorkspace();
    when(workspaceService.listStandardWorkspaces(false)).thenReturn(List.of(workspace));
    when(connectionService.listWorkspaceStandardSyncs(workspace.getWorkspaceId(), false)).thenReturn(List.of(getStandardSync()));
    when(sourceService.listWorkspaceSourceConnection(any())).thenReturn(List.of(getSource()));
    when(sourceService.isSourceActive(any())).thenReturn(true);
    when(destinationService.listWorkspaceDestinationConnection(any())).thenReturn(List.of(getDestination()));
    when(destinationService.isDestinationActive(any())).thenReturn(true);
    when(actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(any(), any(), any())).thenReturn(getActorDefinitionVersion());
    when(actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(any(), any(), any())).thenReturn(getActorDefinitionVersion());

    // Mock license API responses
    when(instanceConfigurationHandler.licenseInfo()).thenReturn(new LicenseInfoResponse()
        .licenseStatus(LicenseStatus.PRO)
        .expirationDate(OffsetDateTime.now().plusDays(10).toEpochSecond())
        .usedNodes(2));
    // Mock k8s responses
    final Node node1 = new Node();
    final ObjectMeta metadata = new ObjectMeta();
    metadata.setName("node1");
    node1.setMetadata(metadata);
    final NodeStatus status = new NodeStatus();
    final NodeCondition condition = new NodeCondition();
    condition.setType("Ready");
    condition.setStatus("true");
    final List<NodeCondition> conditions = List.of(condition);
    status.setConditions(conditions);
    final Map<String, Quantity> allocatable = new HashMap<>();
    allocatable.put("cpu", new Quantity("500m"));
    allocatable.put("memory", new Quantity("1Gi"));
    status.setAllocatable(allocatable);
    node1.setStatus(status);
    final NodeList nodeList = new NodeList();
    nodeList.setItems(List.of(node1));
    final NonNamespaceOperation op = mock(NonNamespaceOperation.class);
    when(kubernetesClient.nodes()).thenReturn(op);
    when(op.list()).thenReturn(nodeList);

    final Pod pod1 = new Pod();
    final ObjectMeta podMetadata = new ObjectMeta();
    podMetadata.setName("pod1");
    pod1.setMetadata(podMetadata);
    final PodStatus podStatus = new PodStatus();
    podStatus.setPhase("Running");
    pod1.setStatus(podStatus);

    final PodSpec podSpec = new PodSpec();
    final Container container = new Container();
    container.setName("containerName");
    podSpec.setContainers(List.of(container));
    final ResourceRequirements requirements = new ResourceRequirements();
    final Map<String, Quantity> limits = new HashMap<>();
    limits.put("cpu", new Quantity("500m"));
    limits.put("memory", new Quantity("1Gi"));
    requirements.setLimits(limits);
    container.setResources(requirements);
    podSpec.setContainers(List.of(container));
    pod1.setSpec(podSpec);

    final PodList podList = new PodList();
    podList.setItems(List.of(pod1));

    final MixedOperation<Pod, PodList, PodResource> mop = mock(MixedOperation.class);
    final NonNamespaceOperation<Pod, PodList, PodResource> podNamespaceOperation = mock(NonNamespaceOperation.class);
    when(kubernetesClient.pods()).thenReturn(mop);
    when(mop.inNamespace("ab")).thenReturn(podNamespaceOperation);
    when(podNamespaceOperation.list()).thenReturn(podList);

  }

  @Test
  void testGenerateDiagnosticReport() throws IOException {
    final File zipFile = diagnosticToolHandler.generateDiagnosticReport();
    Assertions.assertTrue(zipFile.exists());
    // Check the content of the zip file
    try (final FileInputStream fis = new FileInputStream(zipFile);
        final ZipInputStream zis = new ZipInputStream(fis)) {

      ZipEntry entry;
      boolean foundInstanceYaml = false;
      boolean foundDeploymentYaml = false;

      // Iterate through the entries in the zip
      while ((entry = zis.getNextEntry()) != null) {
        if (entry.getName().equals(DiagnosticToolHandler.AIRBYTE_INSTANCE_YAML)) {
          foundInstanceYaml = true;

          // Check the content of airbyte_instance.yaml
          final byte[] buffer = new byte[1024];
          int bytesRead;
          final StringBuilder content = new StringBuilder();
          while ((bytesRead = zis.read(buffer)) != -1) {
            content.append(new String(buffer, 0, bytesRead));
          }
          // workspace information
          Assertions.assertTrue(content.toString().contains("workspaces"));
          Assertions.assertTrue(content.toString().contains("connections"));
          Assertions.assertTrue(content.toString().contains("connectors"));
          // license information
          Assertions.assertTrue(content.toString().contains("license"));
          Assertions.assertTrue(content.toString().contains("expiryDate"));
          Assertions.assertTrue(content.toString().contains("usedNodes"));
        } else if (entry.getName().equals(DiagnosticToolHandler.AIRBYTE_DEPLOYMENT_YAML)) {
          foundDeploymentYaml = true;

          // Check the content of airbyte_deployment.yaml
          final byte[] buffer = new byte[1024];
          int bytesRead;
          final StringBuilder content = new StringBuilder();
          while ((bytesRead = zis.read(buffer)) != -1) {
            content.append(new String(buffer, 0, bytesRead));
          }
          // k8s information
          Assertions.assertTrue(content.toString().contains("k8s"));
          Assertions.assertTrue(content.toString().contains("nodes"));
          Assertions.assertTrue(content.toString().contains("pods"));
        }
      }

      // Ensure all yaml files are present in the zip
      Assertions.assertTrue(foundInstanceYaml);
      Assertions.assertTrue(foundDeploymentYaml);
    }
  }

  private static @NotNull StandardWorkspace getStandardWorkspace() {
    return new StandardWorkspace()
        .withName("workspace1")
        .withWorkspaceId(UUID.randomUUID());
  }

  private static StandardSync getStandardSync() {
    return new StandardSync()
        .withName("connection1")
        .withStatus(StandardSync.Status.ACTIVE)
        .withConnectionId(UUID.randomUUID())
        .withSourceId(UUID.randomUUID())
        .withDestinationId(UUID.randomUUID());
  }

  private static SourceConnection getSource() {
    return new SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withName("source")
        .withSourceDefinitionId(UUID.randomUUID());
  }

  private static ActorDefinitionVersionHelper.@NotNull ActorDefinitionVersionWithOverrideStatus getActorDefinitionVersion() {
    return new ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus(
        new ActorDefinitionVersion().withDockerImageTag("tag").withSupportState(ActorDefinitionVersion.SupportState.SUPPORTED), true);
  }

  private static DestinationConnection getDestination() {
    return new DestinationConnection().withDestinationId(UUID.randomUUID()).withName("destination1").withDestinationDefinitionId(UUID.randomUUID());
  }

}
