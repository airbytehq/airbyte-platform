/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.LicenseInfoResponse
import io.airbyte.api.model.generated.LicenseStatus
import io.airbyte.commons.csp.CheckResult
import io.airbyte.commons.csp.CspChecker
import io.airbyte.commons.csp.Storage
import io.airbyte.commons.server.helpers.KubernetesClientPermissionHelper
import io.airbyte.commons.storage.StorageType
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.DestinationConnection
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.fabric8.kubernetes.api.model.Container
import io.fabric8.kubernetes.api.model.Node
import io.fabric8.kubernetes.api.model.NodeCondition
import io.fabric8.kubernetes.api.model.NodeList
import io.fabric8.kubernetes.api.model.NodeStatus
import io.fabric8.kubernetes.api.model.ObjectMeta
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.PodList
import io.fabric8.kubernetes.api.model.PodSpec
import io.fabric8.kubernetes.api.model.PodStatus
import io.fabric8.kubernetes.api.model.Quantity
import io.fabric8.kubernetes.api.model.ResourceRequirements
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation
import io.fabric8.kubernetes.client.dsl.Resource
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.io.File
import java.io.FileInputStream
import java.time.OffsetDateTime
import java.util.UUID
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream

/**
 * DiagnosticToolHandlerTest.
 */
internal class DiagnosticToolHandlerTest {
  @Test
  fun testGenerateDiagnosticReport() {
    val diagnosticToolHandler = mockDiagnosticToolHandler(true)
    val zipFile: File? = diagnosticToolHandler.generateDiagnosticReport()
    Assertions.assertTrue(zipFile!!.exists())
    FileInputStream(zipFile).use { fis ->
      ZipInputStream(fis).use { zis ->
        var entry: ZipEntry?
        var foundInstanceYaml: Boolean = false
        var foundDeploymentYaml: Boolean = false
        var foundCspChecksYaml: Boolean = false

        // Iterate through the entries in the zip
        while ((zis.nextEntry.also { entry = it }) != null) {
          if (entry!!.name == AIRBYTE_INSTANCE_YAML) {
            foundInstanceYaml = true

            // Check the content of airbyte_instance.yaml
            val buffer: ByteArray = ByteArray(1024)
            var bytesRead: Int
            val content: StringBuilder = StringBuilder()
            while ((zis.read(buffer).also { bytesRead = it }) != -1) {
              content.append(String(buffer, 0, bytesRead))
            }
            // workspace information
            Assertions.assertTrue(content.toString().contains("workspaces"))
            Assertions.assertTrue(content.toString().contains("connections"))
            Assertions.assertTrue(content.toString().contains("connectors"))
            // license information
            Assertions.assertTrue(content.toString().contains("license"))
            Assertions.assertTrue(content.toString().contains("expiryDate"))
            Assertions.assertTrue(content.toString().contains("usedNodes"))
          } else if (entry!!.name == AIRBYTE_DEPLOYMENT_YAML) {
            foundDeploymentYaml = true

            // Check the content of airbyte_deployment.yaml
            val buffer: ByteArray = ByteArray(1024)
            var bytesRead: Int
            val content: StringBuilder = StringBuilder()
            while ((zis.read(buffer).also { bytesRead = it }) != -1) {
              content.append(String(buffer, 0, bytesRead))
            }
            // k8s information
            Assertions.assertTrue(content.toString().contains("k8s"))
            Assertions.assertTrue(content.toString().contains("nodes"))
            Assertions.assertTrue(content.toString().contains("pods"))
          } else if (entry!!.name == AIRBYTE_CSP_CHECKS) {
            foundCspChecksYaml = true
          }
        }

        // Ensure all yaml files are present in the zip
        Assertions.assertTrue(foundInstanceYaml)
        Assertions.assertTrue(foundDeploymentYaml)
        Assertions.assertTrue(foundCspChecksYaml)
      }
    }
  }

  @Test
  fun testGenerateDiagnosticReportWithoutNodeViewerPermissionContainsNoDeploymnetYaml() {
    val diagnosticToolHandler = mockDiagnosticToolHandler(false)
    val zipFile: File? = diagnosticToolHandler.generateDiagnosticReport()
    Assertions.assertTrue(zipFile!!.exists())
    FileInputStream(zipFile).use { fis ->
      ZipInputStream(fis).use { zis ->
        var entry: ZipEntry?
        var foundInstanceYaml: Boolean = false
        var foundDeploymentYaml: Boolean = false
        var foundCspChecksYaml: Boolean = false

        // Iterate through the entries in the zip
        while ((zis.nextEntry.also { entry = it }) != null) {
          if (entry!!.name == AIRBYTE_INSTANCE_YAML) {
            foundInstanceYaml = true

            // Check the content of airbyte_instance.yaml
            val buffer: ByteArray = ByteArray(1024)
            var bytesRead: Int
            val content: StringBuilder = StringBuilder()
            while ((zis.read(buffer).also { bytesRead = it }) != -1) {
              content.append(String(buffer, 0, bytesRead))
            }
            // workspace information
            Assertions.assertTrue(content.toString().contains("workspaces"))
            Assertions.assertTrue(content.toString().contains("connections"))
            Assertions.assertTrue(content.toString().contains("connectors"))
            // license information
            Assertions.assertTrue(content.toString().contains("license"))
            Assertions.assertTrue(content.toString().contains("expiryDate"))
            Assertions.assertTrue(content.toString().contains("usedNodes"))
          } else if (entry!!.name == AIRBYTE_DEPLOYMENT_YAML) {
            foundDeploymentYaml = true

            // Check the content of airbyte_deployment.yaml
            val buffer: ByteArray = ByteArray(1024)
            var bytesRead: Int
            val content: StringBuilder = StringBuilder()
            while ((zis.read(buffer).also { bytesRead = it }) != -1) {
              content.append(String(buffer, 0, bytesRead))
            }
            // k8s information
            Assertions.assertTrue(content.toString().contains("k8s"))
            Assertions.assertTrue(content.toString().contains("nodes"))
            Assertions.assertTrue(content.toString().contains("pods"))
          } else if (entry!!.name == AIRBYTE_CSP_CHECKS) {
            foundCspChecksYaml = true
          }
        }

        // Ensure all yaml files are present in the zip
        Assertions.assertTrue(foundInstanceYaml)
        Assertions.assertFalse(foundDeploymentYaml)
        Assertions.assertTrue(foundCspChecksYaml)
      }
    }
  }
}

private fun mockDiagnosticToolHandler(withNodes: Boolean): DiagnosticToolHandler {
  val workspaceService: WorkspaceService =
    mockk {
      every { listStandardWorkspaces(false) } returns listOf(standardWorkspace)
    }

  val connectionService: ConnectionService =
    mockk {
      every { listWorkspaceStandardSyncs(standardWorkspace.workspaceId, false) } returns listOf(standardSync)
    }

  val sourceService: SourceService =
    mockk {
      every { listWorkspaceSourceConnection(any()) } returns listOf(sourceConnection)
      every { isSourceActive(any()) } returns true
      every { getSourceDefinitionFromSource(sourceConnection.sourceDefinitionId) } returns mockk()
    }

  val destinationService: DestinationService =
    mockk {
      every { listWorkspaceDestinationConnection(any()) } returns listOf(destinationConnection)
      every { isDestinationActive(any()) } returns true
      every { getStandardDestinationDefinition(destinationConnection.destinationDefinitionId) } returns mockk()
    }

  val actorDefinitionVersionHelper: ActorDefinitionVersionHelper =
    mockk {
      every { getSourceVersionWithOverrideStatus(any(), any(), any()) } returns actorDefinitionVersion
      every { getDestinationVersionWithOverrideStatus(any(), any(), any()) } returns actorDefinitionVersion
    }

  val cspChecker: CspChecker =
    mockk {
      every { check() } returns CheckResult(Storage(StorageType.LOCAL, emptyList()))
    }

  val instanceConfigurationHandler: InstanceConfigurationHandler =
    mockk {
      every { licenseInfo() } returns
        LicenseInfoResponse()
          .edition(
            "pro",
          ).licenseStatus(LicenseStatus.PRO)
          .expirationDate(OffsetDateTime.now().plusDays(10).toEpochSecond())
          .usedNodes(2)
    }

  val kubernetesClient: KubernetesClient = mockk {}
  val kubernetesClientPermissionHelper: KubernetesClientPermissionHelper = mockk {}

  val diagnosticToolHandler =
    DiagnosticToolHandler(
      workspaceService,
      connectionService,
      sourceService,
      destinationService,
      actorDefinitionVersionHelper,
      instanceConfigurationHandler,
      kubernetesClient,
      kubernetesClientPermissionHelper,
      cspChecker,
    )

  val node =
    Node().apply {
      metadata = ObjectMeta().apply { name = "node1" }
      status =
        NodeStatus().apply {
          conditions =
            listOf(
              NodeCondition().apply {
                type = "Ready"
                status = "true"
              },
            )
          allocatable = mapOf("cpu" to Quantity("500m"), "memory" to Quantity("1Gi"))
        }
    }

  every { kubernetesClient.nodes() } returns
    mockk {
      every { list() } returns NodeList().apply { items = listOf(node) }
    }

  val container =
    Container().apply {
      name = "containerName"
      resources =
        ResourceRequirements().apply {
          limits = mapOf("cpu" to Quantity("500m"), "memory" to Quantity("1Gi"))
        }
    }

  val pod =
    Pod().apply {
      metadata = ObjectMeta().apply { name = "pod1" }
      status = PodStatus().apply { phase = "Running" }
      spec = PodSpec().apply { containers = listOf(container) }
    }

  val podList = PodList().apply { items = listOf(pod) }
  every { kubernetesClient.namespace } returns "ab"
  every { kubernetesClient.pods() } returns
    mockk {
      every { inNamespace("ab") } returns
        mockk {
          every { list() } returns podList
        }
    }

  if (withNodes) {
    val mockNodeOperation = mockk<NonNamespaceOperation<Node, NodeList, Resource<Node>>>()
    every { mockNodeOperation.list() } returns NodeList().apply { items = listOf(node) }
    every { kubernetesClientPermissionHelper.listNodes() } returns mockNodeOperation
  }

  return diagnosticToolHandler
}

private val standardWorkspace: StandardWorkspace =
  StandardWorkspace()
    .withName("workspace1")
    .withWorkspaceId(UUID.randomUUID())

private val standardSync: StandardSync =
  StandardSync()
    .withName("connection1")
    .withStatus(StandardSync.Status.ACTIVE)
    .withConnectionId(UUID.randomUUID())
    .withSourceId(UUID.randomUUID())
    .withDestinationId(UUID.randomUUID())

private val sourceConnection: SourceConnection =
  SourceConnection()
    .withSourceId(UUID.randomUUID())
    .withName("source")
    .withSourceDefinitionId(UUID.randomUUID())

private val actorDefinitionVersion: ActorDefinitionVersionWithOverrideStatus =
  ActorDefinitionVersionWithOverrideStatus(
    ActorDefinitionVersion()
      .withDockerImageTag("tag")
      .withSupportState(ActorDefinitionVersion.SupportState.SUPPORTED),
    true,
  )

private val destinationConnection: DestinationConnection =
  DestinationConnection()
    .withDestinationId(UUID.randomUUID())
    .withName("destination1")
    .withDestinationDefinitionId(UUID.randomUUID())
