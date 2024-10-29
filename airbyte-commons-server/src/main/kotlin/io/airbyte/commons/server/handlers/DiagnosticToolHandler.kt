/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ActorType
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
import io.fabric8.kubernetes.api.model.Quantity
import io.fabric8.kubernetes.client.KubernetesClient
import jakarta.inject.Singleton
import org.yaml.snakeyaml.DumperOptions
import org.yaml.snakeyaml.Yaml
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.time.Instant
import java.util.UUID
import java.util.function.Consumer
import java.util.stream.Stream
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

const val AIRBYTE_INSTANCE_YAML: String = "airbyte_instance.yaml"
const val AIRBYTE_DEPLOYMENT_YAML: String = "airbyte_deployment.yaml"
const val DIAGNOSTIC_REPORT_FILE_NAME: String = "diagnostic_report"
const val DIAGNOSTIC_REPORT_FILE_FORMAT: String = ".zip"
const val UNKNOWN: String = "Unknown"

/**
 * DiagnosticToolHandler.
 */
@Singleton
open class DiagnosticToolHandler(
  private val workspaceService: WorkspaceService,
  private val connectionService: ConnectionService,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
  private val instanceConfigurationHandler: InstanceConfigurationHandler,
  private val kubernetesClient: KubernetesClient,
) {
  private val yamlDumperOptions = DumperOptions().apply { defaultFlowStyle = DumperOptions.FlowStyle.BLOCK }

  /**
   * Generate diagnostic report by collecting relevant data and zipping them into a single file.
   *
   * @return File - generated zip file as a diagnostic report
   */
  fun generateDiagnosticReport(): File? =
    try {
      // Generate the zip file content in memory as byte[]
      val zipFileContent = generateZipInMemory()

      // Write the byte[] to a temporary file
      val tempFile = File.createTempFile(DIAGNOSTIC_REPORT_FILE_NAME, DIAGNOSTIC_REPORT_FILE_FORMAT)
      FileOutputStream(tempFile).use { fos ->
        fos.write(zipFileContent)
      }
      // Return the temporary file
      tempFile
    } catch (e: IOException) {
      logger.error { "Error generating diagnostic report: ${e.message}" }
      null
    }

  private fun generateZipInMemory(): ByteArray {
    val byteArrayOutputStream = ByteArrayOutputStream()
    val zipOut = ZipOutputStream(byteArrayOutputStream)
    try {
      addAirbyteInstanceYaml(zipOut)
    } catch (e: Exception) {
      logger.error { "Error in writing airbyte instance yaml. Message: ${e.message}" }
    }
    try {
      addAirbyteDeploymentYaml(zipOut)
    } catch (e: IOException) {
      logger.error { "Error in writing deployment yaml. Message: ${e.message}" }
    }
    zipOut.finish()
    return byteArrayOutputStream.toByteArray()
  }

  private fun addAirbyteInstanceYaml(zipOut: ZipOutputStream) {
    // In-memory construct an entry (yaml file) in the final zip output.
    val airbyteInstanceYaml = ZipEntry(AIRBYTE_INSTANCE_YAML)
    zipOut.putNextEntry(airbyteInstanceYaml)

    // Write instance information to the zip entry
    val airbyteInstanceYamlContent = generateAirbyteInstanceYaml()
    zipOut.write(airbyteInstanceYamlContent.toByteArray())
    zipOut.closeEntry()
  }

  private fun generateAirbyteInstanceYaml(): String {
    val airbyteInstanceYamlData =
      mapOf(
        // Collect workspace information
        "workspaces" to collectWorkspaceInfo(),
        // Collect license information
        "license" to collectLicenseInfo(),
        // TODO: Collect other information here, e.g: application logs, etc.
      )
    val yaml = Yaml(yamlDumperOptions)
    return yaml.dump(airbyteInstanceYamlData)
  }

  private fun collectWorkspaceInfo(): List<Map<String, Any>> =
    try {
      // get all workspaces
      logger.info { "Collecting workspaces data..." }
      workspaceService
        .listStandardWorkspaces(false)
        .map { workspace: StandardWorkspace ->
          mapOf(
            "name" to workspace.name,
            "id" to workspace.workspaceId.toString(),
            "connections" to collectConnectionInfo(workspace.workspaceId),
            "connectors" to collectConnectorInfo(workspace.workspaceId),
          )
        }
        .toList()
    } catch (e: IOException) {
      logger.error { "Error collecting workspace information. Message: ${e.message}}" }
      emptyList()
    }

  private fun collectConnectionInfo(workspaceId: UUID): List<Map<String, String>> =
    try {
      logger.info { "Collecting connections data..." }
      // get all connections by workspaceId
      connectionService
        .listWorkspaceStandardSyncs(workspaceId, false)
        .map { connection: StandardSync ->
          mapOf(
            "name" to connection.name,
            "id" to connection.connectionId.toString(),
            "status" to connection.status.toString(),
            "sourceId" to connection.sourceId.toString(),
            "destinationId" to connection.destinationId.toString(),
          )
        }
        .toList()
    } catch (e: IOException) {
      logger.error { "Error collecting connection information. Message: ${e.message}" }
      emptyList()
    }

  private fun collectConnectorInfo(workspaceId: UUID): List<Map<String, String>> =
    try {
      logger.info { "Collecting connectors data..." }
      // get all sources by workspaceId (only include active ones in the report)
      val sources =
        sourceService
          .listWorkspaceSourceConnection(workspaceId)
          .filter { source: SourceConnection ->
            // TODO: isSourceActive feels like it could not throw and just return false if the config is not
            // found.
            try {
              return@filter sourceService.isSourceActive(source.sourceId)
            } catch (e: IOException) {
              return@filter false
            }
          }
          .map { source: SourceConnection ->
            var sourceDefinitionVersion: ActorDefinitionVersionWithOverrideStatus? = null
            try {
              sourceDefinitionVersion =
                actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(
                  sourceService.getSourceDefinitionFromSource(source.sourceDefinitionId),
                  workspaceId,
                  source.sourceId,
                )
            } catch (e: Exception) {
              logger.error { "Error collecting source version information. Message: ${e.message}" }
            }
            mapOf(
              "name" to source.name,
              "id" to source.sourceId.toString(),
              "type" to ActorType.SOURCE.toString(),
              "connectorDefinitionId" to source.sourceDefinitionId.toString(),
              "connectorDockerImageTag" to (sourceDefinitionVersion?.actorDefinitionVersion?.dockerImageTag ?: ""),
              "connectorVersionOverrideApplied" to (sourceDefinitionVersion?.isOverrideApplied?.toString() ?: ""),
              "connectorSupportState" to (sourceDefinitionVersion?.actorDefinitionVersion?.supportState?.toString() ?: ""),
            )
          }
          .toList()

      // get all destinations by workspaceId (only include active ones in the report)
      val destinations =
        destinationService
          .listWorkspaceDestinationConnection(workspaceId)
          .filter { destination: DestinationConnection ->
            // TODO: isDestinationActive feels like it could not throw and just return false if the config is
            // not found.
            try {
              return@filter destinationService.isDestinationActive(destination.destinationId)
            } catch (e: IOException) {
              return@filter false
            }
          }
          .map { destination: DestinationConnection ->
            var destinationDefinitionVersion: ActorDefinitionVersionWithOverrideStatus? = null
            try {
              destinationDefinitionVersion =
                actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(
                  destinationService.getStandardDestinationDefinition(destination.destinationId),
                  workspaceId,
                  destination.destinationId,
                )
            } catch (e: Exception) {
              logger.error { "Error collecting destination version information. Message: ${e.message}" }
            }
            mapOf(
              "name" to destination.name,
              "id" to destination.destinationId.toString(),
              "type" to ActorType.DESTINATION.toString(),
              "connectorDefinitionId" to destination.destinationId.toString(),
              "connectorDockerImageTag" to (destinationDefinitionVersion?.actorDefinitionVersion?.dockerImageTag ?: ""),
              "connectorVersionOverrideApplied" to (destinationDefinitionVersion?.isOverrideApplied?.toString() ?: ""),
              "connectorSupportState" to (destinationDefinitionVersion?.actorDefinitionVersion?.supportState?.toString() ?: ""),
            )
          }.toList()
      // merge the two lists
      Stream.concat(sources.stream(), destinations.stream()).toList()
    } catch (e: IOException) {
      logger.error { "Error collecting connectors information. Message: ${e.message}" }
      emptyList()
    }

  private fun collectLicenseInfo(): Map<String, Any> {
    logger.info { "Collecting license data..." }
    val license = instanceConfigurationHandler.licenseInfo()
    if (license == null) {
      logger.error { "Error collecting license information" }
      return emptyMap()
    }

    val licenseInfo =
      mapOf<String, Any>(
        "edition" to license.edition,
        "status" to license.licenseStatus.toString(),
        "expiryDate" to Instant.ofEpochSecond(license.expirationDate).toString(),
        "maxEditors" to (license.maxEditors ?: UNKNOWN),
        "maxNodes" to (license.maxNodes ?: UNKNOWN),
        "usedEditors" to (license.usedEditors ?: UNKNOWN),
        "usedNodes" to (license.usedNodes ?: UNKNOWN),
      )
    return licenseInfo
  }

  private fun addAirbyteDeploymentYaml(zipOut: ZipOutputStream) {
    val zipEntry = ZipEntry(AIRBYTE_DEPLOYMENT_YAML)
    zipOut.putNextEntry(zipEntry)
    val deploymentYamlContent = generateDeploymentYaml()
    zipOut.write(deploymentYamlContent.toByteArray())
    zipOut.closeEntry()
  }

  private fun generateDeploymentYaml(): String {
    // Collect cluster information
    val deploymentYamlData =
      mapOf(
        "k8s" to collectK8sInfo(),
      )
    val yaml = Yaml(yamlDumperOptions)
    return yaml.dump(deploymentYamlData)
  }

  private fun collectK8sInfo(): Map<String, Any> {
    logger.info { "Collecting k8s data..." }
    val kubernetesInfo =
      mapOf(
        "nodes" to collectNodeInfo(kubernetesClient),
        "pods" to collectPodInfo(kubernetesClient),
      )
    return kubernetesInfo
  }

  private fun collectNodeInfo(client: KubernetesClient): List<Map<String, Any>> {
    logger.info { "Collecting nodes data..." }
    val nodeList: MutableList<Map<String, Any>> = ArrayList()
    val nodes = client.nodes()?.list()?.items ?: emptyList()
    for (node in nodes) {
      val nodeInfo =
        mapOf(
          "name" to node.metadata.name,
          "readyStatus" to (
            node.status.conditions
              .filter { it.type == "Ready" }
              .firstNotNullOfOrNull { it.status } ?: UNKNOWN
          ),
          "cpu" to (node.status.allocatable["cpu"]?.let { it.amount.toString() + it.format } ?: UNKNOWN),
          "memory" to (node.status.allocatable["memory"]?.let { it.amount.toString() + it.format } ?: UNKNOWN),
        )
      nodeList.add(nodeInfo)
    }
    return nodeList
  }

  private fun collectPodInfo(client: KubernetesClient): List<Map<String, Any>> {
    logger.info { "Collecting pods data..." }
    val podList: MutableList<Map<String, Any>> = ArrayList()
    val pods = client.pods()?.inNamespace("ab")?.list()?.items ?: emptyList()
    for (pod in pods) {
      val podInfo: MutableMap<String, Any> = HashMap()
      podInfo["name"] = pod.metadata.name
      podInfo["status"] = pod.status.phase
      val containerLimits: MutableList<Map<String, Any?>> = ArrayList()
      pod.spec.containers.forEach(
        Consumer { container: Container ->
          val containerLimit: MutableMap<String, Any?> = HashMap()
          containerLimit["containerName"] = container.name
          val limit = getContainerResourceLimit(container)
          containerLimit["cpu"] = limit?.get("cpu")?.let { it.amount.toString() + it.format.toString() } ?: UNKNOWN
          containerLimit["memory"] = limit?.get("memory")?.let { it.amount.toString() + it.format.toString() } ?: UNKNOWN
          containerLimits.add(containerLimit)
        },
      )
      podInfo["limits"] = containerLimits
      podList.add(podInfo)
    }
    return podList
  }

  private fun getContainerResourceLimit(container: Container): Map<String, Quantity>? {
    if (container.resources == null || container.resources.limits == null) {
      return null
    }
    return container.resources.limits
  }
}
