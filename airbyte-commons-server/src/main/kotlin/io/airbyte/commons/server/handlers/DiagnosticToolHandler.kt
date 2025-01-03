/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ActorType
import io.airbyte.commons.csp.CspChecker
import io.airbyte.commons.server.helpers.KubernetesClientPermissionHelper
import io.airbyte.commons.server.helpers.PermissionDeniedException
import io.airbyte.commons.yaml.Yamls
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
import io.fabric8.kubernetes.api.model.Node
import io.fabric8.kubernetes.api.model.NodeList
import io.fabric8.kubernetes.api.model.Quantity
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation
import io.fabric8.kubernetes.client.dsl.Resource
import jakarta.inject.Singleton
import org.yaml.snakeyaml.DumperOptions
import org.yaml.snakeyaml.Yaml
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.time.Instant
import java.util.UUID
import java.util.stream.Stream
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

internal const val AIRBYTE_INSTANCE_YAML = "airbyte_instance.yaml"
internal const val AIRBYTE_DEPLOYMENT_YAML = "airbyte_deployment.yaml"
internal const val AIRBYTE_CSP_CHECKS = "airbyte_csp_checks.yaml"
private const val DIAGNOSTIC_REPORT_FILE_NAME = "diagnostic_report"
private const val DIAGNOSTIC_REPORT_FILE_FORMAT = ".zip"
private const val UNKNOWN = "Unknown"

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
  private val kubernetesClientPermissionHelper: KubernetesClientPermissionHelper,
  private val cspChecker: CspChecker,
) {
  private val yaml = Yaml(DumperOptions().apply { defaultFlowStyle = DumperOptions.FlowStyle.BLOCK })

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
      FileOutputStream(tempFile).use { it.write(zipFileContent) }
      // Return the temporary file
      tempFile
    } catch (e: IOException) {
      logger.error { "Error generating diagnostic report: ${e.message}" }
      null
    }

  private fun generateZipInMemory(): ByteArray {
    val byteArrayOutputStream = ByteArrayOutputStream()
    val zipOut = ZipOutputStream(byteArrayOutputStream)

    runCatching {
      addAirbyteInstanceYaml(zipOut)
    }.onFailure {
      logger.error { "Error in writing airbyte instance yaml. Message: ${it.message}" }
    }
    runCatching {
      val nodes =
        try {
          kubernetesClientPermissionHelper.listNodes()
        } catch (e: PermissionDeniedException) {
          logger.warn { "Skipping writing deployment yaml; node viewer permission denied. Message: ${e.message}" }
          null
        }
      nodes?.let { addAirbyteDeploymentYaml(zipOut, it) }
    }.onFailure {
      logger.error { "Error in writing deployment yaml. Message: ${it.message}" }
    }

    runCatching {
      addAirbyteCspChecks(zipOut)
    }.onFailure { logger.error { "Error in writing csp-check yaml. Message: ${it.message}" } }

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

    return yaml.dump(airbyteInstanceYamlData)
  }

  private fun addAirbyteCspChecks(zipOut: ZipOutputStream) {
    ZipEntry(AIRBYTE_CSP_CHECKS).let { zipOut.putNextEntry(it) }
    zipOut.write(generateAirbyteCspChecks().toByteArray())
    zipOut.closeEntry()
  }

  private fun generateAirbyteCspChecks(): String = Yamls.serialize(cspChecker.check())

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
        }.toList()
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
        }.toList()
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
              sourceService.isSourceActive(source.sourceId)
            } catch (e: IOException) {
              false
            }
          }.map { source: SourceConnection ->
            var sourceDefinitionVersion: ActorDefinitionVersionWithOverrideStatus? = null
            try {
              sourceDefinitionVersion =
                actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(
                  sourceService.getStandardSourceDefinition(source.sourceDefinitionId),
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
          }.toList()

      // get all destinations by workspaceId (only include active ones in the report)
      val destinations =
        destinationService
          .listWorkspaceDestinationConnection(workspaceId)
          .filter { destination: DestinationConnection ->
            // TODO: isDestinationActive feels like it could not throw and just return false if the config is
            // not found.
            try {
              destinationService.isDestinationActive(destination.destinationId)
            } catch (e: IOException) {
              false
            }
          }.map { destination: DestinationConnection ->
            var destinationDefinitionVersion: ActorDefinitionVersionWithOverrideStatus? = null
            try {
              destinationDefinitionVersion =
                actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(
                  destinationService.getStandardDestinationDefinition(destination.destinationDefinitionId),
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
              "connectorDefinitionId" to destination.destinationDefinitionId.toString(),
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

  private fun addAirbyteDeploymentYaml(
    zipOut: ZipOutputStream,
    nodes: NonNamespaceOperation<Node, NodeList, Resource<Node>>?,
  ) {
    val zipEntry = ZipEntry(AIRBYTE_DEPLOYMENT_YAML)
    zipOut.putNextEntry(zipEntry)
    val deploymentYamlContent = generateDeploymentYaml(nodes)
    zipOut.write(deploymentYamlContent.toByteArray())
    zipOut.closeEntry()
  }

  private fun generateDeploymentYaml(nodes: NonNamespaceOperation<Node, NodeList, Resource<Node>>?): String {
    // Collect cluster information
    val deploymentYamlData = mapOf("k8s" to collectK8sInfo(nodes))
    return yaml.dump(deploymentYamlData)
  }

  private fun collectK8sInfo(nodes: NonNamespaceOperation<Node, NodeList, Resource<Node>>?): Map<String, Any> {
    logger.info { "Collecting k8s data..." }
    val kubernetesInfo =
      mapOf(
        "nodes" to collectNodeInfo(nodes),
        "pods" to collectPodInfo(kubernetesClient),
      )
    return kubernetesInfo
  }

  private fun collectNodeInfo(nodes: NonNamespaceOperation<Node, NodeList, Resource<Node>>?): List<Map<String, Any>> {
    logger.info { "Collecting nodes data..." }

    val nodeList: List<Map<String, Any>> =
      nodes
        ?.list()
        ?.items
        ?.map { node ->
          val limits = Limits(node.status.allocatable)
          val readyStatus =
            node.status.conditions
              .filter { it.type == "Ready" }
              .firstNotNullOfOrNull { it.status } ?: UNKNOWN

          mapOf(
            "name" to node.metadata.name,
            "readyStatus" to readyStatus,
            "cpu" to limits.cpu,
            "memory" to limits.memory,
          )
        }?.toList() ?: emptyList()

    return nodeList
  }

  private fun collectPodInfo(client: KubernetesClient): List<Map<String, Any>> {
    logger.info { "Collecting pods data..." }
    val currentNamespace = client.namespace
    logger.info { "Current namespace from client: $currentNamespace" }
    val pods =
      client
        .pods()
        ?.inNamespace(currentNamespace)
        ?.list()
        ?.items ?: emptyList()

    val podList: List<Map<String, Any>> =
      pods
        .map { pod ->
          val podInfo =
            mutableMapOf<String, Any>(
              "name" to pod.metadata.name,
              "status" to pod.status.phase,
            )

          val containerLimits: List<Map<String, String>> =
            pod.spec.containers
              .map { container ->
                val limits = Limits(container.resources?.limits)

                mapOf(
                  "containerName" to container.name,
                  "cpu" to limits.cpu,
                  "memory" to limits.memory,
                )
              }.toList()

          podInfo["limits"] = containerLimits
          podInfo
        }.toList()

    return podList
  }
}

private data class Limits(
  val cpu: String = UNKNOWN,
  val memory: String = UNKNOWN,
) {
  constructor(limits: Map<String, Quantity>?) : this(
    cpu = limits?.get("cpu")?.let { it.amount.toString() + it.format.toString() } ?: UNKNOWN,
    memory = limits?.get("memory")?.let { it.amount.toString() + it.format.toString() } ?: UNKNOWN,
  )
}
