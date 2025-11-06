/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.convertValue
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.CheckConnectionRead
import io.airbyte.api.client.model.generated.ConnectionCreate
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.api.client.model.generated.ConnectorBuilderProjectDetails
import io.airbyte.api.client.model.generated.ConnectorBuilderProjectIdWithWorkspaceId
import io.airbyte.api.client.model.generated.ConnectorBuilderProjectWithWorkspaceId
import io.airbyte.api.client.model.generated.ConnectorBuilderPublishRequestBody
import io.airbyte.api.client.model.generated.CreateDeclarativeSourceDefinitionRequest
import io.airbyte.api.client.model.generated.CreateDefinitionRequest
import io.airbyte.api.client.model.generated.CustomDestinationDefinitionCreate
import io.airbyte.api.client.model.generated.CustomSourceDefinitionCreate
import io.airbyte.api.client.model.generated.DataplaneGroupListRequestBody
import io.airbyte.api.client.model.generated.DeclarativeSourceManifest
import io.airbyte.api.client.model.generated.DestinationCreate
import io.airbyte.api.client.model.generated.DestinationDefinitionCreate
import io.airbyte.api.client.model.generated.DestinationUpdate
import io.airbyte.api.client.model.generated.JobIdRequestBody
import io.airbyte.api.client.model.generated.ScopeType
import io.airbyte.api.client.model.generated.SourceCreate
import io.airbyte.api.client.model.generated.SourceDefinitionCreate
import io.airbyte.api.client.model.generated.SourceUpdate
import io.airbyte.api.client.model.generated.UpdateDeclarativeSourceDefinitionRequest
import io.airbyte.api.client.model.generated.UpdateDefinitionRequest
import io.airbyte.api.client.model.generated.WorkspaceCreate
import io.airbyte.api.client.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.test.acceptance.CUSTOM_DOCKER_REPO
import io.airbyte.test.acceptance.CUSTOM_DOCKER_TAG
import io.airbyte.test.acceptance.NAME_PREFIX
import java.net.URI
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

private val CUSTOM_URL = URI("")

internal fun AirbyteApiClient.createWorkspace(): UUID =
  workspaceApi
    .createWorkspace(
      WorkspaceCreate(
        name = "acceptance tests ${now()}",
        organizationId = DEFAULT_ORGANIZATION_ID,
        email = "acceptance-tests@airbyte.io",
      ),
    ).workspaceId

internal fun AirbyteApiClient.deleteWorkspace(workspaceId: UUID) = workspaceApi.deleteWorkspace(WorkspaceIdRequestBody(workspaceId))

internal fun AirbyteApiClient.createSourceDefinition(workspaceId: UUID): UUID =
  sourceDefinitionApi
    .createCustomSourceDefinition(
      CustomSourceDefinitionCreate(
        sourceDefinition =
          SourceDefinitionCreate(
            name = "$NAME_PREFIX - ${now()}",
            dockerRepository = CUSTOM_DOCKER_REPO,
            dockerImageTag = CUSTOM_DOCKER_TAG,
            documentationUrl = CUSTOM_URL,
          ),
        scopeId = workspaceId,
        scopeType = ScopeType.WORKSPACE,
      ),
    ).sourceDefinitionId

internal fun AirbyteApiClient.createAtcSource(
  workspaceId: UUID,
  sourceDefinitionId: UUID,
  cfg: AtcConfig = AtcConfig(),
): UUID =
  sourceApi
    .createSource(
      SourceCreate(
        sourceDefinitionId = sourceDefinitionId,
        connectionConfiguration = mapper.convertValue<JsonNode>(cfg),
        workspaceId = workspaceId,
        name = "$NAME_PREFIX - ${now()}",
      ),
    ).sourceId

internal fun AirbyteApiClient.createSource(
  workspaceId: UUID,
  sourceDefinitionId: UUID,
  cfg: String,
): UUID =
  sourceApi
    .createSource(
      SourceCreate(
        sourceDefinitionId = sourceDefinitionId,
        connectionConfiguration = mapper.readTree(cfg),
        workspaceId = workspaceId,
        name = "$NAME_PREFIX - ${now()}",
      ),
    ).sourceId

internal fun AirbyteApiClient.updateAtcSource(
  sourceId: UUID,
  cfg: AtcConfig = AtcConfig(),
): UUID {
  val config = mapper.convertValue<JsonNode>(cfg)
  val update =
    SourceUpdate(
      sourceId = sourceId,
      connectionConfiguration = config,
      name = "$NAME_PREFIX - ${now()}",
    )
  val status = sourceApi.checkConnectionToSourceForUpdate(update).status
  if (status != CheckConnectionRead.Status.SUCCEEDED) {
    throw Exception("check connection failed: $status")
  }

  return sourceApi.updateSource(update).sourceId
}

internal fun AirbyteApiClient.createDestinationDefinition(workspaceId: UUID): UUID =
  destinationDefinitionApi
    .createCustomDestinationDefinition(
      CustomDestinationDefinitionCreate(
        destinationDefinition =
          DestinationDefinitionCreate(
            name = "$NAME_PREFIX - ${now()}",
            dockerRepository = CUSTOM_DOCKER_REPO,
            dockerImageTag = CUSTOM_DOCKER_TAG,
            documentationUrl = CUSTOM_URL,
          ),
        scopeId = workspaceId,
        scopeType = ScopeType.WORKSPACE,
      ),
    ).destinationDefinitionId

internal fun AirbyteApiClient.createAtcDestination(
  workspaceId: UUID,
  destinationDefinitionId: UUID,
  cfg: AtcConfig = AtcConfig(),
): UUID =
  destinationApi
    .createDestination(
      DestinationCreate(
        destinationDefinitionId = destinationDefinitionId,
        connectionConfiguration = mapper.convertValue<JsonNode>(cfg),
        workspaceId = workspaceId,
        name = "$NAME_PREFIX - ${now()}",
      ),
    ).destinationId

internal fun AirbyteApiClient.updateAtcDestination(
  destinationId: UUID,
  cfg: AtcConfig = AtcConfig(),
): UUID {
  val config = mapper.convertValue<JsonNode>(cfg)
  val update =
    DestinationUpdate(
      destinationId = destinationId,
      connectionConfiguration = config,
      name = "$NAME_PREFIX - ${now()}",
    )

  val status = destinationApi.checkConnectionToDestinationForUpdate(update).status
  if (status != CheckConnectionRead.Status.SUCCEEDED) {
    throw Exception("check connection failed: $status")
  }

  return destinationApi.updateDestination(update).destinationId
}

internal fun AirbyteApiClient.createConnection(connectionCreate: ConnectionCreate): UUID =
  connectionApi.createConnection(connectionCreate).connectionId

internal fun AirbyteApiClient.deleteConnection(connectionId: UUID) {
  connectionApi.deleteConnection(ConnectionIdRequestBody(connectionId))
}

internal fun AirbyteApiClient.connectionStatus(connectionId: UUID): ConnectionStatus =
  connectionApi.getConnection(ConnectionIdRequestBody(connectionId)).status

internal fun AirbyteApiClient.syncConnection(connectionId: UUID): Long = connectionApi.syncConnection(ConnectionIdRequestBody(connectionId)).job.id

internal fun AirbyteApiClient.fetchDataplaneGroupId(organizationId: UUID = DEFAULT_ORGANIZATION_ID): UUID =
  dataplaneGroupApi
    .listDataplaneGroups(DataplaneGroupListRequestBody(organizationId))
    .dataplaneGroups
    ?.firstOrNull()
    ?.dataplaneGroupId
    ?: throw IllegalStateException("No Dataplane Group Id found for organization $organizationId")

internal fun AirbyteApiClient.jobLogs(jobId: Long): Map<String, String> =
  jobsApi
    .getJobInfo(JobIdRequestBody(jobId))
    .attempts
    .associate { "${it.attempt.id} (${it.attempt.status})" to it.logs?.events }
    .mapValues { it.value?.joinToString(separator = "\n") { event -> event.message } ?: "" }
    .filterValues { it.isNotEmpty() }

internal fun AirbyteApiClient.jobCancel(jobId: Long) = jobsApi.cancelJob(JobIdRequestBody(jobId))

internal fun AirbyteApiClient.createConnectorBuilderProject(workspaceId: UUID): UUID =
  connectorBuilderProjectApi
    .createConnectorBuilderProject(
      ConnectorBuilderProjectWithWorkspaceId(
        workspaceId = workspaceId,
        builderProject = ConnectorBuilderProjectDetails(name = "acceptance tests ${now()}"),
      ),
    ).builderProjectId

internal fun AirbyteApiClient.deleteConnectorBuilderProject(
  workspaceId: UUID,
  buildProjectId: UUID,
): UUID {
  connectorBuilderProjectApi.deleteConnectorBuilderProject(
    ConnectorBuilderProjectIdWithWorkspaceId(
      workspaceId = workspaceId,
      builderProjectId = buildProjectId,
    ),
  )
  return buildProjectId
}

internal fun AirbyteApiClient.publishConnectorBuilderProject(
  workspaceId: UUID,
  builderProjectId: UUID,
  manifest: String,
  spec: String,
): UUID =
  connectorBuilderProjectApi
    .publishConnectorBuilderProject(
      ConnectorBuilderPublishRequestBody(
        workspaceId = workspaceId,
        builderProjectId = builderProjectId,
        name = "acceptance tests ${now()}",
        initialDeclarativeManifest =
          DeclarativeSourceManifest(
            description = "acceptance tests ${now()}",
            manifest = mapper.readTree(manifest),
            spec = mapper.readTree(spec),
            version = 1L,
          ),
      ),
    ).sourceDefinitionId

fun PublicApiClient.publicCreateSourceDefinition(workspaceId: UUID): UUID =
  sourceDefinitionsApi
    .publicCreateSourceDefinition(
      workspaceId = workspaceId,
      createDefinitionRequest =
        CreateDefinitionRequest(
          name = "$NAME_PREFIX - ${now()}",
          dockerRepository = CUSTOM_DOCKER_REPO,
          dockerImageTag = CUSTOM_DOCKER_TAG,
          documentationUrl = CUSTOM_URL,
        ),
    ).id
    .let { UUID.fromString(it) }

fun PublicApiClient.publicUpdateSourceDefinition(
  workspaceId: UUID,
  srcDefId: UUID,
): UUID =
  sourceDefinitionsApi
    .publicUpdateSourceDefinition(
      workspaceId = workspaceId,
      definitionId = srcDefId,
      updateDefinitionRequest =
        UpdateDefinitionRequest(
          name = "$NAME_PREFIX-update - ${now()}",
          dockerImageTag = CUSTOM_DOCKER_TAG,
        ),
    ).id
    .let { UUID.fromString(it) }

fun PublicApiClient.publicDeleteSourceDefinition(
  workspaceId: UUID,
  srcDefId: UUID,
): UUID =
  sourceDefinitionsApi
    .publicDeleteSourceDefinition(
      workspaceId = workspaceId,
      definitionId = srcDefId,
    ).id
    .let { UUID.fromString(it) }

fun PublicApiClient.publicCreateDestinationDefinition(workspaceId: UUID): UUID =
  destinationDefinitionsApi
    .publicCreateDestinationDefinition(
      workspaceId = workspaceId,
      createDefinitionRequest =
        CreateDefinitionRequest(
          name = "$NAME_PREFIX - ${now()}",
          dockerRepository = CUSTOM_DOCKER_REPO,
          dockerImageTag = CUSTOM_DOCKER_TAG,
          documentationUrl = CUSTOM_URL,
        ),
    ).id
    .let { UUID.fromString(it) }

fun PublicApiClient.publicUpdateDestinationDefinition(
  workspaceId: UUID,
  dstDefId: UUID,
): UUID =
  destinationDefinitionsApi
    .publicUpdateDestinationDefinition(
      workspaceId = workspaceId,
      definitionId = dstDefId,
      updateDefinitionRequest =
        UpdateDefinitionRequest(
          name = "$NAME_PREFIX-update - ${now()}",
          dockerImageTag = CUSTOM_DOCKER_TAG,
        ),
    ).id
    .let { UUID.fromString(it) }

fun PublicApiClient.publicDeleteDestinationDefinition(
  workspaceId: UUID,
  dstDefId: UUID,
): UUID =
  destinationDefinitionsApi
    .publicDeleteDestinationDefinition(
      workspaceId = workspaceId,
      definitionId = dstDefId,
    ).id
    .let { UUID.fromString(it) }

fun PublicApiClient.publicCreateDeclarativeSourceDefinition(
  workspaceId: UUID,
  name: String,
  manifest: String,
): UUID =
  declarativeSourceDefinitionsApi
    .publicCreateDeclarativeSourceDefinition(
      workspaceId = workspaceId,
      createDeclarativeSourceDefinitionRequest =
        CreateDeclarativeSourceDefinitionRequest(
          name = name,
          manifest = mapper.readTree(manifest),
        ),
    ).id
    .let { UUID.fromString(it) }

fun PublicApiClient.publicUpdateDeclarativeSourceDefinition(
  workspaceId: UUID,
  decSrcDefId: UUID,
  manifest: String,
): UUID =
  declarativeSourceDefinitionsApi
    .publicUpdateDeclarativeSourceDefinition(
      workspaceId = workspaceId,
      definitionId = decSrcDefId,
      updateDeclarativeSourceDefinitionRequest = UpdateDeclarativeSourceDefinitionRequest(manifest = mapper.readTree(manifest)),
    ).id
    .let { UUID.fromString(it) }

/**
 * Generates the current timestamp formatted as 'yyyyMMddHHmmss'.
 *
 * @return A string representing the current timestamp in the specified format.
 */
private fun now(): String {
  val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
  return LocalDateTime.now().format(formatter)
}

/** Jackson object mapper. */
private val mapper = jacksonObjectMapper()
