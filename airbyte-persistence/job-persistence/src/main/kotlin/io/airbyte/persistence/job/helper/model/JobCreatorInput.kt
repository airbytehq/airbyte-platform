package io.airbyte.persistence.job.helper.model

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.DestinationConnection
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOperation
import java.util.UUID

data class JobCreatorInput(
  val source: SourceConnection,
  val destination: DestinationConnection,
  val standardSync: StandardSync,
  val sourceDockerImageName: String,
  val sourceProtocolVersion: Version,
  val destinationDockerImageName: String,
  val destinationProtocolVersion: Version,
  val standardSyncOperations: List<StandardSyncOperation>,
  val webhookOperationConfigs: JsonNode?,
  val sourceDefinition: StandardSourceDefinition,
  val destinationDefinition: StandardDestinationDefinition,
  val sourceDefinitionVersion: ActorDefinitionVersion,
  val destinationDefinitionVersion: ActorDefinitionVersion,
  val workspaceId: UUID,
)
