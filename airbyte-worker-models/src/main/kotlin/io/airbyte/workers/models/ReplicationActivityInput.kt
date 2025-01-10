package io.airbyte.workers.models

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.ConnectionContext
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.SyncResourceRequirements
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import java.util.UUID

/**
 * A class holding the input to the Temporal replication activity.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class ReplicationActivityInput(
  // Actor ID for the source used in the sync - this is used to update the actor configuration when
  // requested.
  val sourceId: UUID? = null,
// Actor ID for the destination used in the sync - this is used to update the actor configuration
// when requested.
  val destinationId: UUID? = null,
// Source-connector specific blob. Must be a valid JSON string.
  var sourceConfiguration: JsonNode? = null,
// Destination-connector specific blob. Must be a valid JSON string.
  var destinationConfiguration: JsonNode? = null,
// The job info -- job id, attempt id -- for this sync.
  val jobRunConfig: JobRunConfig? = null,
// Config related to the source launcher (rather than the source itself) e.g., whether it's a custom
// connector.
  val sourceLauncherConfig: IntegrationLauncherConfig? = null,
// Config related to the destination launcher (rather than the destination itself) e.g., whether it
// supports DBT
// transformations.
  val destinationLauncherConfig: IntegrationLauncherConfig? = null,
// Resource requirements to use for the sync.
  val syncResourceRequirements: SyncResourceRequirements? = null,
// The id of the workspace associated with this sync.
  val workspaceId: UUID? = null,
// The id of the connection associated with this sync.
  val connectionId: UUID? = null,
// The task queue that replication will use.
  val taskQueue: String? = null,
// Whether this 'sync' is performing a logical reset.
  var isReset: Boolean? = null,
// The type of namespace definition - e.g. source, destination, or custom.
  val namespaceDefinition: JobSyncConfig.NamespaceDefinitionType? = null,
  val namespaceFormat: String? = null,
// Prefix that will be prepended to the name of each stream when it is written to the destination.
  val prefix: String? = null,
// The results of schema refresh, including the applied diff which is used to determine which
// streams to backfill.
  var schemaRefreshOutput: RefreshSchemaActivityOutput? = null,
// Replication context object containing relevant IDs
  val connectionContext: ConnectionContext? = null,
  val signalInput: String? = null,
  val networkSecurityTokens: List<String>? = null,
)
