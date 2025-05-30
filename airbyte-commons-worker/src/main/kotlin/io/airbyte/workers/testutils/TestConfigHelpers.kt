/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.testutils

import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.ConnectionContext
import io.airbyte.config.DestinationConnection
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncInput
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.State
import io.airbyte.config.SyncMode
import io.airbyte.config.helpers.CatalogHelpers
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.Field
import java.util.UUID

private const val CONNECTION_NAME = "favorite_color_pipe"
private const val STREAM_NAME = "user_preferences"
private const val FIELD_NAME = "favorite_color"
private const val LAST_SYNC_TIME: Long = 1598565106

/**
 * Helper for creating test configs.
 */
object TestConfigHelpers {
  const val DESTINATION_IMAGE: String = "destination:1.1"
  const val SOURCE_IMAGE: String = "source:1.1"

  /**
   * Create sync config and appropriate sync input.
   *
   * @return sync config and sync input.
   */
  @JvmStatic
  fun createSyncConfig(
    organizationId: UUID,
    sourceDefinitionId: UUID,
  ): Pair<StandardSync, StandardSyncInput> {
    val replicationInputPair = createReplicationConfig()
    val replicationInput = replicationInputPair.second
    // For now, these are identical, so we delegate to createReplicationConfig and copy it over for
    // simplicity.
    // Someday these will likely diverge, so it may not make sense to base the sync input on the
    // replication input.
    return Pair(
      replicationInputPair.first,
      StandardSyncInput()
        .withNamespaceDefinition(replicationInput.getNamespaceDefinition())
        .withPrefix(replicationInput.getPrefix())
        .withSourceId(replicationInput.getSourceId())
        .withDestinationId(replicationInput.getDestinationId())
        .withDestinationConfiguration(replicationInput.getDestinationConfiguration())
        .withSourceConfiguration(replicationInput.getSourceConfiguration())
        .withOperationSequence(replicationInput.getOperationSequence())
        .withWorkspaceId(replicationInput.getWorkspaceId())
        .withConnectionContext(
          ConnectionContext()
            .withOrganizationId(organizationId)
            .withSourceDefinitionId(sourceDefinitionId),
        ).withUseAsyncActivities(true)
        .withUseAsyncReplicate(true),
    )
  }

  /**
   * Create sync config and appropriate replication input.
   *
   * @param multipleNamespaces stream namespaces.
   * @return sync config and replication input
   */
  @JvmOverloads
  @JvmStatic
  fun createReplicationConfig(multipleNamespaces: Boolean = false): Pair<StandardSync, ReplicationInput> {
    val workspaceId = UUID.randomUUID()
    val sourceDefinitionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val destinationDefinitionId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val connectionId = UUID.randomUUID()
    val normalizationOperationId = UUID.randomUUID()
    val dbtOperationId = UUID.randomUUID()

    val sourceConnection =
      Jsons.jsonNode(
        mapOf(
          "apiKey" to "123",
          "region" to "us-east",
        ),
      )

    val destinationConnection =
      Jsons.jsonNode(
        mapOf(
          "username" to "airbyte",
          "token" to "anau81b",
        ),
      )

    val sourceConnectionConfig =
      SourceConnection()
        .withConfiguration(sourceConnection)
        .withWorkspaceId(workspaceId)
        .withSourceDefinitionId(sourceDefinitionId)
        .withSourceId(sourceId)
        .withTombstone(false)

    val destinationConnectionConfig =
      DestinationConnection()
        .withConfiguration(destinationConnection)
        .withWorkspaceId(workspaceId)
        .withDestinationDefinitionId(destinationDefinitionId)
        .withDestinationId(destinationId)
        .withTombstone(false)

    val normalizationOperation =
      StandardSyncOperation()
        .withOperationId(normalizationOperationId)
        .withName("Normalization")
        .withTombstone(false)

    val customDbtOperation =
      StandardSyncOperation()
        .withOperationId(dbtOperationId)
        .withName("Custom Transformation")
        .withTombstone(false)

    val catalog = ConfiguredAirbyteCatalog()
    if (multipleNamespaces) {
      val streamOne =
        ConfiguredAirbyteStream(
          CatalogHelpers.createAirbyteStream(STREAM_NAME, "namespace", Field.of(FIELD_NAME, JsonSchemaType.STRING)),
          SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND,
        )
      val streamTwo =
        ConfiguredAirbyteStream(
          CatalogHelpers.createAirbyteStream(STREAM_NAME, "namespace2", Field.of(FIELD_NAME, JsonSchemaType.STRING)),
          SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND,
        )

      val streams = listOf(streamOne, streamTwo)
      catalog.withStreams(streams)
    } else {
      val stream =
        ConfiguredAirbyteStream(
          CatalogHelpers.createAirbyteStream(STREAM_NAME, Field.of(FIELD_NAME, JsonSchemaType.STRING)),
          SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND,
        )
      catalog.withStreams(listOf(stream))
    }

    val standardSync =
      StandardSync()
        .withConnectionId(connectionId)
        .withDestinationId(destinationId)
        .withSourceId(sourceId)
        .withStatus(StandardSync.Status.ACTIVE)
        .withName(CONNECTION_NAME)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
        .withPrefix(CONNECTION_NAME)
        .withCatalog(catalog)
        .withOperationIds(listOf(normalizationOperationId, dbtOperationId))

    val stateValue = Jsons.serialize(mapOf<String, String>("lastSync" to LAST_SYNC_TIME.toString()))

    val state = State().withState(Jsons.jsonNode<String?>(stateValue))

    val replicationInput =
      ReplicationInput()
        .withNamespaceDefinition(standardSync.getNamespaceDefinition())
        .withConnectionId(connectionId)
        .withPrefix(standardSync.getPrefix())
        .withSourceId(sourceId)
        .withSourceLauncherConfig(IntegrationLauncherConfig().withDockerImage(SOURCE_IMAGE))
        .withDestinationId(destinationId)
        .withDestinationConfiguration(destinationConnectionConfig.getConfiguration())
        .withDestinationLauncherConfig(IntegrationLauncherConfig().withDockerImage(DESTINATION_IMAGE))
        .withCatalog(standardSync.getCatalog())
        .withSourceConfiguration(sourceConnectionConfig.getConfiguration())
        .withState(state)
        .withOperationSequence(listOf(normalizationOperation, customDbtOperation))
        .withWorkspaceId(workspaceId)

    return Pair(standardSync, replicationInput)
  }
}
