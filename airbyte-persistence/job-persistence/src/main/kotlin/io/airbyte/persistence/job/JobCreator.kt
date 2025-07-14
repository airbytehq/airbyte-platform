/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.DestinationConnection
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.persistence.domain.StreamRefresh
import jakarta.annotation.Nullable
import java.io.IOException
import java.util.Optional
import java.util.UUID

/**
 * Abstraction to hide implementation of enqueuing a job.
 */
interface JobCreator {
  /**
   * Create a sync job.
   *
   * @param source db model representing where data comes from
   * @param destination db model representing where data goes
   * @param standardSync sync options
   * @param sourceDockerImage docker image to use for the source
   * @param destinationDockerImage docker image to use for the destination
   * @param workspaceId workspace id
   * @return the new job if no other conflicting job was running, otherwise empty
   * @throws IOException if something wrong happens
   */
  @Throws(IOException::class)
  fun createSyncJob(
    source: SourceConnection,
    destination: DestinationConnection,
    standardSync: StandardSync,
    sourceDockerImage: String,
    sourceDockerImageIsDefault: Boolean,
    sourceProtocolVersion: Version,
    destinationDockerImage: String,
    destinationDockerImageIsDefault: Boolean,
    destinationProtocolVersion: Version,
    standardSyncOperations: List<StandardSyncOperation>,
    @Nullable webhookOperationConfigs: JsonNode?,
    sourceDefinition: StandardSourceDefinition,
    destinationDefinition: StandardDestinationDefinition,
    sourceDefinitionVersion: ActorDefinitionVersion,
    destinationDefinitionVersion: ActorDefinitionVersion,
    workspaceId: UUID,
    isScheduled: Boolean,
  ): Optional<Long>

  /**
   * Create a reset job.
   *
   * @param destination db model representing where data goes
   * @param standardSync sync options
   * @param destinationDockerImage docker image to use for the destination
   * @param streamsToReset select which streams should be reset
   * @return the new job if no other conflicting job was running, otherwise empty
   * @throws IOException if something wrong happens
   */
  @Throws(IOException::class)
  fun createResetConnectionJob(
    destination: DestinationConnection,
    standardSync: StandardSync,
    destinationDefinition: StandardDestinationDefinition,
    destinationDefinitionVersion: ActorDefinitionVersion,
    destinationDockerImage: String,
    destinationProtocolVersion: Version,
    isCustom: Boolean,
    standardSyncOperations: List<StandardSyncOperation>,
    streamsToReset: List<StreamDescriptor>,
    workspaceId: UUID,
  ): Optional<Long>

  @Throws(IOException::class)
  fun createRefreshConnection(
    source: SourceConnection,
    destination: DestinationConnection,
    standardSync: StandardSync,
    sourceDockerImageName: String,
    sourceProtocolVersion: Version,
    destinationDockerImageName: String,
    destinationProtocolVersion: Version,
    standardSyncOperations: List<StandardSyncOperation>,
    @Nullable webhookOperationConfigs: JsonNode?,
    sourceDefinition: StandardSourceDefinition,
    destinationDefinition: StandardDestinationDefinition,
    sourceDefinitionVersion: ActorDefinitionVersion,
    destinationDefinitionVersion: ActorDefinitionVersion,
    workspaceId: UUID,
    streamsToRefresh: List<StreamRefresh>,
  ): Optional<Long>
}
