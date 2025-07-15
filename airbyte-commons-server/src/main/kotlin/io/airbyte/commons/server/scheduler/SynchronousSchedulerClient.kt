/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.scheduler

import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.DestinationConnection
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.WorkloadPriority
import io.airbyte.protocol.models.v0.ConnectorSpecification
import java.io.IOException
import java.util.UUID

/**
 * Exposes a way of executing short-lived jobs as RPC calls. Blocks until the job completes. No
 * metadata will be stored in the Jobs table for jobs triggered via this client.
 */
interface SynchronousSchedulerClient {
  @Throws(IOException::class)
  fun createSourceCheckConnectionJob(
    source: SourceConnection,
    sourceVersion: ActorDefinitionVersion,
    isCustomConnector: Boolean,
    actorDefinitionResourceRequirements: ResourceRequirements?,
  ): SynchronousResponse<StandardCheckConnectionOutput>

  @Throws(IOException::class)
  fun createDestinationCheckConnectionJob(
    destination: DestinationConnection,
    destinationVersion: ActorDefinitionVersion,
    isCustomConnector: Boolean,
    actorDefinitionResourceRequirements: ResourceRequirements?,
  ): SynchronousResponse<StandardCheckConnectionOutput>

  @Throws(IOException::class)
  fun createDiscoverSchemaJob(
    source: SourceConnection,
    sourceVersion: ActorDefinitionVersion,
    isCustomConnector: Boolean,
    actorDefinitionResourceRequirements: ResourceRequirements?,
    priority: WorkloadPriority?,
  ): SynchronousResponse<UUID>

  @Throws(IOException::class)
  fun createDestinationDiscoverJob(
    destination: DestinationConnection,
    destinationDefinition: StandardDestinationDefinition,
    destinationVersion: ActorDefinitionVersion,
  ): SynchronousResponse<UUID>

  @Throws(IOException::class)
  fun createGetSpecJob(
    dockerImage: String,
    isCustomConnector: Boolean,
    workspaceId: UUID,
  ): SynchronousResponse<ConnectorSpecification>
}
