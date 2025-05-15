/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.context

import java.util.UUID

/**
 * Context of a Replication.
 * <p>
 * Contains the relevant ids of the object involved in a sync. This is not the place to hold
 * configuration.
 *
 * @param connectionId The connection ID associated with the sync.
 * @param sourceId The source ID associated with the sync.
 * @param destinationId The destination ID associated with the sync.
 * @param jobId The job ID associated with the sync.
 * @param attempt The attempt number of the sync.
 * @param workspaceId The workspace ID associated with the sync.
 * @param sourceImage The name and version of the source image.
 * @param destinationImage The name and version of the destination image.
 * @param sourceDefinitionId The source definition ID associated with the sync
 * @param destinationDefinitionId The source definition ID associated with the sync
 */
data class ReplicationContext(
  val isReset: Boolean,
  val connectionId: UUID,
  val sourceId: UUID,
  val destinationId: UUID,
  val jobId: Long,
  val attempt: Int,
  val workspaceId: UUID,
  val sourceImage: String,
  val destinationImage: String,
  val sourceDefinitionId: UUID,
  val destinationDefinitionId: UUID,
)
