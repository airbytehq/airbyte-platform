/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models.fusion

import java.time.Instant
import java.util.UUID

enum class EnablementActorType {
  SOURCE,
  DESTINATION,
}

data class ActorEnablementFlags(
  val enableAgentAccess: Boolean = false,
  val enableIndexing: Boolean = false,
  val enableBackfill: Boolean = false,
  val backfillStartTime: Instant? = null,
)

data class ActorEnablementState(
  val organizationId: UUID,
  val workspaceId: UUID,
  val actorId: UUID,
  val actorType: EnablementActorType,
  val flags: ActorEnablementFlags,
)

data class SyncEnablementState(
  val inputPayload: String,
  val sourceSearchIndexing: Boolean,
  val destinationSearchIndexing: Boolean,
)
