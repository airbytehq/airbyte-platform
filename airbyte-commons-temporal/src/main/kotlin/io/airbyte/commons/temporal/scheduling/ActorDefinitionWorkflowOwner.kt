/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling

import io.airbyte.config.ActorType
import java.util.Optional
import java.util.UUID

data class ActorDefinitionWorkflowOwner(
  val workspaceId: UUID,
  val actorType: ActorType,
  val actorDefinitionId: Optional<UUID>,
)
