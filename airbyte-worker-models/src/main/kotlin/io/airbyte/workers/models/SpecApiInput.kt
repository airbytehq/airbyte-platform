/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.models

import java.util.UUID

data class SpecApiInput(
  val requestId: String,
  val commandId: String?,
  val actorDefinitionId: UUID?,
  val dockerImage: String?,
  val dockerImageTag: String,
  val workspaceId: UUID,
)
