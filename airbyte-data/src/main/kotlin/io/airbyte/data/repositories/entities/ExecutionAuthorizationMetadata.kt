/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.core.annotation.Nullable
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.util.UUID

/** Saved selectors only. Execution inputs and output are deliberately absent. */
@MappedEntity("commands")
data class CommandAuthorizationMetadata(
  @field:Id val id: String,
  val workloadId: String,
  val workspaceId: UUID,
  val organizationId: UUID,
  val commandType: String,
)

@MappedEntity("workload")
data class WorkloadAuthorizationMetadata(
  @field:Id val id: String,
  @Nullable val workspaceId: UUID?,
  @Nullable val organizationId: UUID?,
  val type: String,
)
