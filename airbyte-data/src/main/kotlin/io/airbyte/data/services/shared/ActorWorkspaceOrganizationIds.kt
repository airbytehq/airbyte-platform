/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import java.util.UUID

/**
 * A record that represents IDs for an actor and its associated workspace and organization.
 *
 * @param actorId - actor ID
 * @param workspaceId - workspace ID
 * @param organizationId - organization ID
 */
@JvmRecord
data class ActorWorkspaceOrganizationIds(
  val actorId: UUID,
  val workspaceId: UUID,
  val organizationId: UUID?,
)
