/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models.auth

import java.util.UUID

/** Non-secret saved workspace and organization IDs for permission checks. */
data class WorkspaceAndOrganization(
  val workspaceId: UUID,
  val organizationId: UUID,
)

class AuthorizationResourceNotFoundException : RuntimeException("Required authorization resource was not found")
