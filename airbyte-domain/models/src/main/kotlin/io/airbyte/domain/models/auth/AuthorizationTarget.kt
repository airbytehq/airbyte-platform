/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models.auth

import java.util.UUID

/** Saved ownership, including resources that have no workspace. */
sealed interface AuthorizationTarget {
  data class Workspace(
    val workspaceAndOrganization: WorkspaceAndOrganization,
  ) : AuthorizationTarget

  data class Organization(
    val organizationId: UUID,
  ) : AuthorizationTarget

  data object Global : AuthorizationTarget
}
