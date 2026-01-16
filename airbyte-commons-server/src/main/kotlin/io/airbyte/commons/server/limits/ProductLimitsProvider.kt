/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.limits

import java.util.UUID

interface ProductLimitsProvider {
  class WorkspaceLimits(
    val maxConnections: Long?,
    val maxSourcesOfSameType: Long?,
    val maxDestinations: Long?,
  )

  class OrganizationLimits(
    val maxWorkspaces: Long?,
    val maxUsers: Long?,
  )

  enum class Dimension {
    CONNECTIONS,
    DISTINCT_SOURCES,
    DESTINATIONS,
    WORKSPACES,
    USERS,
  }

  enum class EnforcementLevel {
    STRICT,
    ADVISORY,
  }

  fun getLimitForWorkspace(workspaceId: UUID): WorkspaceLimits

  fun getLimitForOrganization(organizationId: UUID): OrganizationLimits
}
