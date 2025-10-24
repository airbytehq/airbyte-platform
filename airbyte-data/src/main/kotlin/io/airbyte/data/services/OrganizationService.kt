/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.Organization
import java.io.IOException
import java.util.Optional
import java.util.UUID

/**
 * A service that manages organizations.
 */
interface OrganizationService {
  fun getOrganization(organizationId: UUID): Optional<Organization>

  fun getOrganizationForWorkspaceId(workspaceId: UUID): Optional<Organization>

  fun writeOrganization(organization: Organization)
}
