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
  @Throws(IOException::class)
  fun getOrganization(organizationId: UUID): Optional<Organization>

  @Throws(IOException::class)
  fun getOrganizationForWorkspaceId(workspaceId: UUID): Optional<Organization>

  @Throws(IOException::class)
  fun writeOrganization(organization: Organization)
}
