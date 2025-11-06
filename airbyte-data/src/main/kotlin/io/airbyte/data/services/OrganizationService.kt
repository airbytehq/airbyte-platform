/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.Organization
import io.airbyte.data.services.shared.ResourcesByUserQueryPaginated
import java.io.IOException
import java.util.Optional
import java.util.UUID

/**
 * A service that manages organizations.
 */
interface OrganizationService {
  fun getOrganization(organizationId: UUID): Optional<Organization>

  fun getOrganizationForWorkspaceId(workspaceId: UUID): Optional<Organization>

  fun getOrganizationForConnectionId(connectionId: UUID): Optional<Organization>

  fun getDefaultOrganization(): Optional<Organization>

  fun getOrganizationBySsoConfigRealm(ssoConfigRealm: String): Optional<Organization>

  fun listOrganizationsByUserId(
    userId: UUID,
    keyword: Optional<String>,
    includeDeleted: Boolean = false,
  ): List<Organization>

  fun listOrganizationsByUserIdPaginated(
    query: ResourcesByUserQueryPaginated,
    keyword: Optional<String>,
  ): List<Organization>

  fun writeOrganization(organization: Organization)

  /**
   * Check if a user is a member of an organization.
   *
   * @param userId the user ID to check
   * @param organizationId the organization ID to check
   * @return true if the user has any permission in the organization, false otherwise
   */
  fun isMember(
    userId: UUID,
    organizationId: UUID,
  ): Boolean
}
