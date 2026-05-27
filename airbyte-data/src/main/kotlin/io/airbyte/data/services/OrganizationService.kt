/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.Organization
import io.airbyte.data.services.shared.ResourcesByUserQueryPaginated
import io.opentelemetry.instrumentation.annotations.WithSpan
import java.util.Optional
import java.util.UUID

/**
 * A service that manages organizations.
 */
interface OrganizationService {
  @WithSpan
  fun getOrganization(organizationId: UUID): Optional<Organization>

  fun getOrganizationForWorkspaceId(workspaceId: UUID): Optional<Organization>

  fun getOrganizationForConnectionId(connectionId: UUID): Optional<Organization>

  fun getDefaultOrganization(): Optional<Organization>

  fun getOrganizationBySsoConfigRealm(ssoConfigRealm: String): Optional<Organization>

  /**
   * List organizations accessible to a user, excluding agentic (ADP-managed) organizations
   * for non-instance-admins. Used by Data Replication org-list paths in the Airbyte Cloud
   * webapp where ADP-managed orgs must stay hidden.
   *
   * Public-API / Embedded-API callers must use [listAllOrganizationsByUserId] instead so
   * that ADP customers (Sonar etc.) can still see their own agentic orgs.
   */
  fun listOrganizationsByUserId(
    userId: UUID,
    keyword: Optional<String>,
    includeDeleted: Boolean = false,
  ): List<Organization>

  /**
   * Paginated variant of [listOrganizationsByUserId]. Same agentic-filter semantics.
   */
  fun listOrganizationsByUserIdPaginated(
    query: ResourcesByUserQueryPaginated,
    keyword: Optional<String>,
  ): List<Organization>

  /**
   * List every organization accessible to a user, including agentic (ADP-managed) ones.
   * Used by the public API and Embedded API paths, where the caller (e.g. Sonar) is itself
   * the ADP product and must continue to see its own agentic orgs.
   *
   * Internal Data Replication callers must use [listOrganizationsByUserId] instead.
   */
  fun listAllOrganizationsByUserId(
    userId: UUID,
    keyword: Optional<String>,
    includeDeleted: Boolean = false,
  ): List<Organization>

  /**
   * Paginated variant of [listAllOrganizationsByUserId]. Same unfiltered semantics.
   */
  fun listAllOrganizationsByUserIdPaginated(
    query: ResourcesByUserQueryPaginated,
    keyword: Optional<String>,
  ): List<Organization>

  fun writeOrganization(organization: Organization)

  fun setOrganizationAgenticStatus(
    organizationId: UUID,
    isAgentic: Boolean,
  ): Optional<Organization>

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
