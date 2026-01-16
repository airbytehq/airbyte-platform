/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.config.Organization
import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.PermissionRepository
import io.airbyte.data.repositories.SsoConfigRepository
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.airbyte.data.services.shared.ResourcesByUserQueryPaginated
import io.micronaut.context.annotation.Primary
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

@Singleton
@Primary
class OrganizationServiceDataImpl(
  var organizationRepository: OrganizationRepository,
  var ssoConfigRepository: SsoConfigRepository,
  var permissionRepository: PermissionRepository,
) : OrganizationService {
  override fun getOrganization(organizationId: UUID): Optional<Organization> =
    organizationRepository.findById(organizationId).map { organization ->
      val ssoConfig = ssoConfigRepository.findByOrganizationId(organizationId)
      organization.toConfigModel().withSsoRealm(ssoConfig?.keycloakRealm)
    }

  override fun getOrganizationForWorkspaceId(workspaceId: UUID): Optional<Organization> =
    organizationRepository.findByWorkspaceId(workspaceId).map { organization ->
      val ssoConfig = ssoConfigRepository.findByOrganizationId(organization.id!!)
      organization.toConfigModel().withSsoRealm(ssoConfig?.keycloakRealm)
    }

  override fun getOrganizationForConnectionId(connectionId: UUID): Optional<Organization> =
    organizationRepository.findByConnectionId(connectionId).map { organization ->
      val ssoConfig = ssoConfigRepository.findByOrganizationId(organization.id!!)
      organization.toConfigModel().withSsoRealm(ssoConfig?.keycloakRealm)
    }

  override fun getDefaultOrganization(): Optional<Organization> = getOrganization(DEFAULT_ORGANIZATION_ID)

  override fun getOrganizationBySsoConfigRealm(ssoConfigRealm: String): Optional<Organization> =
    organizationRepository.findBySsoConfigRealm(ssoConfigRealm).map { organization ->
      val ssoConfig = ssoConfigRepository.findByOrganizationId(organization.id!!)
      organization.toConfigModel().withSsoRealm(ssoConfig?.keycloakRealm)
    }

  override fun listOrganizationsByUserId(
    userId: UUID,
    keyword: Optional<String>,
    includeDeleted: Boolean,
  ): List<Organization> =
    if (permissionRepository.isInstanceAdmin(userId)) {
      // For instance admins, query all organizations directly without permission checks
      organizationRepository
        .findAllWithSsoRealm(
          keyword.orElse(null),
          includeDeleted,
        ).map { it.toConfigModel() }
    } else {
      // For regular users, use the existing logic with permission checks
      organizationRepository
        .findByUserIdWithSsoRealm(
          userId,
          keyword.orElse(null),
          includeDeleted,
        ).map { it.toConfigModel() }
    }

  override fun listOrganizationsByUserIdPaginated(
    query: ResourcesByUserQueryPaginated,
    keyword: Optional<String>,
  ): List<Organization> =
    if (permissionRepository.isInstanceAdmin(query.userId)) {
      // For instance admins, query all organizations directly without permission checks
      organizationRepository
        .findAllPaginatedWithSsoRealm(
          keyword.orElse(null),
          query.includeDeleted,
          query.pageSize,
          query.rowOffset,
        ).map { it.toConfigModel() }
    } else {
      // For regular users, use the existing logic with permission checks
      organizationRepository
        .findByUserIdPaginatedWithSsoRealm(
          query.userId,
          keyword.orElse(null),
          query.includeDeleted,
          query.pageSize,
          query.rowOffset,
        ).map { it.toConfigModel() }
    }

  override fun writeOrganization(organization: Organization) {
    if (organization.organizationId == null || !organizationRepository.existsById(organization.organizationId)) {
      organizationRepository.save(organization.toEntity())
    } else {
      organizationRepository.update(organization.toEntity())
    }
  }

  override fun isMember(
    userId: UUID,
    organizationId: UUID,
  ): Boolean = permissionRepository.existsByUserIdAndOrganizationId(userId, organizationId)
}
