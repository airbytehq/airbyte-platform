/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.config.Organization
import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.SsoConfigRepository
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.impls.data.mappers.EntityOrganization
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
  ): List<Organization> {
    val organizations =
      organizationRepository.findByUserId(
        userId,
        keyword.orElse(null),
        includeDeleted,
      )

    if (organizations.isEmpty()) {
      return emptyList()
    }

    // Batch fetch SSO configs to avoid N+1 query problem
    val organizationIds = organizations.mapNotNull { it.id }
    val ssoConfigsByOrgId =
      ssoConfigRepository
        .findByOrganizationIdIn(organizationIds)
        .associateBy { it.organizationId }

    return organizations.map { organization ->
      val ssoConfig = ssoConfigsByOrgId[organization.id]
      organization.toConfigModel().withSsoRealm(ssoConfig?.keycloakRealm)
    }
  }

  override fun listOrganizationsByUserIdPaginated(
    query: ResourcesByUserQueryPaginated,
    keyword: Optional<String>,
  ): List<Organization> {
    val organizations =
      organizationRepository.findByUserIdPaginated(
        query.userId,
        keyword.orElse(null),
        query.includeDeleted,
        query.pageSize,
        query.rowOffset,
      )

    if (organizations.isEmpty()) {
      return emptyList()
    }

    // Batch fetch SSO configs to avoid N+1 query problem
    val organizationIds = organizations.mapNotNull { it.id }
    val ssoConfigsByOrgId =
      ssoConfigRepository
        .findByOrganizationIdIn(organizationIds)
        .associateBy { it.organizationId }

    return organizations.map { organization ->
      val ssoConfig = ssoConfigsByOrgId[organization.id]
      organization.toConfigModel().withSsoRealm(ssoConfig?.keycloakRealm)
    }
  }

  override fun writeOrganization(organization: Organization) {
    if (organization.organizationId == null || !organizationRepository.existsById(organization.organizationId)) {
      organizationRepository.save(organization.toEntity())
    } else {
      organizationRepository.update(organization.toEntity())
    }
  }
}
