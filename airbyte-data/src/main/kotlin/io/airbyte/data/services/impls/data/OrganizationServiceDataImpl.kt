/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.Organization
import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.impls.data.mappers.EntityOrganization
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.micronaut.context.annotation.Primary
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

@Singleton
@Primary
class OrganizationServiceDataImpl(
  var organizationRepository: OrganizationRepository,
) : OrganizationService {
  override fun getOrganization(organizationId: UUID): Optional<Organization> =
    organizationRepository.findById(organizationId).map(EntityOrganization::toConfigModel)

  override fun getOrganizationForWorkspaceId(workspaceId: UUID): Optional<Organization> =
    organizationRepository.findByWorkspaceId(workspaceId).map(EntityOrganization::toConfigModel)

  override fun writeOrganization(organization: Organization) {
    if (organization.organizationId == null || !organizationRepository.existsById(organization.organizationId)) {
      organizationRepository.save(organization.toEntity())
    } else {
      organizationRepository.update(organization.toEntity())
    }
  }
}
