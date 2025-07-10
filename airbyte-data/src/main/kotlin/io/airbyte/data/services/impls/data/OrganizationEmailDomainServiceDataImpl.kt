/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.OrganizationEmailDomain
import io.airbyte.data.repositories.OrganizationEmailDomainRepository
import io.airbyte.data.services.OrganizationEmailDomainService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class OrganizationEmailDomainServiceDataImpl(
  private val repository: OrganizationEmailDomainRepository,
) : OrganizationEmailDomainService {
  override fun findByEmailDomain(emailDomain: String): List<OrganizationEmailDomain> =
    repository.findByEmailDomain(emailDomain).map { it.toConfigModel() }

  override fun createEmailDomain(emailDomainConfig: OrganizationEmailDomain) {
    repository.save(
      io.airbyte.data.repositories.entities.OrganizationEmailDomain(
        organizationId = emailDomainConfig.organizationId,
        emailDomain = emailDomainConfig.emailDomain,
      ),
    )
  }

  override fun deleteAllEmailDomains(organizationId: UUID) = repository.deleteByOrganizationId(organizationId)

  override fun findByOrganizationId(organizationId: UUID): List<OrganizationEmailDomain> =
    repository.findByOrganizationId(organizationId).map {
      it.toConfigModel()
    }
}
