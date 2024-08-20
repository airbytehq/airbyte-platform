package io.airbyte.data.services.impls.data

import io.airbyte.config.OrganizationEmailDomain
import io.airbyte.data.repositories.OrganizationEmailDomainRepository
import io.airbyte.data.services.OrganizationEmailDomainService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import jakarta.inject.Singleton

@Singleton
class OrganizationEmailDomainServiceDataImpl(private val repository: OrganizationEmailDomainRepository) : OrganizationEmailDomainService {
  override fun findByEmailDomain(emailDomain: String): List<OrganizationEmailDomain> {
    return repository.findByEmailDomain(emailDomain).map { it.toConfigModel() }
  }
}
