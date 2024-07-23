package io.airbyte.data.services

import io.airbyte.config.OrganizationEmailDomain

interface OrganizationEmailDomainService {
  fun findByEmailDomain(emailDomain: String): List<OrganizationEmailDomain>
}
