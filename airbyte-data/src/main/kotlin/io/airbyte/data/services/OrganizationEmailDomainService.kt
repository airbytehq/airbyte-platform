/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.OrganizationEmailDomain
import java.util.UUID

interface OrganizationEmailDomainService {
  fun findByEmailDomain(emailDomain: String): List<OrganizationEmailDomain>

  fun createEmailDomain(emailDomainConfig: OrganizationEmailDomain)

  fun deleteAllEmailDomains(organizationId: UUID)

  fun findByOrganizationId(organizationId: UUID): List<OrganizationEmailDomain>
}
