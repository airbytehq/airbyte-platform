/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.OrganizationEmailDomain

interface OrganizationEmailDomainService {
  fun findByEmailDomain(emailDomain: String): List<OrganizationEmailDomain>
}
