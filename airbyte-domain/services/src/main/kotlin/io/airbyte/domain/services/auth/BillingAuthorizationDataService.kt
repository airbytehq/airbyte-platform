/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.data.services.OrganizationPaymentConfigService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class BillingAuthorizationDataService(
  private val data: AuthorizationDataService,
  private val paymentConfigs: OrganizationPaymentConfigService,
) {
  fun hasPaymentConfiguration(organizationId: UUID): Boolean {
    val config = paymentConfigs.findByOrganizationId(organizationId) ?: return false
    if (config.organizationId != organizationId) throw AuthorizationResourceNotFoundException()
    return true
  }

  fun backfillJobOwner(jobId: Long): WorkspaceAndOrganization {
    val owner = data.billingConnectionWorkspaceAndOrganization(data.replicationJobConnectionId(jobId))
    data.organization(owner.organizationId)
    check(hasPaymentConfiguration(owner.organizationId)) { "OrganizationPaymentConfig not found for organization ${owner.organizationId}" }
    return owner
  }
}
