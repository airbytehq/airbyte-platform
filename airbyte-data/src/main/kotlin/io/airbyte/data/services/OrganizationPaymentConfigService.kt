package io.airbyte.data.services

import io.airbyte.config.OrganizationPaymentConfig
import java.util.UUID

interface OrganizationPaymentConfigService {
  fun findByOrganizationId(organizationId: UUID): OrganizationPaymentConfig?

  fun findByPaymentProviderId(paymentProviderId: String): OrganizationPaymentConfig?

  fun savePaymentConfig(organizationPaymentConfig: OrganizationPaymentConfig)
}
