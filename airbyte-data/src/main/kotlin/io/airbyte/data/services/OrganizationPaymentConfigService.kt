/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.OrganizationPaymentConfig
import java.util.UUID

interface OrganizationPaymentConfigService {
  fun findByOrganizationId(organizationId: UUID): OrganizationPaymentConfig?

  fun findByPaymentProviderId(paymentProviderId: String): OrganizationPaymentConfig?

  fun savePaymentConfig(organizationPaymentConfig: OrganizationPaymentConfig)
}
