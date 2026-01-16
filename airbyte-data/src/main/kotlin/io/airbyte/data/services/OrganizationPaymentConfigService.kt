/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.OrganizationPaymentConfig
import java.util.UUID

interface OrganizationPaymentConfigService {
  fun findByOrganizationId(organizationId: UUID): OrganizationPaymentConfig?

  fun findByPaymentProviderId(paymentProviderId: String): OrganizationPaymentConfig?

  fun savePaymentConfig(organizationPaymentConfig: OrganizationPaymentConfig)

  fun saveDefaultPaymentConfig(organizationId: UUID) {
    val paymentConfig =
      OrganizationPaymentConfig()
        .withOrganizationId(organizationId)
        .withPaymentStatus(OrganizationPaymentConfig.PaymentStatus.UNINITIALIZED)
        .withSubscriptionStatus(OrganizationPaymentConfig.SubscriptionStatus.PRE_SUBSCRIPTION)
    savePaymentConfig(paymentConfig)
  }
}
