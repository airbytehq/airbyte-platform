/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models.billing

import io.airbyte.domain.models.OrganizationId
import java.math.BigDecimal

/**
 * Billing alert settings for one organization. A null [recipientEmail] means alerts go to the
 * payment provider email. A zero threshold disables that alert.
 *
 * [configured] is false when the organization has no row. That is a normal state for a settings
 * page, not an error, so reads return [unconfigured] instead of a 404. The SCIM configuration read
 * makes the same choice with its NOT_CONFIGURED status; the payment config read returns 404
 * because an organization without one never entered billing.
 */
data class BillingNotificationSettings(
  val organizationId: OrganizationId,
  val configured: Boolean,
  val recipientEmail: String?,
  val syncCreditThreshold: BigDecimal,
  val invoiceAmountThreshold: BigDecimal,
) {
  companion object {
    fun unconfigured(organizationId: OrganizationId): BillingNotificationSettings =
      BillingNotificationSettings(
        organizationId = organizationId,
        configured = false,
        recipientEmail = null,
        syncCreditThreshold = BigDecimal.ZERO,
        invoiceAmountThreshold = BigDecimal.ZERO,
      )
  }
}

class BillingNotificationSettingsValidationException(
  message: String,
) : RuntimeException(message)
