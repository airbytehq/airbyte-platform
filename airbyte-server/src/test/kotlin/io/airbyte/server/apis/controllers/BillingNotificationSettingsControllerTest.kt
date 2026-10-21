/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.server.generated.models.BillingNotificationSettingsUpdateRequestBody
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.math.BigDecimal
import java.util.UUID

class BillingNotificationSettingsControllerTest {
  private val controller = BillingNotificationSettingsController()

  @Test
  fun `billing notification settings are unavailable in OSS`() {
    val organizationId = UUID.randomUUID()
    assertThrows<ApiNotImplementedInOssProblem> {
      controller.getBillingNotificationSettings(OrganizationIdRequestBody(organizationId = organizationId))
    }
    assertThrows<ApiNotImplementedInOssProblem> {
      controller.updateBillingNotificationSettings(
        BillingNotificationSettingsUpdateRequestBody(
          organizationId = organizationId,
          syncCreditThreshold = BigDecimal.ONE,
          invoiceAmountThreshold = BigDecimal.TEN,
        ),
      )
    }
  }
}
