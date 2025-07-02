/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.PlatformLlmSyncJobFailureExplanation
import io.airbyte.commons.server.handlers.EntitlementHandler
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID

internal class EntitlementHandlerTest {
  private val entitlementService = mockk<EntitlementService>()
  private val handler = EntitlementHandler(entitlementService)

  private val organizationId = UUID.randomUUID()

  @Test
  fun `isEntitled returns result from client for known entitlement`() {
    val expectedEntitlement = PlatformLlmSyncJobFailureExplanation
    val expectedResult = EntitlementResult(expectedEntitlement.featureId, true, null)

    every { entitlementService.checkEntitlement(organizationId, expectedEntitlement) } returns expectedResult

    val result = handler.isEntitled(organizationId, expectedEntitlement.featureId)

    assertEquals(expectedResult, result)
    verify(exactly = 1) { entitlementService.checkEntitlement(organizationId, expectedEntitlement) }
  }

  @Test
  fun `isEntitled throws when given unknown entitlementId`() {
    val unknownEntitlementId = "feature-nonexistent"

    val exception =
      assertThrows(IllegalArgumentException::class.java) {
        handler.isEntitled(organizationId, unknownEntitlementId)
      }

    assertTrue(exception.message!!.contains(unknownEntitlementId))
  }
}
