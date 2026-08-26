/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.sso

import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.RbacRolesEntitlement
import io.airbyte.domain.models.OrganizationId
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

class SsoRbacEntitlementCheckerTest {
  private val entitlementService = mockk<EntitlementService>()
  private val organizationId = OrganizationId(UUID.randomUUID())
  private val checker = SsoRbacEntitlementChecker(entitlementService)

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `returns definitive entitlement results unchanged`(isEntitled: Boolean) {
    val expected = EntitlementResult(RbacRolesEntitlement.featureId, isEntitled = isEntitled)
    every { entitlementService.checkEntitlement(organizationId, RbacRolesEntitlement) } returns expected

    assertEquals(expected, checker.checkWithinBudget(organizationId, Duration.ofSeconds(1)))
  }

  @Test
  fun `returns an indeterminate result when the entitlement check exceeds the budget`() {
    every { entitlementService.checkEntitlement(organizationId, RbacRolesEntitlement) } answers {
      Thread.sleep(30_000)
      EntitlementResult(RbacRolesEntitlement.featureId, isEntitled = true)
    }

    val startedAt = System.nanoTime()
    val result = checker.checkWithinBudget(organizationId, Duration.ofMillis(100))
    val elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt)

    assertFalse(result.isEntitled)
    assertFalse(result.isEntitlementCheckSuccessful)
    assertEquals(RbacRolesEntitlement.featureId, result.featureId)
    assertTrue(elapsed < 5_000, "the lookup should respect its budget, but took ${elapsed}ms")
  }

  @Test
  fun `returns an indeterminate result when the entitlement check throws`() {
    every { entitlementService.checkEntitlement(organizationId, RbacRolesEntitlement) } throws IllegalStateException("unavailable")

    val result = checker.checkWithinBudget(organizationId, Duration.ofSeconds(1))

    assertFalse(result.isEntitled)
    assertFalse(result.isEntitlementCheckSuccessful)
    assertEquals(RbacRolesEntitlement.featureId, result.featureId)
  }
}
