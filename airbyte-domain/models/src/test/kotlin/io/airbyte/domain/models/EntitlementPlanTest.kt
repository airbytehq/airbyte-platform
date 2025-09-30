/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

internal class EntitlementPlanTest {
  @Test
  fun `fromId returns correct plan for valid ID`() {
    assertEquals(EntitlementPlan.CORE, EntitlementPlan.fromId("plan-airbyte-core"))
    assertEquals(EntitlementPlan.SME, EntitlementPlan.fromId("plan-airbyte-sme"))
    assertEquals(EntitlementPlan.STANDARD, EntitlementPlan.fromId("plan-airbyte-standard"))
    assertEquals(EntitlementPlan.STANDARD_TRIAL, EntitlementPlan.fromId("plan-airbyte-standard-trial"))
    assertEquals(EntitlementPlan.FLEX, EntitlementPlan.fromId("plan-airbyte-flex"))
    assertEquals(EntitlementPlan.PRO, EntitlementPlan.fromId("plan-airbyte-pro"))
    assertEquals(EntitlementPlan.PRO_TRIAL, EntitlementPlan.fromId("plan-airbyte-unified-trial"))
  }

  @Test
  fun `fromId throws IllegalArgumentException for invalid ID`() {
    val exception =
      assertThrows<IllegalArgumentException> {
        EntitlementPlan.fromId("invalid-plan-id")
      }
    assertEquals("No EntitlementPlan with id=invalid-plan-id", exception.message)
  }

  @Test
  fun `fromId throws IllegalArgumentException for empty ID`() {
    val exception =
      assertThrows<IllegalArgumentException> {
        EntitlementPlan.fromId("")
      }
    assertEquals("No EntitlementPlan with id=", exception.message)
  }

  @Test
  fun `fromId throws IllegalArgumentException for null ID`() {
    val exception =
      assertThrows<IllegalArgumentException> {
        EntitlementPlan.fromId(null.toString())
      }
    assertEquals("No EntitlementPlan with id=null", exception.message)
  }

  @Test
  fun `isGreaterOrEqualTo works correctly for same value plans`() {
    // Value 0 plans
    assertTrue(EntitlementPlan.CORE.isGreaterOrEqualTo(EntitlementPlan.SME))
    assertTrue(EntitlementPlan.SME.isGreaterOrEqualTo(EntitlementPlan.CORE))
    assertTrue(EntitlementPlan.STANDARD.isGreaterOrEqualTo(EntitlementPlan.STANDARD_TRIAL))
    assertTrue(EntitlementPlan.STANDARD_TRIAL.isGreaterOrEqualTo(EntitlementPlan.STANDARD))

    // Value 2 plans
    assertTrue(EntitlementPlan.PRO.isGreaterOrEqualTo(EntitlementPlan.PRO_TRIAL))
    assertTrue(EntitlementPlan.PRO_TRIAL.isGreaterOrEqualTo(EntitlementPlan.PRO))
  }

  @Test
  fun `isGreaterOrEqualTo works correctly for different value plans`() {
    // PRO (value 2) >= FLEX (value 1)
    assertTrue(EntitlementPlan.PRO.isGreaterOrEqualTo(EntitlementPlan.FLEX))
    assertTrue(EntitlementPlan.PRO_TRIAL.isGreaterOrEqualTo(EntitlementPlan.FLEX))

    // FLEX (value 1) >= STANDARD (value 0)
    assertTrue(EntitlementPlan.FLEX.isGreaterOrEqualTo(EntitlementPlan.STANDARD))
    assertTrue(EntitlementPlan.FLEX.isGreaterOrEqualTo(EntitlementPlan.CORE))

    // PRO (value 2) >= STANDARD (value 0)
    assertTrue(EntitlementPlan.PRO.isGreaterOrEqualTo(EntitlementPlan.STANDARD))
    assertTrue(EntitlementPlan.PRO_TRIAL.isGreaterOrEqualTo(EntitlementPlan.CORE))
  }

  @Test
  fun `isGreaterOrEqualTo works correctly for self comparison`() {
    EntitlementPlan.entries.forEach { plan ->
      assertTrue(plan.isGreaterOrEqualTo(plan), "$plan should be greater or equal to itself")
    }
  }

  @Test
  fun `isGreaterOrEqualTo returns false when comparing lower to higher value plans`() {
    // STANDARD (value 0) < FLEX (value 1)
    assertFalse(EntitlementPlan.STANDARD.isGreaterOrEqualTo(EntitlementPlan.FLEX))
    assertFalse(EntitlementPlan.CORE.isGreaterOrEqualTo(EntitlementPlan.FLEX))

    // FLEX (value 1) < PRO (value 2)
    assertFalse(EntitlementPlan.FLEX.isGreaterOrEqualTo(EntitlementPlan.PRO))
    assertFalse(EntitlementPlan.FLEX.isGreaterOrEqualTo(EntitlementPlan.PRO_TRIAL))

    // STANDARD (value 0) < PRO (value 2)
    assertFalse(EntitlementPlan.STANDARD.isGreaterOrEqualTo(EntitlementPlan.PRO))
    assertFalse(EntitlementPlan.CORE.isGreaterOrEqualTo(EntitlementPlan.PRO_TRIAL))
  }

  @Test
  fun `isLessThan works correctly for different value plans`() {
    // STANDARD (value 0) < FLEX (value 1)
    assertTrue(EntitlementPlan.STANDARD.isLessThan(EntitlementPlan.FLEX))
    assertTrue(EntitlementPlan.CORE.isLessThan(EntitlementPlan.FLEX))

    // FLEX (value 1) < PRO (value 2)
    assertTrue(EntitlementPlan.FLEX.isLessThan(EntitlementPlan.PRO))
    assertTrue(EntitlementPlan.FLEX.isLessThan(EntitlementPlan.PRO_TRIAL))

    // STANDARD (value 0) < PRO (value 2)
    assertTrue(EntitlementPlan.STANDARD.isLessThan(EntitlementPlan.PRO))
    assertTrue(EntitlementPlan.CORE.isLessThan(EntitlementPlan.PRO_TRIAL))
  }

  @Test
  fun `isLessThan returns false for same value plans`() {
    // Value 0 plans
    assertFalse(EntitlementPlan.CORE.isLessThan(EntitlementPlan.SME))
    assertFalse(EntitlementPlan.SME.isLessThan(EntitlementPlan.CORE))
    assertFalse(EntitlementPlan.STANDARD.isLessThan(EntitlementPlan.STANDARD_TRIAL))
    assertFalse(EntitlementPlan.STANDARD_TRIAL.isLessThan(EntitlementPlan.STANDARD))

    // Value 2 plans
    assertFalse(EntitlementPlan.PRO.isLessThan(EntitlementPlan.PRO_TRIAL))
    assertFalse(EntitlementPlan.PRO_TRIAL.isLessThan(EntitlementPlan.PRO))
  }

  @Test
  fun `isLessThan returns false for self comparison`() {
    EntitlementPlan.entries.forEach { plan ->
      assertFalse(plan.isLessThan(plan), "$plan should not be less than itself")
    }
  }

  @Test
  fun `isLessThan returns false when comparing higher to lower value plans`() {
    // PRO (value 2) > FLEX (value 1)
    assertFalse(EntitlementPlan.PRO.isLessThan(EntitlementPlan.FLEX))
    assertFalse(EntitlementPlan.PRO_TRIAL.isLessThan(EntitlementPlan.FLEX))

    // FLEX (value 1) > STANDARD (value 0)
    assertFalse(EntitlementPlan.FLEX.isLessThan(EntitlementPlan.STANDARD))
    assertFalse(EntitlementPlan.FLEX.isLessThan(EntitlementPlan.CORE))

    // PRO (value 2) > STANDARD (value 0)
    assertFalse(EntitlementPlan.PRO.isLessThan(EntitlementPlan.STANDARD))
    assertFalse(EntitlementPlan.PRO_TRIAL.isLessThan(EntitlementPlan.CORE))
  }

  @Test
  fun `supportedOrbPlanExternalIds mapping is correct`() {
    val expectedMapping =
      mapOf(
        SupportedOrbPlan.CLOUD_LEGACY to EntitlementPlan.STANDARD,
        SupportedOrbPlan.CLOUD_SELF_SERVE_ANNUAL to EntitlementPlan.STANDARD,
        SupportedOrbPlan.CLOUD_SELF_SERVE_MONTHLY to EntitlementPlan.STANDARD,
        SupportedOrbPlan.PRO to EntitlementPlan.PRO,
        SupportedOrbPlan.PRO_LEGACY to EntitlementPlan.PRO,
        SupportedOrbPlan.PARTNER to EntitlementPlan.PARTNER,
      )
    assertEquals(expectedMapping, EntitlementPlan.supportedOrbPlanNameOverrides)
  }

  @Test
  fun `comparison methods are consistent with each other`() {
    val plans = EntitlementPlan.entries

    plans.forEach { plan1 ->
      plans.forEach { plan2 ->
        val isGreaterOrEqual = plan1.isGreaterOrEqualTo(plan2)
        val isLess = plan1.isLessThan(plan2)

        // If plan1 >= plan2, then plan1 should NOT be < plan2 (unless they're equal)
        if (isGreaterOrEqual && plan1.value > plan2.value) {
          assertFalse(isLess, "$plan1 cannot be both >= and < $plan2")
        }

        // If plan1 < plan2, then plan1 should NOT be >= plan2
        if (isLess) {
          assertFalse(isGreaterOrEqual, "$plan1 cannot be both < and >= $plan2")
        }

        // If plans have same value, they should be >= but not <
        if (plan1.value == plan2.value) {
          assertTrue(isGreaterOrEqual, "$plan1 should be >= $plan2 when they have same value")
          assertFalse(isLess, "$plan1 should not be < $plan2 when they have same value")
        }
      }
    }
  }
}
