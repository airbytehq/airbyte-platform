/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import org.junit.jupiter.api.Assertions.assertEquals
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
    assertEquals(EntitlementPlan.UNIFIED_TRIAL, EntitlementPlan.fromId("plan-airbyte-unified-trial"))
    assertEquals(EntitlementPlan.PLUS, EntitlementPlan.fromId("plan-airbyte-plus"))
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
}
