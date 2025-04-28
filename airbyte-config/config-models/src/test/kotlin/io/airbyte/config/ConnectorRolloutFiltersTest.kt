/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ConnectorRolloutFiltersTest {
  @Test
  fun `should match tier in list`() {
    val filter =
      CustomerTierFilter(
        operator = Operator.IN,
        value = listOf(CustomerTier.TIER_1, CustomerTier.TIER_2),
      )

    assertTrue(filter.evaluate(CustomerTier.TIER_2))
    assertTrue(filter.evaluate(CustomerTier.TIER_1))
  }

  @Test
  fun `should not match tier not in list`() {
    val filter =
      CustomerTierFilter(
        operator = Operator.IN,
        value = listOf(CustomerTier.TIER_2),
      )

    assertFalse(filter.evaluate(CustomerTier.TIER_0))
    assertFalse(filter.evaluate(CustomerTier.TIER_1))
  }

  @Test
  fun `should accept input when no filters are present`() {
    val filters = ConnectorRolloutFilters()

    val result = filters.customerTierFilters.all { it.evaluate(CustomerTier.TIER_2) }

    assertTrue(result)
  }

  @Test
  fun `should apply multiple filters using AND logic`() {
    val filter1 =
      CustomerTierFilter(
        operator = Operator.IN,
        value = listOf(CustomerTier.TIER_2),
      )
    val filter2 =
      CustomerTierFilter(
        operator = Operator.IN,
        value = listOf(CustomerTier.TIER_2, CustomerTier.TIER_1),
      )

    val filters =
      ConnectorRolloutFilters(
        customerTierFilters = listOf(filter1, filter2),
      )

    val result = filters.customerTierFilters.all { it.evaluate(CustomerTier.TIER_2) }
    assertTrue(result)

    val shouldFail = filters.customerTierFilters.all { it.evaluate(CustomerTier.TIER_1) }
    assertFalse(shouldFail)
  }
}
