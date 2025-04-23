/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.config.AttributeName
import io.airbyte.config.CustomerTier
import io.airbyte.config.Operator
import io.airbyte.data.repositories.entities.ConnectorRolloutFilters
import io.airbyte.data.repositories.entities.CustomerTierFilter
import io.airbyte.data.repositories.entities.JobBypassFilter
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ConnectorRolloutMapperTest {
  @Test
  fun `should map entity to config and back without loss`() {
    val entity =
      ConnectorRolloutFilters(
        jobBypassFilter = JobBypassFilter(name = "BYPASS_JOBS", value = true),
        customerTierFilters =
          listOf(
            CustomerTierFilter(
              name = "TIER",
              operator = "IN",
              value = listOf(CustomerTier.TIER_1.name, CustomerTier.TIER_2.name),
            ),
          ),
      )

    val config = entity.toConfigModel()
    val roundtrippedEntity = config.toEntity()
    val roundtrippedModel = roundtrippedEntity.toConfigModel()

    assertEquals(config, roundtrippedModel)
  }

  @Test
  fun `should correctly parse tier list from JSON`() {
    val entity =
      ConnectorRolloutFilters(
        jobBypassFilter = null,
        customerTierFilters =
          listOf(
            CustomerTierFilter(
              name = "TIER",
              operator = "IN",
              value = listOf(CustomerTier.TIER_2.name),
            ),
          ),
      )

    val config = entity.toConfigModel()
    val filter = config.customerTierFilters.first()

    assertEquals(AttributeName.TIER, filter.name)
    assertEquals(Operator.IN, filter.operator)
    assertEquals(listOf(CustomerTier.TIER_2), filter.value)
  }

  @Test
  fun `should serialize config back to expected JSON structure`() {
    val config =
      ConnectorRolloutFilters(
        jobBypassFilter = JobBypassFilter(name = "BYPASS_JOBS", value = true),
        customerTierFilters =
          listOf(
            CustomerTierFilter(
              name = "TIER",
              operator = "IN",
              value = listOf(CustomerTier.TIER_0.name, CustomerTier.TIER_1.name),
            ),
          ),
      ).toConfigModel()

    val entity = config.toEntity()
    val expectedTierJson = listOf("TIER_0", "TIER_1")
    val actualTierJson = entity.customerTierFilters.first().value
    assertEquals(expectedTierJson, actualTierJson)

    val expectedJobBypassValue = true
    val actualJobBypassValue = entity.jobBypassFilter!!.value
    assertEquals(expectedJobBypassValue, actualJobBypassValue)
  }

  @Test
  fun `customerTierFilter should throw on invalid tier value`() {
    val badTierJson = """{ "value": ["TIER_9"] }"""
    val entity =
      ConnectorRolloutFilters(
        jobBypassFilter = null,
        customerTierFilters =
          listOf(
            CustomerTierFilter(
              name = "TIER",
              operator = "IN",
              value = listOf(badTierJson),
            ),
          ),
      )

    val ex =
      assertThrows(RuntimeException::class.java) {
        entity.toConfigModel()
      }

    assertTrue(ex.message!!.contains("Failed to parse customer attribute"))
  }

  @Test
  fun `customerTierFilter should throw on missing value field`() {
    val missingValueJson = """{ }"""
    val entity =
      ConnectorRolloutFilters(
        jobBypassFilter = null,
        customerTierFilters =
          listOf(
            CustomerTierFilter(
              name = "TIER",
              operator = "IN",
              value = listOf(missingValueJson),
            ),
          ),
      )

    val ex =
      assertThrows(RuntimeException::class.java) {
        entity.toConfigModel()
      }

    assertTrue(ex.message!!.contains("Failed to parse customer attribute"))
  }

  @Test
  fun `customerTierFilter should throw on unknown attribute name`() {
    val entity =
      ConnectorRolloutFilters(
        jobBypassFilter = null,
        customerTierFilters =
          listOf(
            CustomerTierFilter(
              name = "banana",
              operator = "in",
              value = listOf(CustomerTier.TIER_2.name),
            ),
          ),
      )

    val ex =
      assertThrows(RuntimeException::class.java) {
        entity.toConfigModel()
      }

    assertTrue(ex.message!!.contains("Failed to parse customer attribute"))
  }

  @Test
  fun `customerTierFilter should throw on unknown operator`() {
    val entity =
      ConnectorRolloutFilters(
        jobBypassFilter = null,
        customerTierFilters =
          listOf(
            CustomerTierFilter(
              name = "TIER",
              operator = "explode",
              value = listOf(CustomerTier.TIER_1.name),
            ),
          ),
      )

    val ex =
      assertThrows(RuntimeException::class.java) {
        entity.toConfigModel()
      }

    assertTrue(ex.message!!.contains("Failed to parse customer attribute"))
  }
}
