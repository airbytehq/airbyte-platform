/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.airbyte.config.AttributeName
import io.airbyte.config.CustomerTier
import io.airbyte.config.Operator
import io.airbyte.data.repositories.entities.ConnectorRolloutFilters
import io.airbyte.data.repositories.entities.OrganizationCustomerAttributeFilter
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ConnectorRolloutMapperTest {
  private val objectMapper = jacksonObjectMapper()

  @Test
  fun `should map entity to config and back without loss`() {
    val entity =
      ConnectorRolloutFilters(
        organizationCustomerAttributeFilters =
          listOf(
            OrganizationCustomerAttributeFilter(
              name = "tier",
              operator = "in",
              value = objectMapper.readTree("""{ "value": ["${CustomerTier.TIER_1}", "${CustomerTier.TIER_2}"] }"""),
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
        organizationCustomerAttributeFilters =
          listOf(
            OrganizationCustomerAttributeFilter(
              name = "tier",
              operator = "in",
              value = objectMapper.readTree("""{ "value": ["${CustomerTier.TIER_2}"] }"""),
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
        organizationCustomerAttributeFilters =
          listOf(
            OrganizationCustomerAttributeFilter(
              name = "tier",
              operator = "in",
              value = objectMapper.readTree("""{ "value": ["${CustomerTier.TIER_0}", "${CustomerTier.TIER_1}"] }"""),
            ),
          ),
      ).toConfigModel()

    val entity = config.toEntity()
    val expectedJson = objectMapper.readTree("""{ "value": ["TIER_0", "${CustomerTier.TIER_1}"] }""")
    val actualJson = entity.organizationCustomerAttributeFilters.first().value

    assertEquals(expectedJson, actualJson)
  }

  @Test
  fun `should throw on invalid tier value`() {
    val badTierJson = """{ "value": ["TIER_9"] }"""
    val entity =
      ConnectorRolloutFilters(
        organizationCustomerAttributeFilters =
          listOf(
            OrganizationCustomerAttributeFilter(
              name = "tier",
              operator = "in",
              value = objectMapper.readTree(badTierJson),
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
  fun `should throw on missing value field`() {
    val missingValueJson = """{ }"""
    val entity =
      ConnectorRolloutFilters(
        organizationCustomerAttributeFilters =
          listOf(
            OrganizationCustomerAttributeFilter(
              name = "tier",
              operator = "in",
              value = objectMapper.readTree(missingValueJson),
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
  fun `should throw on unknown attribute name`() {
    val json = """{ "value": ["${CustomerTier.TIER_2}"] }"""
    val entity =
      ConnectorRolloutFilters(
        organizationCustomerAttributeFilters =
          listOf(
            OrganizationCustomerAttributeFilter(
              name = "banana",
              operator = "in",
              value = objectMapper.readTree(json),
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
  fun `should throw on unknown operator`() {
    val json = """{ "value": ["${CustomerTier.TIER_1}"] }"""
    val entity =
      ConnectorRolloutFilters(
        organizationCustomerAttributeFilters =
          listOf(
            OrganizationCustomerAttributeFilter(
              name = "tier",
              operator = "explode",
              value = objectMapper.readTree(json),
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
