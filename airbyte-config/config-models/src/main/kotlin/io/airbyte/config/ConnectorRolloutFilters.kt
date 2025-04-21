/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

sealed interface Expression<T> {
  fun evaluate(input: T): Boolean
}

enum class AttributeName {
  TIER,
}

enum class Operator {
  IN,
}

sealed interface AttributeValue

data class CustomerTierFilter(
  val name: AttributeName = AttributeName.TIER,
  val operator: Operator,
  val value: List<CustomerTier>,
) : Expression<CustomerTier> {
  override fun evaluate(input: CustomerTier): Boolean =
    when (name) {
      AttributeName.TIER -> {
        when (operator) {
          Operator.IN -> input in value
        }
      }
    }
}

data class ConnectorRolloutFilters(
  val customerTierFilters: List<CustomerTierFilter> = emptyList(),
)
