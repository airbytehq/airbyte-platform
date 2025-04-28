/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

sealed interface Expression<T> {
  fun evaluate(input: T): Boolean
}

enum class AttributeName {
  TIER,
  BYPASS_JOBS,
}

enum class Operator {
  IN,
  IS_TRUE,
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
          else -> throw IllegalArgumentException("Unsupported operator: $operator for attribute $name")
        }
      }
      else -> throw IllegalArgumentException("Unsupported attribute: $name")
    }
}

data class JobBypassFilter(
  val name: AttributeName = AttributeName.BYPASS_JOBS,
  val value: Boolean,
) : Expression<Unit> {
  override fun evaluate(input: Unit): Boolean = value
}

data class ConnectorRolloutFilters(
  val customerTierFilters: List<CustomerTierFilter> = emptyList(),
  val jobBypassFilter: JobBypassFilter? = null,
)
