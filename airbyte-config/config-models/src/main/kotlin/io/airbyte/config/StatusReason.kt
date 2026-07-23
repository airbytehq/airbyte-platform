/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

/**
 * Enum representing reasons why a connection may have a particular status.
 */
enum class StatusReason(
  val value: String,
) {
  SUBSCRIPTION_DOWNGRADED_ACCESS_REVOKED("subscription_downgraded_access_revoked"),
  ;

  override fun toString(): String = value

  companion object {
    private val VALUE_MAP = entries.associateBy { it.value }

    fun fromValue(value: String): StatusReason =
      VALUE_MAP[value]
        ?: throw IllegalArgumentException("Unknown StatusReason value: $value")

    fun fromValueOrNull(value: String?): StatusReason? = value?.let { VALUE_MAP[it] }
  }
}
