/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.license

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonValue
import java.util.Date

/**
 * An immutable representation of an Airbyte License.
 */
data class AirbyteLicense(
  val type: LicenseType,
  val expirationDate: Date? = null,
  val maxNodes: Int? = null,
  val maxEditors: Int? = null,
) {
  enum class LicenseType(
    @get:JsonValue val value: String,
  ) {
    PRO("pro"),
    INVALID("invalid"),
    TRIAL("trial"),
    ENTERPRISE("enterprise"),
    ;

    override fun toString(): String = value

    companion object {
      @JsonCreator
      @JvmStatic
      fun fromValue(value: String): LicenseType =
        entries.firstOrNull { it.value == value } ?: throw IllegalArgumentException("Unexpected value '$value'")
    }
  }
}
