/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.csp

import com.fasterxml.jackson.annotation.JsonInclude
import io.airbyte.commons.annotation.InternalForTesting

/**
 * Status is the base class for all status checks.  See the subclasses for implementations.
 *
 * @param result a human readable string representing the status
 * @param message is an optional message to display alongside the status. Primarily for error messages.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
sealed class Status(
  val result: String,
  val message: String? = null,
)

@InternalForTesting
internal const val STATUS_PASS = "pass"

/**
 * Represents a successful status check.
 */
class PassStatus : Status(result = STATUS_PASS) {
  override fun toString(): String = result
}

@InternalForTesting
internal const val STATUS_FAIL = "fail"

/**
 * Represents a failed status check.
 *
 * @param throwable the throwable that triggered this failure.
 */
class FailStatus(
  throwable: Throwable? = null,
) : Status(result = STATUS_FAIL, message = throwable?.message) {
  override fun toString(): String = "$result ($message)"
}
