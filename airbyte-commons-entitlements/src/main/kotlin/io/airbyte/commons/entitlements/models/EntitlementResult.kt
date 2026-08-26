/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements.models

data class EntitlementResult(
  val featureId: String,
  val isEntitled: Boolean,
  val reason: String? = null,
  // Required for enterprise connector entitlements
  val featureName: String? = null,
  /**
   * False means the check did not produce a definitive answer (exception, Stigg fallback, timeout,
   * or kill switch), so [isEntitled] is not a denial. Callers must fall back to existing behavior
   * rather than acting on it. [isEntitled] = true together with this being false is possible.
   */
  val isEntitlementCheckSuccessful: Boolean = true,
)
