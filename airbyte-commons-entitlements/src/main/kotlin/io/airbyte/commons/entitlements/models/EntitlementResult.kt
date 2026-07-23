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
)
