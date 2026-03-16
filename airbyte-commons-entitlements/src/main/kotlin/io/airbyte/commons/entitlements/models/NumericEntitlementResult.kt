/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements.models

data class NumericEntitlementResult(
  val featureId: String,
  val hasAccess: Boolean,
  val value: Long?,
  val reason: String? = null,
)
