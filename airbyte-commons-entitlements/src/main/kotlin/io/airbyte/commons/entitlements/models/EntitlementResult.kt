/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements.models

data class EntitlementResult(
  val entitlementId: String,
  val isEntitled: Boolean,
  val reason: String? = null,
)
