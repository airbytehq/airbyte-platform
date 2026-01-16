/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

// Entitlement features defined in our entitlement service (Stigg).
// IDs map to a feature ID in Stigg.
enum class EntitlementFeature(
  val id: String,
) {
  RBAC("feature-rbac-roles"),
  ;

  companion object {
    fun fromId(id: String): EntitlementFeature =
      entries.firstOrNull { it.id == id }
        ?: throw IllegalArgumentException("No EntitlementFeature with id=$id")
  }
}
