/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements.models

import java.util.UUID

interface Entitlement {
  val featureId: String
}

open class FeatureEntitlement(
  override val featureId: String,
) : Entitlement

open class ConnectorEntitlement(
  val actorDefinitionId: UUID,
) : Entitlement {
  companion object {
    const val PREFIX = "feature-enterprise-connector-"

    fun isConnectorFeatureId(featureId: String): Boolean = featureId.startsWith(PREFIX)

    fun fromFeatureId(featureId: String): ConnectorEntitlement? =
      try {
        val uuid = UUID.fromString(featureId.removePrefix(PREFIX))
        ConnectorEntitlement(uuid)
      } catch (e: IllegalArgumentException) {
        null
      }
  }

  override val featureId: String = "$PREFIX$actorDefinitionId"
}
