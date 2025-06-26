/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements.models

import java.util.UUID

interface Entitlement {
  val id: String
}

// Make this open if it has logic and you want instantiable base class
open class FeatureEntitlement(
  override val id: String,
) : Entitlement

// Likewise for ConnectorEntitlement
open class ConnectorEntitlement(
  val actorDefinitionId: UUID,
) : Entitlement {
  override val id: String = "feature-enterprise-connector-$actorDefinitionId"
}
