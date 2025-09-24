/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements.models

import java.util.UUID

/**
 * Represents a feature or capability entitlement in the Airbyte platform.
 *
 * Entitlements are used to control access to premium features, enterprise connectors,
 * and other paid capabilities based on an organization's subscription plan or license.
 */
interface Entitlement {
  /** The unique identifier for this entitlement feature */
  val featureId: String
  val name: String
}

/**
 * A generic feature entitlement identified by a simple feature ID string.
 *
 * @param featureId The unique identifier for this feature entitlement
 */
open class FeatureEntitlement(
  override val featureId: String,
) : Entitlement {
  override val name: String =
    if (featureId.startsWith("feature-")) {
      featureId.removePrefix("feature-")
    } else {
      featureId
    }
}

/**
 * Entitlement for enterprise/premium connectors that require special licensing.
 *
 * Enterprise connectors are typically advanced connectors that require additional
 * licensing beyond the standard Airbyte offering. This entitlement type maps
 * specific connector definitions to their required entitlements.
 *
 * @param actorDefinitionId The UUID of the connector definition this entitlement applies to
 */
abstract class ConnectorEntitlement(
  val actorDefinitionId: UUID,
) : Entitlement {
  abstract override val name: String

  override val featureId: String = "$PREFIX$actorDefinitionId"

  companion object {
    /** Prefix used for all enterprise connector feature IDs */
    const val PREFIX = "feature-enterprise-connector-"

    /**
     * Checks if a feature ID represents an enterprise connector entitlement.
     *
     * @param featureId The feature ID to check
     * @return true if this is an enterprise connector feature ID
     */
    fun isConnectorFeatureId(featureId: String): Boolean = featureId.startsWith(PREFIX)

    internal fun parseActorDefinitionIdOrNull(featureId: String): UUID? {
      if (!isConnectorFeatureId(featureId)) return null
      val idPart = featureId.removePrefix(PREFIX)
      return try {
        UUID.fromString(idPart)
      } catch (_: IllegalArgumentException) {
        null
      }
    }
  }
}
