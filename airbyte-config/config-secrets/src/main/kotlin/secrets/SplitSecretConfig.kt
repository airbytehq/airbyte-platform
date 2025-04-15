/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate

/**
 * Data class that provides a way to store the output of a [SecretsHelpers] "split" operation
 * which takes a "full config" (including raw secret values) and creates a "partial config" (secrets
 * removed and has airbyte-managed coordinate pointers to a persistence layer).
 *
 * The split methods don't actually update the persistence layer itself. The coordinate to secret
 * payload map in this class allows the system calling "split" to update the persistence with those
 * new coordinate values.
 */
class SplitSecretConfig(
  val partialConfig: JsonNode,
  private val coordinateToPayload: Map<AirbyteManagedSecretCoordinate, String>,
) {
  fun getCoordinateToPayload(): Map<AirbyteManagedSecretCoordinate, String> = coordinateToPayload
}
