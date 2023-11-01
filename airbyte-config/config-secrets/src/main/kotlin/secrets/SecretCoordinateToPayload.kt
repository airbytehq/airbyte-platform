/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode

/**
 * POJO to hold secret coordinate and the secret.
 *
 * @param secretCoordinate secret coordinate
 * @param payload the secret
 * @param secretCoordinateForDB json coordinate
 */
data class SecretCoordinateToPayload(
  val secretCoordinate: SecretCoordinate,
  val payload: String,
  val secretCoordinateForDB: JsonNode,
)
