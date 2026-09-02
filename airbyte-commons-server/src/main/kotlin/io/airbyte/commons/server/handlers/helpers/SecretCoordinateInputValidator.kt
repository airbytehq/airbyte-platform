/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.constants.AirbyteSecretConstants
import io.airbyte.commons.json.JsonPaths
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.config.secrets.ConfigWithSecretReferences
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.SecretsHelpers.SecretReferenceHelpers.CoordinateInputShape
import io.airbyte.config.secrets.SecretsHelpers.SecretReferenceHelpers.getSecretCoordinateInputs
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageId

/**
 * Validates coordinate-prefixed values supplied on actor configuration writes.
 */
object SecretCoordinateInputValidator {
  /**
   * Rejects a config whose secret fields carry a coordinate that the caller cannot legitimately
   * reference: an Airbyte-managed coordinate, which is a read-only pointer returned on read, a blank
   * coordinate, or any coordinate for a workspace without its own configured secret storage, where
   * the coordinate would resolve against storage shared with other tenants.
   *
   * A coordinate identical to the one already persisted at the same config path is left unchanged
   * rather than rewritten: the field is replaced with the persisted secret node, the same way
   * [AirbyteSecretConstants.SECRETS_MASK] is, so an unmodified read payload can be written back
   * without the secret being written again. Returns the config to persist.
   *
   * A coordinate can also arrive in the object form a persisted config uses, which internal write
   * paths produce legitimately by copying persisted secret nodes into an incoming config. Both forms
   * are held to the same rule, since a copied node is by construction identical to the one stored at
   * its path, and only a read of someone's config yields one that is not.
   *
   * [persistedConfig] is only invoked when the config actually carries a coordinate.
   */
  fun validateSecretCoordinateInput(
    config: JsonNode,
    connectionSpecification: JsonNode,
    secretStorageId: SecretStorageId?,
    persistedConfig: () -> ConfigWithSecretReferences?,
  ): JsonNode {
    val coordinateInputs = getSecretCoordinateInputs(config, connectionSpecification)
    if (coordinateInputs.isEmpty()) {
      return config
    }
    val hasConfiguredSecretStorage = secretStorageId != null && secretStorageId != SecretStorage.DEFAULT_SECRET_STORAGE_ID
    val persisted by lazy(persistedConfig)
    val storedCoordinates by lazy {
      persisted?.let { getSecretCoordinateInputs(it.originalConfig, connectionSpecification) }.orEmpty()
    }
    var sanitizedConfig = config
    coordinateInputs.forEach { (path, input) ->
      val coordinate = input.coordinate

      // Matched against the stored config as well as the resolved references, so that an orphaned
      // reference row does not turn an ordinary save into a rejection.
      fun isUnchanged(): Boolean =
        coordinate.fullCoordinate in
          setOfNotNull(
            persisted
              ?.referencedSecrets
              ?.get(path)
              ?.secretCoordinate
              ?.fullCoordinate,
            storedCoordinates[path]?.coordinate?.fullCoordinate,
          )

      when {
        coordinate.fullCoordinate.isBlank() -> throw BadRequestException(REJECTION_MESSAGE)
        coordinate is AirbyteManagedSecretCoordinate && !isUnchanged() -> throw BadRequestException(REJECTION_MESSAGE)
        coordinate !is AirbyteManagedSecretCoordinate && !hasConfiguredSecretStorage && !isUnchanged() ->
          throw BadRequestException(REJECTION_MESSAGE)
        isUnchanged() && input.shape == CoordinateInputShape.PREFIXED_STRING -> {
          // Leave the stored secret alone by carrying the persisted node over, as the mask does.
          val persistedNode =
            JsonPaths.getSingleValue(persisted!!.originalConfig, path).orElseThrow {
              BadRequestException(REJECTION_MESSAGE)
            }
          sanitizedConfig = JsonPaths.replaceAtJsonNodeLoud(sanitizedConfig, path, persistedNode)
        }
      }
    }
    return sanitizedConfig
  }

  private val REJECTION_MESSAGE =
    "Secret coordinates are read-only pointers and are not accepted as configuration input. " +
      "Send the secret value, or ${AirbyteSecretConstants.SECRETS_MASK} to leave the stored secret unchanged."
}
