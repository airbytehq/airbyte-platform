/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import java.util.UUID
import java.util.function.Supplier

sealed class SecretCoordinate {
  abstract val fullCoordinate: String

  companion object {
    /**
     * Used to turn a full string coordinate into a [SecretCoordinate]. First attempts to parse the
     * coordinate as an [AirbyteManagedSecretCoordinate]. If that fails, it falls back to an
     * [ExternalSecretCoordinate].
     */
    fun fromFullCoordinate(fullCoordinate: String): SecretCoordinate =
      AirbyteManagedSecretCoordinate.fromFullCoordinate(fullCoordinate) ?: ExternalSecretCoordinate(fullCoordinate)
  }

  data class ExternalSecretCoordinate(
    override val fullCoordinate: String,
  ) : SecretCoordinate()

  data class AirbyteManagedSecretCoordinate(
    private val rawCoordinateBase: String = generateCoordinateBase(DEFAULT_SECRET_BASE_PREFIX, DEFAULT_SECRET_BASE_ID),
    val version: Long = DEFAULT_VERSION,
  ) : SecretCoordinate() {
    val coordinateBase: String = ensureAirbytePrefix(rawCoordinateBase)

    /**
     * Constructor that generates a new [AirbyteManagedSecretCoordinate] with a coordinate base
     * generated based on provided inputs
     */
    constructor(
      secretBasePrefix: String,
      secretBaseId: UUID,
      version: Long,
      uuidSupplier: Supplier<UUID> = Supplier { UUID.randomUUID() },
    ) : this(
      generateCoordinateBase(secretBasePrefix, secretBaseId, uuidSupplier),
      version,
    )

    override val fullCoordinate: String
      get() = "${coordinateBase}$VERSION_DELIMITER$version"

    companion object {
      const val DEFAULT_SECRET_BASE_PREFIX = "workspace_"
      val DEFAULT_SECRET_BASE_ID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")
      const val DEFAULT_VERSION = 1L

      private const val VERSION_DELIMITER = "_v"
      private const val AIRBYTE_PREFIX = "airbyte_"

      private fun ensureAirbytePrefix(coordinateBase: String): String =
        if (coordinateBase.startsWith(AIRBYTE_PREFIX)) {
          coordinateBase
        } else {
          AIRBYTE_PREFIX + coordinateBase
        }

      private fun generateCoordinateBase(
        secretBasePrefix: String,
        secretBaseId: UUID,
        uuidSupplier: Supplier<UUID> = Supplier { UUID.randomUUID() },
      ): String = "$AIRBYTE_PREFIX${secretBasePrefix}${secretBaseId}_secret_${uuidSupplier.get()}"

      /**
       * Used to turn a full string coordinate into an [AirbyteManagedSecretCoordinate] if it
       * follows the particular expected format. Otherwise, returns null.
       */
      fun fromFullCoordinate(fullCoordinate: String): AirbyteManagedSecretCoordinate? {
        if (!fullCoordinate.startsWith(AIRBYTE_PREFIX)) return null

        val splitIndex = fullCoordinate.lastIndexOf(VERSION_DELIMITER)
        if (splitIndex == -1) return null

        val coordinateBase = fullCoordinate.substring(0, splitIndex)
        val version =
          fullCoordinate.substring(splitIndex + VERSION_DELIMITER.length).toLongOrNull()
            ?: return null

        return AirbyteManagedSecretCoordinate(coordinateBase, version)
      }
    }
  }
}
