/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.google.common.base.Preconditions
import io.airbyte.config.secrets.persistence.SecretPersistence
import java.util.Objects

/**
 * A secret coordinate represents a specific secret at a specific version stored within a
 * [SecretPersistence].
 *
 * We use "coordinate base" to refer to a string reference to a secret without versioning
 * information. We use "full coordinate" to refer to a string reference that includes both the
 * coordinate base and version-specific information. You should be able to go from a "full
 * coordinate" to a coordinate object and back without loss of information.
 *
 * Example coordinate base:
 * airbyte_workspace_e0eb0554-ffe0-4e9c-9dc0-ed7f52023eb2_secret_9eba44d8-51e7-48f1-bde2-619af0e42c22
 *
 * Example full coordinate:
 * airbyte_workspace_e0eb0554-ffe0-4e9c-9dc0-ed7f52023eb2_secret_9eba44d8-51e7-48f1-bde2-619af0e42c22_v1
 *
 * This coordinate system was designed to work well with Google Secrets Manager but should work with
 * other secret storage backends as well.
 */
class SecretCoordinate(val coordinateBase: String, val version: Long) {
  val fullCoordinate: String
    get() = coordinateBase + "_v" + version

  override fun equals(other: Any?): Boolean {
    if (this === other) {
      return true
    }
    if (other == null || javaClass != other.javaClass) {
      return false
    }
    val that = other as SecretCoordinate
    return toString() == that.toString()
  }

  /**
   * The hash code is computed using the [SecretCoordinate.fullCoordinate] because the full
   * secret coordinate should be a valid unique representation of the secret coordinate.
   */
  override fun hashCode(): Int {
    return Objects.hash(fullCoordinate)
  }

  companion object {
    /**
     * Used to turn a full string coordinate into a coordinate object using a full coordinate generated
     * by [SecretsHelpers.getCoordinate].
     *
     * This will likely need refactoring if we end up using a secret store that doesn't allow the same
     * format of full coordinate.
     *
     * @param fullCoordinate coordinate with version
     * @return secret coordinate object
     */
    fun fromFullCoordinate(fullCoordinate: String): SecretCoordinate {
      val splits = fullCoordinate.split("_v")
      Preconditions.checkArgument(splits.size == 2)
      return SecretCoordinate(splits[0], splits[1].toLong())
    }
  }
}
