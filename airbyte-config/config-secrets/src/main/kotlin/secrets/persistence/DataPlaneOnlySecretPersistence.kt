/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.persistence

import io.airbyte.config.secrets.SecretCoordinate

/**
 * DataPlaneOnlySecretPersistence is used when a customer configures their secret persistence so that only their data plane has access to the secret store – i.e. Airbyte and the control plane has zero access to their secret store, and their secret store is configured by environment variables that are private to their self-hosted data plane.
 *
 * This implementation fails on all operations because the control plane should not have access to the secrets.
 */
class DataPlaneOnlySecretPersistence : SecretPersistence {
  override fun read(coordinate: SecretCoordinate): String =
    throw UnsupportedOperationException(
      "Tried to read secret coordinate ${coordinate.fullCoordinate} from the control plane, but this is not supported when using secret storage config provided by the data plane environment.",
    )

  override fun write(
    coordinate: SecretCoordinate.AirbyteManagedSecretCoordinate,
    payload: String,
  ): Unit =
    throw UnsupportedOperationException(
      "Tried to write secrets from the control plane, but this is not supported when using secret storage config provided by the data plane environment.",
    )

  override fun delete(coordinate: SecretCoordinate.AirbyteManagedSecretCoordinate): Unit =
    throw UnsupportedOperationException(
      "Tried to delete secret coordinate ${coordinate.fullCoordinate} from the control plane, but this is not supported when using secret storage config provided by the data plane environment.",
    )
}
