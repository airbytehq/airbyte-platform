/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import java.util.UUID

data class SecretReferenceConfig(
  val secretCoordinate: SecretCoordinate,
  val secretStorageId: UUID? = null,
  val secretReferenceId: UUID? = null,
)
