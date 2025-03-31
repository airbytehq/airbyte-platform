/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

data class SecretReferenceWithConfig(
  val secretReference: SecretReference,
  val secretConfig: SecretConfig,
)
