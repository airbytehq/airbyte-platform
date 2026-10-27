/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models.auth

import java.util.Optional
import java.util.UUID

/** Nonsecret project references consumed by builder authorization. */
data class BuilderProjectAuthorizationMetadata(
  val definition: Optional<UUID>,
  val baseVersion: Optional<UUID>,
  val hasDraft: Boolean,
)
