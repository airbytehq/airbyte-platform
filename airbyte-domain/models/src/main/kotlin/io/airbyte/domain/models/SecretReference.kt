/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import java.time.OffsetDateTime
import java.util.UUID

/**
 * A secret reference describes a reference to a secret that is stored in a secret storage. The
 * scope type and scope id fields are used to determine the context in which the secret is referenced.
 * The hydration path is used to determine where in the config the secret should be injected, if set.
 * A secret reference without a hydration path describes the case where the secret is not injected
 * into some sub-configuration, but instead describes a top level secret (ie a secret that contains
 * the entire configuration for some resource).
 *
 */
data class SecretReference(
  val id: SecretReferenceId,
  val secretConfigId: SecretConfigId,
  val scopeType: SecretReferenceScopeType,
  val scopeId: UUID,
  val hydrationPath: String?,
  val createdAt: OffsetDateTime?,
  val updatedAt: OffsetDateTime?,
)

enum class SecretReferenceScopeType {
  ACTOR,
  SECRET_STORAGE,
  CONNECTION_TEMPLATE,
  ACTOR_OAUTH_PARAMETER,
}

data class SecretReferenceCreate(
  val secretConfigId: SecretConfigId,
  val scopeType: SecretReferenceScopeType,
  val scopeId: UUID,
  val hydrationPath: String?,
)
