/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import com.fasterxml.jackson.databind.JsonNode
import java.time.OffsetDateTime
import java.util.UUID

/**
 * A secret storage describes some external storage that contains secrets. The scope type and scope id
 * describe the context in which the secret storage can be used (ie some particular workspace or organization).
 * If configuredFromEnvironment is set to true, then the secret storage is configured from
 * environment variables. Otherwise, there should exist a secret reference that has this secret
 * storage as its scope, and references a secret that contains the secret storage's configuration.
 */
data class SecretStorage(
  val id: SecretStorageId? = null,
  val scopeType: SecretStorageScopeType,
  val scopeId: UUID,
  val descriptor: String,
  val storageType: SecretStorageType,
  val configuredFromEnvironment: Boolean,
  val tombstone: Boolean = false,
  val createdBy: UUID,
  val updatedBy: UUID,
  val createdAt: OffsetDateTime?,
  val updatedAt: OffsetDateTime?,
)

enum class SecretStorageScopeType {
  ORGANIZATION,
  WORKSPACE,
}

enum class SecretStorageType {
  AWS_SECRETS_MANAGER,
  GOOGLE_SECRET_MANAGER,
  AZURE_KEY_VAULT,
  VAULT,
  LOCAL_TESTING,
}

data class SecretStorageWithConfig(
  val secretStorage: SecretStorage,
  val config: JsonNode?,
)
