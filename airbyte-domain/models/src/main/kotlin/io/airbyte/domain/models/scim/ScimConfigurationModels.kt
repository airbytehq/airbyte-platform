/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models.scim

import java.time.OffsetDateTime
import java.util.UUID

enum class ScimIdpProvider(
  val storageValue: String,
) {
  OKTA("okta"),
  MICROSOFT_ENTRA_ID("microsoft_entra_id"),
  ;

  companion object {
    fun fromStorageValue(value: String): ScimIdpProvider =
      entries.firstOrNull { it.storageValue == value }
        ?: throw IllegalStateException("Unsupported SCIM IdP provider: $value")
  }
}

enum class ScimConfigurationStatus {
  NOT_CONFIGURED,
  ENABLED,
  DISABLED,
}

data class ScimConfigurationRead(
  val status: ScimConfigurationStatus,
  val idpProvider: ScimIdpProvider? = null,
  val createdAt: OffsetDateTime? = null,
  val updatedAt: OffsetDateTime? = null,
  val token: String? = null,
)

class ScimAccessDeniedException(
  message: String,
) : RuntimeException(message)

class ScimConfigurationConflictException(
  message: String,
) : RuntimeException(message)

class ScimOrganizationNotFoundException(
  val organizationId: UUID,
) : RuntimeException("Organization $organizationId was not found")
