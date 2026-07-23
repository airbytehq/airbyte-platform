/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import java.util.UUID

enum class SsoConfigStatus {
  DRAFT,
  ACTIVE,
}

enum class SsoDefaultRole {
  ORGANIZATION_ADMIN,
  ORGANIZATION_EDITOR,
  ORGANIZATION_MEMBER,
}

val DEFAULT_SSO_ROLE = SsoDefaultRole.ORGANIZATION_MEMBER

data class SsoConfig(
  val organizationId: UUID,
  val companyIdentifier: String,
  val clientId: String,
  val clientSecret: String,
  val discoveryUrl: String,
  val emailDomain: String?, // not required when status is DRAFT
  val status: SsoConfigStatus,
  // null means "not specified by the caller". On create the consumer falls back to DEFAULT_SSO_ROLE;
  // on a draft update a null role leaves the stored role unchanged (so omitting it never downgrades it).
  val defaultRole: SsoDefaultRole? = null,
)
