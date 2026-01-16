/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import java.util.UUID

enum class SsoConfigStatus {
  DRAFT,
  ACTIVE,
}

data class SsoConfig(
  val organizationId: UUID,
  val companyIdentifier: String,
  val clientId: String,
  val clientSecret: String,
  val discoveryUrl: String,
  val emailDomain: String?, // not required when status is DRAFT
  val status: SsoConfigStatus,
)
