/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import java.util.UUID

data class SsoConfig(
  val organizationId: UUID,
  val companyIdentifier: String,
  val clientId: String,
  val clientSecret: String,
  val discoveryUrl: String,
  val emailDomain: String,
)
