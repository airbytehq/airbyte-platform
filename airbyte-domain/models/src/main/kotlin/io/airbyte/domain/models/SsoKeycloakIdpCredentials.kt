/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import java.util.UUID

data class SsoKeycloakIdpCredentials(
  val organizationId: UUID,
  val clientId: String,
  val clientSecret: String,
)
