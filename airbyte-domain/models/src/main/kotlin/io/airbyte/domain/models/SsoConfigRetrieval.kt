/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

data class SsoConfigRetrieval(
  val companyIdentifier: String,
  val clientId: String,
  val clientSecret: String,
  val emailDomains: List<String>,
)
