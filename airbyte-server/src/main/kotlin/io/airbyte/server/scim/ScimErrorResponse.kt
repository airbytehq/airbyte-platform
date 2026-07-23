/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

data class ScimErrorResponse(
  val schemas: List<String> = listOf(SCIM_ERROR_SCHEMA),
  val status: String,
  val detail: String,
)

internal const val SCIM_ERROR_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:Error"
