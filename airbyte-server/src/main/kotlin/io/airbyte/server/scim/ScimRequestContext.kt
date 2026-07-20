/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import io.airbyte.domain.services.scim.ScimAuthenticationContext
import io.micronaut.http.HttpRequest

internal const val SCIM_AUTHENTICATION_ATTRIBUTE = "io.airbyte.scim.authentication"

fun HttpRequest<*>.scimAuthenticationContext(): ScimAuthenticationContext =
  getAttribute(SCIM_AUTHENTICATION_ATTRIBUTE, ScimAuthenticationContext::class.java)
    .orElseThrow { IllegalStateException("Authenticated SCIM context is missing") }
