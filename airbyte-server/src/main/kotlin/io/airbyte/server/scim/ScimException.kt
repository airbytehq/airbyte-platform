/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import io.micronaut.http.HttpStatus

class ScimException(
  val status: HttpStatus,
  val scimType: String? = null,
  detail: String,
) : RuntimeException(detail)
