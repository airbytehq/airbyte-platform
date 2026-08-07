/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import io.micronaut.http.HttpStatus

object ScimErrors {
  fun invalidFilter(detail: String = "The SCIM filter is malformed or unsupported"): ScimException =
    ScimException(HttpStatus.BAD_REQUEST, "invalidFilter", detail)

  fun invalidPath(detail: String = "The SCIM path is malformed or unsupported"): ScimException =
    ScimException(HttpStatus.BAD_REQUEST, "invalidPath", detail)

  fun invalidValue(detail: String = "The SCIM value is invalid"): ScimException = ScimException(HttpStatus.BAD_REQUEST, "invalidValue", detail)

  fun mutability(detail: String = "The SCIM attribute cannot be modified"): ScimException =
    ScimException(HttpStatus.BAD_REQUEST, "mutability", detail)

  fun noTarget(detail: String = "The SCIM path did not match a target"): ScimException = ScimException(HttpStatus.BAD_REQUEST, "noTarget", detail)

  fun uniqueness(detail: String = "The SCIM identifier is already in use"): ScimException = ScimException(HttpStatus.CONFLICT, "uniqueness", detail)

  fun notFound(detail: String): ScimException = ScimException(HttpStatus.NOT_FOUND, detail = detail)
}
