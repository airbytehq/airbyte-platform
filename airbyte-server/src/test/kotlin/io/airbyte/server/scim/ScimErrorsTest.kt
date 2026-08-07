/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import io.micronaut.http.HttpStatus
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ScimErrorsTest {
  @Test
  fun `shared factories use the specified status and scimType`() {
    assertError(ScimErrors.invalidFilter(), HttpStatus.BAD_REQUEST, "invalidFilter")
    assertError(ScimErrors.invalidPath(), HttpStatus.BAD_REQUEST, "invalidPath")
    assertError(ScimErrors.invalidValue(), HttpStatus.BAD_REQUEST, "invalidValue")
    assertError(ScimErrors.mutability(), HttpStatus.BAD_REQUEST, "mutability")
    assertError(ScimErrors.uniqueness(), HttpStatus.CONFLICT, "uniqueness")
  }

  private fun assertError(
    exception: ScimException,
    status: HttpStatus,
    scimType: String,
  ) {
    assertThat(exception.status).isEqualTo(status)
    assertThat(exception.scimType).isEqualTo(scimType)
  }
}
