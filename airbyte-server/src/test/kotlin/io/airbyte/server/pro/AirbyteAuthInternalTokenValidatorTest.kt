/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.pro

import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.micronaut.http.HttpRequest
import io.micronaut.security.authentication.Authentication
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import reactor.test.StepVerifier

internal class AirbyteAuthInternalTokenValidatorTest {
  private val airbyteAuthInternalTokenValidator = AirbyteAuthInternalTokenValidator()

  @Test
  fun testValidateTokenIsWorker() {
    val token = "worker"
    val httpRequest = Mockito.mock(HttpRequest::class.java)

    val expectedRoles =
      setOf(
        AuthRoleConstants.ADMIN,
        AuthRoleConstants.WORKSPACE_ADMIN,
        AuthRoleConstants.WORKSPACE_EDITOR,
        AuthRoleConstants.WORKSPACE_READER,
        AuthRoleConstants.ORGANIZATION_ADMIN,
        AuthRoleConstants.ORGANIZATION_EDITOR,
        AuthRoleConstants.ORGANIZATION_READER,
        AuthRoleConstants.ORGANIZATION_MEMBER,
        AuthRoleConstants.AUTHENTICATED_USER,
      )

    val responsePublisher = airbyteAuthInternalTokenValidator.validateToken(token, httpRequest)

    StepVerifier
      .create(responsePublisher)
      .expectNextMatches { r: Authentication -> matchSuccessfulResponse(r, expectedRoles) }
      .verifyComplete()
  }

  @Test
  fun testValidateTokenIsNotWorker() {
    val token = "not-worker"
    val httpRequest = Mockito.mock(HttpRequest::class.java)

    val responsePublisher = airbyteAuthInternalTokenValidator.validateToken(token, httpRequest)

    // Verify the stream remains empty.
    StepVerifier
      .create(responsePublisher)
      .expectComplete()
      .verify()
  }

  private fun matchSuccessfulResponse(
    authentication: Authentication,
    expectedRoles: Collection<String>,
  ): Boolean = authentication.roles.containsAll(expectedRoles)
}
