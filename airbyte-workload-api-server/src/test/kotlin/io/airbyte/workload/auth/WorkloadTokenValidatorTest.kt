/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.auth

import io.micronaut.http.HttpRequest
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.authentication.AuthenticationException
import io.micronaut.security.authentication.AuthenticationFailed
import io.micronaut.security.authentication.AuthenticationFailureReason
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.reactivestreams.Publisher
import reactor.test.StepVerifier
import java.util.Base64

class WorkloadTokenValidatorTest {
  @Test
  internal fun `test that a valid token is successfully authenticated`() {
    val expectedToken = "the expected token value"
    val httpRequest: HttpRequest<*> = mockk()
    val validator = WorkloadTokenValidator(expectedToken)

    val responsePublisher: Publisher<Authentication> =
      validator.validateToken(
        Base64.getEncoder().encodeToString(expectedToken.toByteArray()),
        httpRequest,
      )

    StepVerifier.create(responsePublisher)
      .expectNextMatches { a: Authentication -> matchSuccessfulResponse(a) }
      .verifyComplete()
  }

  @Test
  internal fun `test that a valid token with a newline is successfully authenticated`() {
    val expectedToken = "the expected token value"
    val httpRequest: HttpRequest<*> = mockk()
    val validator = WorkloadTokenValidator(expectedToken)

    val responsePublisher: Publisher<Authentication> =
      validator.validateToken(
        Base64.getEncoder().encodeToString("$expectedToken\n".toByteArray()),
        httpRequest,
      )

    StepVerifier.create(responsePublisher)
      .expectNextMatches { a: Authentication -> matchSuccessfulResponse(a) }
      .verifyComplete()
  }

  @Test
  internal fun `test that an invalid token is rejected`() {
    val expectedToken = "the expected token value"
    val invalidToken = "not the expected token"
    val httpRequest: HttpRequest<*> = mockk()
    val validator = WorkloadTokenValidator(expectedToken)

    val responsePublisher: Publisher<Authentication> =
      validator.validateToken(
        Base64.getEncoder().encodeToString(invalidToken.toByteArray()),
        httpRequest,
      )

    StepVerifier.create(responsePublisher)
      .expectErrorMatches { t: Throwable -> matchUnsuccessfulResponse(t) }
      .verify()
  }

  private fun matchSuccessfulResponse(authentication: Authentication): Boolean {
    return authentication.name == WorkloadTokenValidator.WORKLOAD_API_USER
  }

  private fun matchUnsuccessfulResponse(t: Throwable): Boolean {
    return t::class == AuthenticationException::class &&
      t.message == "${AuthenticationFailed(AuthenticationFailureReason.CREDENTIALS_DO_NOT_MATCH).message.get()}"
  }
}
