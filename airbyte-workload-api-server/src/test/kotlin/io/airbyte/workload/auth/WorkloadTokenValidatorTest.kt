/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.auth

import io.micronaut.http.HttpRequest
import io.micronaut.security.authentication.Authentication
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

    StepVerifier
      .create(responsePublisher)
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

    StepVerifier
      .create(responsePublisher)
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

    // Passes to the next step without emitting anything.
    StepVerifier
      .create(responsePublisher)
      .verifyComplete()
  }

  @Test
  internal fun `test that a token with an underscore is successfully authenticated`() {
    // This payload is specifically constructed so that it encodes to a value that contains an underscore.
    // We used to have a bug in production where such tokens could not be decoded, so this test makes sure
    // that the bug remains fixed.
    val encodesToPayloadWithUnderscore = "{\"foo\":\"anoë\"}"

    val encodedToken = Base64.getUrlEncoder().encodeToString(encodesToPayloadWithUnderscore.toByteArray())
    assert(encodedToken.contains("_"))

    val httpRequest: HttpRequest<*> = mockk()
    val validator = WorkloadTokenValidator(encodesToPayloadWithUnderscore)

    val responsePublisher: Publisher<Authentication> =
      validator.validateToken(
        encodedToken,
        httpRequest,
      )

    StepVerifier
      .create(responsePublisher)
      .expectNextMatches { a: Authentication -> matchSuccessfulResponse(a) }
      .verifyComplete()
  }

  private fun matchSuccessfulResponse(authentication: Authentication): Boolean = authentication.name == WorkloadTokenValidator.WORKLOAD_API_USER
}
