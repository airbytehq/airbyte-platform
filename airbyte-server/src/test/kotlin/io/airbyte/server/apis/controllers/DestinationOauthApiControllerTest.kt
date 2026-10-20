/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.CompleteDestinationOAuthRequest
import io.airbyte.api.server.generated.models.CompleteOAuthResponse
import io.airbyte.api.server.generated.models.DestinationOauthConsentRequest
import io.airbyte.api.server.generated.models.OAuthConsentRead
import io.airbyte.api.server.generated.models.SetInstancewideDestinationOauthParamsRequestBody
import io.airbyte.commons.server.errors.IdNotFoundKnownException
import io.airbyte.commons.server.handlers.OAuthHandler
import io.airbyte.config.persistence.ConfigNotFoundException
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class DestinationOauthApiControllerTest {
  private lateinit var controller: DestinationOauthApiController
  private val oAuthHandler: OAuthHandler = mockk()

  @BeforeEach
  fun setUp() {
    controller = DestinationOauthApiController(oAuthHandler)
  }

  @Test
  fun testCompleteDestinationOAuth() {
    every {
      oAuthHandler.completeDestinationOAuth(
        any(),
      )
    } returns CompleteOAuthResponse(requestSucceeded = true, authPayload = emptyMap()) andThenThrows
      ConfigNotFoundException("", "")

    val request =
      CompleteDestinationOAuthRequest(
        destinationDefinitionId = UUID.randomUUID(),
        workspaceId = UUID.randomUUID(),
        redirectUrl = "https://example.com/callback",
      )
    val result = controller.completeDestinationOAuth(request)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      controller.completeDestinationOAuth(request)
    }
  }

  @Test
  fun testGetDestinationOAuthConsent() {
    every { oAuthHandler.getDestinationOAuthConsent(any()) } returns OAuthConsentRead(consentUrl = "https://example.com/consent") andThenThrows
      ConfigNotFoundException("", "")

    val request =
      DestinationOauthConsentRequest(
        destinationDefinitionId = UUID.randomUUID(),
        workspaceId = UUID.randomUUID(),
        redirectUrl = "https://example.com/callback",
      )
    val result = controller.getDestinationOAuthConsent(request)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      controller.getDestinationOAuthConsent(request)
    }
  }

  @Test
  fun testSetInstancewideDestinationOauthParams() {
    every { oAuthHandler.setDestinationInstancewideOauthParams(any()) } returns Unit

    val request = SetInstancewideDestinationOauthParamsRequestBody(destinationDefinitionId = UUID.randomUUID(), params = mapOf("client_id" to "id"))
    controller.setInstancewideDestinationOauthParams(request)
  }
}
