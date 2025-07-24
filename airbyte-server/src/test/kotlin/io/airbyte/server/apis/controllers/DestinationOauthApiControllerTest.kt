/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.CompleteDestinationOAuthRequest
import io.airbyte.api.model.generated.CompleteOAuthResponse
import io.airbyte.api.model.generated.DestinationOauthConsentRequest
import io.airbyte.api.model.generated.OAuthConsentRead
import io.airbyte.api.model.generated.SetInstancewideDestinationOauthParamsRequestBody
import io.airbyte.commons.server.errors.IdNotFoundKnownException
import io.airbyte.commons.server.handlers.OAuthHandler
import io.airbyte.config.persistence.ConfigNotFoundException
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class DestinationOauthApiControllerTest {
  private lateinit var controller: DestinationOauthApiController
  private val oAuthHandler: OAuthHandler = mockk()

  @BeforeEach
  fun setUp() {
    controller = DestinationOauthApiController(oAuthHandler)
  }

  @Test
  fun testCompleteDestinationOAuth() {
    every { oAuthHandler.completeDestinationOAuth(any()) } returns CompleteOAuthResponse() andThenThrows ConfigNotFoundException("", "")

    val request = CompleteDestinationOAuthRequest()
    val result = controller.completeDestinationOAuth(request)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      controller.completeDestinationOAuth(request)
    }
  }

  @Test
  fun testGetDestinationOAuthConsent() {
    every { oAuthHandler.getDestinationOAuthConsent(any()) } returns OAuthConsentRead() andThenThrows ConfigNotFoundException("", "")

    val request = DestinationOauthConsentRequest()
    val result = controller.getDestinationOAuthConsent(request)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      controller.getDestinationOAuthConsent(request)
    }
  }

  @Test
  fun testSetInstancewideDestinationOauthParams() {
    every { oAuthHandler.setDestinationInstancewideOauthParams(any()) } returns Unit

    val request = SetInstancewideDestinationOauthParamsRequestBody()
    controller.setInstancewideDestinationOauthParams(request)
  }
}
