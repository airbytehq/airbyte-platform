/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.CompleteDestinationOAuthRequest
import io.airbyte.api.model.generated.CompleteOAuthResponse
import io.airbyte.api.model.generated.DestinationOauthConsentRequest
import io.airbyte.api.model.generated.OAuthConsentRead
import io.airbyte.api.model.generated.SetInstancewideDestinationOauthParamsRequestBody
import io.airbyte.commons.server.handlers.OAuthHandler
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.airbyte.server.statusException
import io.airbyte.validation.json.JsonValidationException
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import org.junit.jupiter.api.Test
import java.io.IOException

@MicronautTest
internal class DestinationOauthApiControllerTest {
  @Inject
  lateinit var oAuthHandler: OAuthHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(OAuthHandler::class)
  fun oauthHandler(): OAuthHandler = mockk()

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testCompleteDestinationOAuth() {
    every { oAuthHandler.completeDestinationOAuth(any()) } returns CompleteOAuthResponse() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/destination_oauths/complete_oauth"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, CompleteDestinationOAuthRequest())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, CompleteDestinationOAuthRequest())))
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testGetDestinationOAuthConsent() {
    every { oAuthHandler.getDestinationOAuthConsent(any()) } returns OAuthConsentRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/destination_oauths/get_consent_url"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, DestinationOauthConsentRequest())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, DestinationOauthConsentRequest())))
  }

  @Test
  @Throws(IOException::class)
  fun testDeleteDestination() {
    every { oAuthHandler.setDestinationInstancewideOauthParams(any()) } returns Unit

    val path = "/api/v1/destination_oauths/oauth_params/create"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SetInstancewideDestinationOauthParamsRequestBody())))
  }
}
