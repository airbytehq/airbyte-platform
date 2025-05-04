/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.CompleteOAuthResponse
import io.airbyte.api.model.generated.OAuthConsentRead
import io.airbyte.api.model.generated.SourceDefinitionIdRequestBody
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.commons.server.handlers.OAuthHandler
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.airbyte.server.statusException
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

@MicronautTest
internal class SourceOauthApiControllerTest {
  @Inject
  lateinit var oAuthHandler: OAuthHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(OAuthHandler::class)
  fun oAuthHandler(): OAuthHandler = mockk()

  @Test
  fun testCompleteSourceOAuth() {
    every { oAuthHandler.completeSourceOAuthHandleReturnSecret(any()) } returns CompleteOAuthResponse() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/source_oauths/complete_oauth"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceDefinitionIdRequestBody())))
  }

  @Test
  fun testGetSourceOAuthConsent() {
    every { oAuthHandler.getSourceOAuthConsent(any()) } returns OAuthConsentRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/source_oauths/get_consent_url"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceDefinitionIdRequestBody())))
  }

  @Test
  fun testGetEmbeddedSourceOAuthConsent() {
    every { oAuthHandler.getSourceOAuthConsent(any()) } returns OAuthConsentRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/source_oauths/get_embedded_consent_url"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceDefinitionIdRequestBody())))
  }

  @Test
  fun testSetInstancewideSourceOauthParams() {
    every { oAuthHandler.setSourceInstancewideOauthParams(any()) } returns Unit

    val path = "/api/v1/source_oauths/oauth_params/create"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
  }
}
