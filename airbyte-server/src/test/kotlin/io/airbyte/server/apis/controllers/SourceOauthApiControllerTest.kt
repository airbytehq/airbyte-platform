/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.CompleteOAuthResponse
import io.airbyte.api.server.generated.models.OAuthConsentRead
import io.airbyte.commons.server.handlers.OAuthHandler
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.airbyte.server.statusException
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Property
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@MicronautTest
@Property(name = "controller.response.authorization.fixture", value = "true")
internal class SourceOauthApiControllerTest {
  @Inject
  lateinit var context: ApplicationContext

  lateinit var oAuthHandler: OAuthHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @BeforeAll
  fun setupMock() {
    oAuthHandler = mockk()
    context.registerSingleton(OAuthHandler::class.java, oAuthHandler)
  }

  @Test
  fun testCompleteSourceOAuth() {
    every { oAuthHandler.completeSourceOAuthHandleReturnSecret(any()) } returns
      CompleteOAuthResponse(requestSucceeded = true, authPayload = emptyMap()) andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/source_oauths/complete_oauth"
    val body = mapOf("sourceDefinitionId" to UUID.randomUUID(), "workspaceId" to UUID.randomUUID(), "redirectUrl" to "https://example.com/callback")
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, body)))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, body)))
  }

  @Test
  fun testGetSourceOAuthConsent() {
    every { oAuthHandler.getSourceOAuthConsent(any()) } returns OAuthConsentRead(consentUrl = "https://example.com/consent") andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/source_oauths/get_consent_url"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, consentBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, consentBody())))
  }

  @Test
  fun testGetEmbeddedSourceOAuthConsent() {
    every { oAuthHandler.getSourceOAuthConsent(any()) } returns OAuthConsentRead(consentUrl = "https://example.com/consent") andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/source_oauths/get_embedded_consent_url"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, consentBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, consentBody())))
  }

  @Test
  fun testSetInstancewideSourceOauthParams() {
    every { oAuthHandler.setSourceInstancewideOauthParams(any()) } returns Unit

    val path = "/api/v1/source_oauths/oauth_params/create"
    val body = mapOf("sourceDefinitionId" to UUID.randomUUID(), "params" to mapOf("client_id" to "id"))
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, body)))
  }
}

private fun consentBody() =
  mapOf("sourceDefinitionId" to UUID.randomUUID(), "workspaceId" to UUID.randomUUID(), "redirectUrl" to "https://example.com/callback")
