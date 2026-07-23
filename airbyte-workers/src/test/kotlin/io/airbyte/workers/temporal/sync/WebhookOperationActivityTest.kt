/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync

import com.fasterxml.jackson.databind.JsonNode
import dev.failsafe.FailsafeException
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.config.ConnectionContext
import io.airbyte.config.OperatorWebhookInput
import io.airbyte.config.WebhookConfig
import io.airbyte.config.WebhookOperationConfigs
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.micronaut.http.HttpStatus
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.UUID

internal class WebhookOperationActivityTest {
  private lateinit var webhookActivity: WebhookOperationActivity
  private lateinit var httpClient: HttpClient
  private lateinit var secretsRepositoryReader: SecretsRepositoryReader
  private lateinit var airbyteApiClient: AirbyteApiClient
  private lateinit var featureFlagClient: FeatureFlagClient

  @BeforeEach
  fun init() {
    httpClient = mockk()
    secretsRepositoryReader = mockk()
    airbyteApiClient = mockk()
    featureFlagClient = mockk<TestClient>(relaxed = true)
    webhookActivity =
      WebhookOperationActivityImpl(
        httpClient,
        secretsRepositoryReader,
        airbyteApiClient,
        featureFlagClient,
        mockk<MetricClient>(relaxed = true),
      )
  }

  @Test
  fun webhookActivityInvokesConfiguredWebhook() {
    val mockHttpResponse = mockk<HttpResponse<Any>>()
    every { mockHttpResponse.statusCode() } returns HttpStatus.OK.code
    every {
      secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(
        any<JsonNode>(),
      )
    } returns jsonNode(WORKSPACE_WEBHOOK_CONFIGS)
    val input =
      OperatorWebhookInput()
        .withExecutionBody(WEBHOOK_EXECUTION_BODY)
        .withExecutionUrl(WEBHOOK_EXECUTION_URL)
        .withWebhookConfigId(WEBHOOK_ID)
        .withConnectionContext(ConnectionContext().withOrganizationId(ORGANIZATION_ID))
        .withWorkspaceWebhookConfigs(Jsons.emptyObject())
    every {
      httpClient.send(
        any<HttpRequest>(),
        any<HttpResponse.BodyHandler<Any>>(),
      )
    } returns mockHttpResponse
    val success = webhookActivity.invokeWebhook(input)
    Assertions.assertTrue(success)
  }

  @Test
  fun webhookActivityFailsWhenRetriesExhausted() {
    val exception = IOException("test")
    every {
      httpClient.send(
        any<HttpRequest>(),
        any<HttpResponse.BodyHandler<Any>>(),
      )
    } throws exception
    every {
      secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(
        any<JsonNode>(),
      )
    } returns jsonNode(WORKSPACE_WEBHOOK_CONFIGS)
    val input =
      OperatorWebhookInput()
        .withExecutionBody(WEBHOOK_EXECUTION_BODY)
        .withExecutionUrl(WEBHOOK_EXECUTION_URL)
        .withWebhookConfigId(WEBHOOK_ID)
        .withConnectionContext(ConnectionContext().withOrganizationId(ORGANIZATION_ID))
        .withWorkspaceWebhookConfigs(Jsons.emptyObject())
    val t =
      Assertions.assertThrows(
        FailsafeException::class.java,
      ) { webhookActivity.invokeWebhook(input) }
    Assertions.assertEquals(exception, t.cause)
  }

  companion object {
    private const val WEBHOOK_EXECUTION_BODY = "fake-webhook-execution-body"
    private const val WEBHOOK_EXECUTION_URL = "https://example.com"
    private val ORGANIZATION_ID: UUID = UUID.randomUUID()
    private val WEBHOOK_ID: UUID = UUID.randomUUID()
    private const val WEBHOOK_AUTH_TOKEN = "fake-auth-token"
    private val WORKSPACE_WEBHOOK_CONFIGS =
      WebhookOperationConfigs().withWebhookConfigs(
        listOf(
          WebhookConfig().withId(WEBHOOK_ID).withAuthToken(WEBHOOK_AUTH_TOKEN),
        ),
      )
  }
}
