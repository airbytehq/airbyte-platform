/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.mockito.Mockito
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import java.io.IOException
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.List
import java.util.UUID

internal class WebhookOperationActivityTest {
  private lateinit var webhookActivity: WebhookOperationActivity
  private lateinit var httpClient: HttpClient
  private lateinit var secretsRepositoryReader: SecretsRepositoryReader
  private lateinit var airbyteApiClient: AirbyteApiClient
  private lateinit var featureFlagClient: FeatureFlagClient

  @BeforeEach
  fun init() {
    httpClient = Mockito.mock(HttpClient::class.java)
    secretsRepositoryReader = Mockito.mock(SecretsRepositoryReader::class.java)
    airbyteApiClient = Mockito.mock(AirbyteApiClient::class.java)
    featureFlagClient = Mockito.mock(TestClient::class.java)
    webhookActivity =
      WebhookOperationActivityImpl(
        httpClient,
        secretsRepositoryReader,
        airbyteApiClient,
        featureFlagClient,
        Mockito.mock(MetricClient::class.java),
      )
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun webhookActivityInvokesConfiguredWebhook() {
    val mockHttpResponse = Mockito.mock(HttpResponse::class.java) as HttpResponse<Any>
    whenever(mockHttpResponse.statusCode()).thenReturn(HttpStatus.OK.code)
    whenever(
      secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(
        any<JsonNode>(),
      ),
    ).thenReturn(jsonNode(WORKSPACE_WEBHOOK_CONFIGS))
    val input =
      OperatorWebhookInput()
        .withExecutionBody(WEBHOOK_EXECUTION_BODY)
        .withExecutionUrl(WEBHOOK_EXECUTION_URL)
        .withWebhookConfigId(WEBHOOK_ID)
        .withConnectionContext(ConnectionContext().withOrganizationId(ORGANIZATION_ID))
        .withWorkspaceWebhookConfigs(Jsons.emptyObject())
    whenever(
      httpClient.send(
        any<HttpRequest>(),
        any<HttpResponse.BodyHandler<Any>>(),
      ),
    ).thenReturn(mockHttpResponse)
    val success = webhookActivity.invokeWebhook(input)
    Assertions.assertTrue(success)
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun webhookActivityFailsWhenRetriesExhausted() {
    val exception = IOException("test")
    whenever(
      httpClient.send(
        any<HttpRequest>(),
        any<HttpResponse.BodyHandler<Any>>(),
      ),
    ).thenThrow(exception)
    whenever(
      secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(
        any<JsonNode>(),
      ),
    ).thenReturn(jsonNode(WORKSPACE_WEBHOOK_CONFIGS))
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
        Executable { webhookActivity.invokeWebhook(input) },
      )
    Assertions.assertEquals(exception, t.cause)
  }

  companion object {
    private const val WEBHOOK_EXECUTION_BODY = "fake-webhook-execution-body"
    private const val WEBHOOK_EXECUTION_URL = "http://example.com"
    private val ORGANIZATION_ID: UUID = UUID.randomUUID()
    private val WEBHOOK_ID: UUID = UUID.randomUUID()
    private const val WEBHOOK_AUTH_TOKEN = "fake-auth-token"
    private val WORKSPACE_WEBHOOK_CONFIGS =
      WebhookOperationConfigs().withWebhookConfigs(
        List.of(
          WebhookConfig().withId(WEBHOOK_ID).withAuthToken(WEBHOOK_AUTH_TOKEN),
        ),
      )
  }
}
