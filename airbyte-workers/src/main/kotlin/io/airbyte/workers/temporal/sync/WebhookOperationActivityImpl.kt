/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync

import com.fasterxml.jackson.databind.JsonNode
import datadog.trace.api.Trace
import dev.failsafe.Failsafe
import dev.failsafe.RetryPolicy
import dev.failsafe.function.CheckedSupplier
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ScopeType
import io.airbyte.api.client.model.generated.SecretPersistenceConfigGetRequestBody
import io.airbyte.commons.json.Jsons
import io.airbyte.config.OperatorWebhookInput
import io.airbyte.config.WebhookConfig
import io.airbyte.config.WebhookOperationConfigs
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.UseRuntimeSecretPersistence
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.WEBHOOK_CONFIG_ID_KEY
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.workers.helper.toModel
import io.micronaut.http.HttpStatus
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.temporal.ChronoUnit
import java.util.Map
import java.util.UUID

/**
 * Webhook operation activity temporal impl.
 */
@Singleton
class WebhookOperationActivityImpl(
  @param:Named("webhookHttpClient") private val httpClient: HttpClient,
  private val secretsRepositoryReader: SecretsRepositoryReader,
  private val airbyteApiClient: AirbyteApiClient,
  private val featureFlagClient: FeatureFlagClient,
  private val metricClient: MetricClient,
) : WebhookOperationActivity {
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun invokeWebhook(input: OperatorWebhookInput): Boolean {
    metricClient.count(OssMetricsRegistry.ACTIVITY_WEBHOOK_OPERATION)

    LOGGER.debug("Webhook operation input: {}", input)
    LOGGER.debug("Found webhook config: {}", input.getWorkspaceWebhookConfigs())

    val fullWebhookConfigJson: JsonNode?
    val organizationId = input.getConnectionContext().getOrganizationId()
    if (organizationId != null && featureFlagClient.boolVariation(UseRuntimeSecretPersistence, Organization(organizationId))) {
      try {
        val secretPersistenceConfig =
          airbyteApiClient.secretPersistenceConfigApi.getSecretsPersistenceConfig(
            SecretPersistenceConfigGetRequestBody(ScopeType.ORGANIZATION, organizationId),
          )
        val runtimeSecretPersistence =
          RuntimeSecretPersistence(secretPersistenceConfig.toModel(), metricClient)
        fullWebhookConfigJson =
          secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(input.getWorkspaceWebhookConfigs(), runtimeSecretPersistence)
      } catch (e: IOException) {
        LOGGER.error("Unable to retrieve hydrated webhook configuration for webhook {}.", input.getWebhookConfigId(), e)
        return false
      }
    } else {
      fullWebhookConfigJson = secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(input.getWorkspaceWebhookConfigs())
    }
    val webhookConfigs: WebhookOperationConfigs = Jsons.`object`(fullWebhookConfigJson!!, WebhookOperationConfigs::class.java)
    val webhookConfig =
      webhookConfigs
        .getWebhookConfigs()
        .stream()
        .filter { config: WebhookConfig? -> config!!.getId() == input.getWebhookConfigId() }
        .findFirst()

    if (webhookConfig.isPresent()) {
      ApmTraceUtils.addTagsToTrace(Map.of<String?, UUID?>(WEBHOOK_CONFIG_ID_KEY, input.getWebhookConfigId()))
      LOGGER.info("Invoking webhook operation \"{}\" using URL \"{}\"...", webhookConfig.get().getName(), input.getExecutionUrl())
      val requestBuilder = buildRequest(input, webhookConfig.get())
      return Failsafe
        .with<Any?, RetryPolicy<Any?>?>(WEBHOOK_RETRY_POLICY)
        .get<Boolean>(CheckedSupplier { sendWebhook(requestBuilder, webhookConfig.get()) })
    } else {
      LOGGER.error("Webhook configuration for webhook {} not found.", input.getWebhookConfigId())
      return false
    }
  }

  private fun buildRequest(
    input: OperatorWebhookInput,
    webhookConfig: WebhookConfig,
  ): HttpRequest.Builder {
    val requestBuilder =
      HttpRequest
        .newBuilder()
        .uri(URI.create(input.getExecutionUrl()))
    if (input.getExecutionBody() != null) {
      requestBuilder.POST(HttpRequest.BodyPublishers.ofString(input.getExecutionBody()))
    }
    if (webhookConfig.getAuthToken() != null) {
      requestBuilder
        .header("Content-Type", "application/json")
        .header("Authorization", "Bearer " + webhookConfig.getAuthToken())
        .build()
    }

    return requestBuilder
  }

  @Throws(IOException::class, InterruptedException::class)
  private fun sendWebhook(
    requestBuilder: HttpRequest.Builder,
    webhookConfig: WebhookConfig,
  ): Boolean {
    val response = this.httpClient.send<String?>(requestBuilder.build(), HttpResponse.BodyHandlers.ofString())
    if (response != null) {
      val result = response.statusCode() >= HttpStatus.OK.getCode() && response.statusCode() <= HttpStatus.MULTIPLE_CHOICES.getCode()
      if (result) {
        LOGGER.info("Webhook operation \"{}\" ({}) successful.", webhookConfig.getName(), webhookConfig.getId())
      } else {
        LOGGER.info(
          "Webhook operation \"{}\" ({}) response: {} {}",
          webhookConfig.getName(),
          webhookConfig.getId(),
          response.statusCode(),
          response.body(),
        )
      }
      return result
    } else {
      LOGGER.info("Webhook operation did not return a response.  Reporting invocation as failed...")
      return false
    }
  }

  companion object {
    private val LOGGER: Logger = LoggerFactory.getLogger(WebhookOperationActivityImpl::class.java)
    private const val MAX_RETRIES = 3

    private val WEBHOOK_RETRY_POLICY: RetryPolicy<Any?> =
      RetryPolicy
        .builder<Any?>()
        .withBackoff(1, 5, ChronoUnit.SECONDS)
        .withMaxRetries(MAX_RETRIES)
        .handle(Exception::class.java)
        .build()
  }
}
