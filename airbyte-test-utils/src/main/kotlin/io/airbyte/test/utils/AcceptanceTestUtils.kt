/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.test.utils

import dev.failsafe.RetryPolicy
import dev.failsafe.event.ExecutionAttemptedEvent
import dev.failsafe.event.ExecutionCompletedEvent
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.AirbyteCatalog
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.client.model.generated.DestinationSyncMode
import io.airbyte.api.client.model.generated.SelectedFieldInfo
import io.airbyte.api.client.model.generated.SyncMode
import io.airbyte.commons.auth.AirbyteAuthConstants
import io.github.oshai.kotlinlogging.KotlinLogging
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okio.Buffer
import org.openapitools.client.infrastructure.ClientException
import org.openapitools.client.infrastructure.ServerException
import java.io.IOException
import java.time.Duration
import java.util.Optional
import java.util.function.Predicate

private val logger = KotlinLogging.logger {}

object AcceptanceTestUtils {
  private const val JITTER_MAX_INTERVAL_SECS: Int = 10
  private const val MAX_TRIES: Int = 3
  private const val DEFAULT_TIMEOUT: Long = 60L

  // TODO(mfsiega-airbyte): clean up and centralize the way we do config.
  private const val IS_ENTERPRISE: String = "IS_ENTERPRISE"
  const val IS_GKE: String = "IS_GKE"

  /**
   * test-client is a valid internal service name according to the AirbyteAuthInternalTokenValidator.
   * This header value can be used to set up Acceptance Test clients that can authorize as an instance
   * admin. This is useful for testing Enterprise features without needing to set up a valid Keycloak
   * token.
   */
  private const val X_AIRBYTE_AUTH_HEADER_TEST_CLIENT_VALUE: String = AirbyteAuthConstants.X_AIRBYTE_AUTH_HEADER_INTERNAL_PREFIX + " test-client"

  /**
   * This is a flag that can be used to enable/disable enterprise-only features in acceptance tests.
   */
  @JvmStatic
  fun isEnterprise(): Boolean {
    return System.getenv().getOrDefault(IS_ENTERPRISE, "false") == "true"
  }

  @JvmStatic
  @JvmOverloads
  fun createAirbyteApiClient(
    basePath: String,
    headers: Map<String, String> = mapOf(),
    applyHeaders: Boolean = System.getenv().containsKey(IS_GKE),
  ): AirbyteApiClient {
    // Set up the API client.
    return AirbyteApiClient(
      basePath = basePath,
      policy = createRetryPolicy(),
      httpClient =
        createOkHttpClient(
          applyHeaders = applyHeaders,
          headers = headers,
        ),
    )
  }

  @JvmStatic
  @JvmOverloads
  fun createOkHttpClient(
    applyHeaders: Boolean = System.getenv().containsKey(IS_GKE),
    headers: Map<String, String> = mapOf(),
  ): OkHttpClient {
    val okHttpClient: OkHttpClient =
      OkHttpClient.Builder()
        .addInterceptor(
          Interceptor { chain ->
            val request = chain.request()
            val builder: Request.Builder = request.newBuilder()
            if (applyHeaders) {
              headers.entries.stream().forEach { e: Map.Entry<String, String> -> builder.addHeader(e.key, e.value) }
            }
            if (isEnterprise()) {
              builder.addHeader(AirbyteAuthConstants.X_AIRBYTE_AUTH_HEADER, X_AIRBYTE_AUTH_HEADER_TEST_CLIENT_VALUE)
            }
            chain.proceed(builder.build())
          },
        )
        .addInterceptor(LoggingInterceptor)
        .connectTimeout(Duration.ofSeconds(DEFAULT_TIMEOUT))
        .readTimeout(Duration.ofSeconds(DEFAULT_TIMEOUT))
        .build()
    return okHttpClient
  }

  @JvmStatic
  fun createRetryPolicy(): RetryPolicy<Response> {
    return RetryPolicy.builder<Response>()
      .handle(
        listOf<Class<out Throwable>>(
          IllegalStateException::class.java,
          IOException::class.java,
          UnsupportedOperationException::class.java,
          ClientException::class.java,
          ServerException::class.java,
        ),
      )
      .onRetry { l: ExecutionAttemptedEvent<Response> ->
        logger.warn(l.lastException) { "Retry attempt ${l.attemptCount} of $MAX_TRIES. Last response: ${l.lastResult}" }
      }
      .onRetriesExceeded { l: ExecutionCompletedEvent<Response> ->
        logger.error(l.exception) { "Retry attempts exceeded." }
      }
      .withDelay(Duration.ofSeconds(JITTER_MAX_INTERVAL_SECS.toLong()))
      .withMaxRetries(MAX_TRIES)
      .build()
  }

  @JvmStatic
  fun modifyCatalog(
    originalCatalog: AirbyteCatalog?,
    // TODO replace Optional with nullable values and leverage ?.let { } ?: config.<other value>
    replacementSourceSyncMode: Optional<SyncMode> = Optional.empty(),
    replacementDestinationSyncMode: Optional<DestinationSyncMode> = Optional.empty(),
    replacementCursorFields: Optional<List<String>> = Optional.empty(),
    replacementPrimaryKeys: Optional<List<List<String>>> = Optional.empty(),
    replacementSelected: Optional<Boolean> = Optional.empty(),
    replacementFieldSelectionEnabled: Optional<Boolean> = Optional.empty(),
    replacementSelectedFields: Optional<List<SelectedFieldInfo>> = Optional.empty(),
    replacementMinimumGenerationId: Optional<Long> = Optional.empty(),
    replacementGenerationId: Optional<Long> = Optional.empty(),
    replacementSyncId: Optional<Long> = Optional.empty(),
    streamFilter: Optional<Predicate<AirbyteStreamAndConfiguration>> = Optional.empty(),
  ): AirbyteCatalog {
    val updatedStreams: List<AirbyteStreamAndConfiguration> =
      originalCatalog?.streams?.stream()?.map { s: AirbyteStreamAndConfiguration ->
        val config = s.config
        val newConfig =
          AirbyteStreamConfiguration(
            replacementSourceSyncMode.orElse(config!!.syncMode),
            replacementDestinationSyncMode.orElse(config.destinationSyncMode),
            replacementCursorFields.orElse(config.cursorField),
            replacementPrimaryKeys.orElse(config.primaryKey),
            config.aliasName,
            replacementSelected.orElse(config.selected),
            config.suggested,
            replacementFieldSelectionEnabled.orElse(config.fieldSelectionEnabled),
            replacementSelectedFields.orElse(config.selectedFields),
            config.hashedFields,
            config.mappers,
            replacementMinimumGenerationId.orElse(config.minimumGenerationId),
            replacementGenerationId.orElse(config.generationId),
            replacementSyncId.orElse(config.syncId),
          )
        AirbyteStreamAndConfiguration(s.stream, newConfig)
      }?.filter(streamFilter.orElse { true })?.toList() ?: emptyList()
    return AirbyteCatalog(updatedStreams)
  }

  // logs http requests and responses
  private object LoggingInterceptor : Interceptor {
    override fun intercept(chain: Interceptor.Chain): Response {
      val request: Request = chain.request()
      val requestLogMessage =
        buildString {
          append("Request: ${request.method} ${request.url}")
          request.body?.let { body ->
            val buffer = Buffer()
            body.writeTo(buffer)
            append(" body: ${buffer.readUtf8()}")
          }
        }
      logger.info { requestLogMessage }

      val response: Response = chain.proceed(request)
      // we don't log the response body because it can be very large which can heavily increase
      // the test duration + it doesn't add a lot of information
      logger.info { "Response: ${response.code} ${request.url} " }

      return response
    }
  }
}
