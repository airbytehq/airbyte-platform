/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils

import com.nimbusds.jwt.JWTClaimsSet
import com.nimbusds.jwt.PlainJWT
import dev.failsafe.RetryPolicy
import dev.failsafe.event.ExecutionAttemptedEvent
import dev.failsafe.event.ExecutionCompletedEvent
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.AirbyteCatalog
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.client.model.generated.ConfiguredStreamMapper
import io.airbyte.api.client.model.generated.DestinationSyncMode
import io.airbyte.api.client.model.generated.SelectedFieldInfo
import io.airbyte.api.client.model.generated.SyncMode
import io.airbyte.commons.DEFAULT_USER_ID
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.security.token.jwt.signature.secret.SecretSignature
import io.micronaut.security.token.jwt.signature.secret.SecretSignatureConfiguration
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
   * This is a flag that can be used to enable/disable enterprise-only features in acceptance tests.
   */
  fun isEnterprise(): Boolean = System.getenv().getOrDefault(IS_ENTERPRISE, "false") == "true"

  fun getAirbyteApiUrl(): String {
    val host =
      System.getenv("AIRBYTE_SERVER_HOST").takeIf { !it.isNullOrBlank() }
        ?: throw Exception("AIRBYTE_SERVER_HOST is required")
    return "$host/api"
  }

  private fun JWTClaimsSet.toAuthHeader(): Map<String, String> {
    val jwtSignatureKey = System.getenv("AB_JWT_SIGNATURE_SECRET")
    if (jwtSignatureKey.isNullOrBlank()) {
      val token = PlainJWT(this).serialize()
      return mapOf("Authorization" to "Bearer $token")
    }

    val secretSignatureConfig = SecretSignatureConfiguration("airbyte-internal-api")
    secretSignatureConfig.secret = jwtSignatureKey
    val secretSignature = SecretSignature(secretSignatureConfig)
    val token = secretSignature.sign(this).serialize()
    return mapOf("Authorization" to "Bearer $token")
  }

  private fun getAdminAuthHeaders() =
    JWTClaimsSet
      .Builder()
      .subject(DEFAULT_USER_ID.toString())
      .build()
      .toAuthHeader()

  fun createAirbyteAdminApiClient(): AirbyteApiClient =
    AirbyteApiClient(
      basePath = getAirbyteApiUrl(),
      policy = createRetryPolicy(),
      httpClient = createOkHttpClient(getAdminAuthHeaders()),
    )

  fun createAirbyteUserApiClient(
    email: String,
    verified: Boolean = true,
  ): AirbyteApiClient {
    val headers =
      JWTClaimsSet
        .Builder()
        .subject(email)
        .claim("user_id", email)
        .claim("user_email", email)
        .claim("email_verified", verified)
        .build()
        .toAuthHeader()

    return AirbyteApiClient(
      basePath = getAirbyteApiUrl(),
      policy = createRetryPolicy(),
      httpClient = createOkHttpClient(headers),
    )
  }

  fun createOkHttpClient(headers: Map<String, String> = getAdminAuthHeaders()) =
    OkHttpClient
      .Builder()
      .addInterceptor(
        Interceptor { chain ->
          val request = chain.request()
          val builder: Request.Builder = request.newBuilder()
          headers.entries.stream().forEach { e: Map.Entry<String, String> -> builder.addHeader(e.key, e.value) }
          chain.proceed(builder.build())
        },
      ).addInterceptor(LoggingInterceptor)
      .connectTimeout(Duration.ofSeconds(DEFAULT_TIMEOUT))
      .readTimeout(Duration.ofSeconds(DEFAULT_TIMEOUT))
      .build()

  private fun createRetryPolicy(): RetryPolicy<Response> =
    RetryPolicy
      .builder<Response>()
      .handle(
        listOf<Class<out Throwable>>(
          IllegalStateException::class.java,
          IOException::class.java,
          UnsupportedOperationException::class.java,
          ClientException::class.java,
          ServerException::class.java,
        ),
      ).onRetry { l: ExecutionAttemptedEvent<Response> ->
        logger.warn(l.lastException) { "Retry attempt ${l.attemptCount} of $MAX_TRIES. Last response: ${l.lastResult}" }
      }.onRetriesExceeded { l: ExecutionCompletedEvent<Response> ->
        logger.error(l.exception) { "Retry attempts exceeded." }
      }.withDelay(Duration.ofSeconds(JITTER_MAX_INTERVAL_SECS.toLong()))
      .withMaxRetries(MAX_TRIES)
      .build()

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
    mappers: List<ConfiguredStreamMapper>? = null,
  ): AirbyteCatalog {
    val updatedStreams: List<AirbyteStreamAndConfiguration> =
      originalCatalog
        ?.streams
        ?.stream()
        ?.map { s: AirbyteStreamAndConfiguration ->
          val config = s.config
          val newConfig =
            AirbyteStreamConfiguration(
              syncMode = replacementSourceSyncMode.orElse(config!!.syncMode),
              destinationSyncMode = replacementDestinationSyncMode.orElse(config.destinationSyncMode),
              cursorField = replacementCursorFields.orElse(config.cursorField),
              namespace = null,
              primaryKey = replacementPrimaryKeys.orElse(config.primaryKey),
              aliasName = config.aliasName,
              selected = replacementSelected.orElse(config.selected),
              suggested = config.suggested,
              destinationObjectName = config.destinationObjectName,
              includeFiles = config.includeFiles,
              fieldSelectionEnabled = replacementFieldSelectionEnabled.orElse(config.fieldSelectionEnabled),
              selectedFields = replacementSelectedFields.orElse(config.selectedFields),
              hashedFields = config.hashedFields,
              mappers = mappers ?: config.mappers,
              minimumGenerationId = replacementMinimumGenerationId.orElse(config.minimumGenerationId),
              generationId = replacementGenerationId.orElse(config.generationId),
              syncId = replacementSyncId.orElse(config.syncId),
            )
          AirbyteStreamAndConfiguration(s.stream, newConfig)
        }?.filter(streamFilter.orElse { _ -> true })
        ?.toList() ?: emptyList()
    return AirbyteCatalog(updatedStreams)
  }

  // logs http requests and responses
  private object LoggingInterceptor : Interceptor {
    override fun intercept(chain: Interceptor.Chain): Response {
      val request: Request = chain.request()
      // avoiding spamming ourselves with polling job status.
      val isJobStatus = isJobStatusRequest(request)

      if (!isJobStatus) {
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
      }

      val response: Response = chain.proceed(request)

      if (!isJobStatus) {
        // we don't log the response body because it can be very large which can heavily increase
        // the test duration + it doesn't add a lot of information
        logger.info { "Response: ${response.code} ${request.url} " }
      }

      return response
    }

    fun isJobStatusRequest(request: Request): Boolean = request.url.toString().contains("api/v1/jobs/get")
  }
}
