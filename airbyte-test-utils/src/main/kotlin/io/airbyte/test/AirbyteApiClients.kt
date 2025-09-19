/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test

import com.nimbusds.jwt.JWTClaimsSet
import com.nimbusds.jwt.PlainJWT
import dev.failsafe.RetryPolicy
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.ApiException
import io.airbyte.api.client.generated.ActorDefinitionVersionApi
import io.airbyte.api.client.generated.AttemptApi
import io.airbyte.api.client.generated.BillingApi
import io.airbyte.api.client.generated.CommandApi
import io.airbyte.api.client.generated.PublicDeclarativeSourceDefinitionsApi
import io.airbyte.api.client.generated.PublicDestinationDefinitionsApi
import io.airbyte.api.client.generated.PublicSourceDefinitionsApi
import io.airbyte.api.client.generated.PublicWorkspacesApi
import io.airbyte.api.client.model.generated.CreateDefinitionRequest
import io.airbyte.commons.DEFAULT_USER_ID
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.security.token.jwt.signature.secret.SecretSignature
import io.micronaut.security.token.jwt.signature.secret.SecretSignatureConfiguration
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.logging.HttpLoggingInterceptor
import okio.Buffer
import org.openapitools.client.infrastructure.ClientException
import org.openapitools.client.infrastructure.ServerException
import java.io.IOException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

private val log = KotlinLogging.logger {}

internal object AirbyteApiClients {
  fun adminApiClient(host: String): AirbyteApiClient {
    val headers =
      JWTClaimsSet
        .Builder()
        .subject(DEFAULT_USER_ID.toString())
        .build()
        .toAuthHeader()

    return AirbyteApiClient(
      basePath = "$host/api",
      policy = retryPolicy,
      httpClient = okHttpClient(headers),
    )
  }

  fun userApiClient(
    host: String,
    email: String,
  ): AirbyteApiClient {
    val headers =
      JWTClaimsSet
        .Builder()
        .subject(email)
        .claim("user_id", email)
        .claim("user_email", email)
        .claim("email_verified", true)
        .build()
        .toAuthHeader()

    return AirbyteApiClient(
      basePath = "$host/api",
      policy = retryPolicy,
      httpClient = okHttpClient(headers),
    )
  }

  fun publicApiClient(host: String): PublicApiClient =
    PublicApiClient(
      basePath = "$host/api",
      httpClient = okHttpClient(),
    )
}

class PublicApiClient(
  basePath: String,
  httpClient: OkHttpClient,
) {
  val sourceDefinitionsApi = PublicSourceDefinitionsApi(basePath = basePath, client = httpClient)
  val destinationDefinitionsApi = PublicDestinationDefinitionsApi(basePath = basePath, client = httpClient)
  val declarativeSourceDefinitionsApi = PublicDeclarativeSourceDefinitionsApi(basePath = basePath, client = httpClient)
  val workspaceApi = PublicWorkspacesApi(basePath = basePath, client = httpClient)
}

private val retryPolicy: RetryPolicy<Response> =
  run {
    val maxTries = 3
    val maxJitter = 10.seconds

    RetryPolicy
      .builder<Response>()
      .handle(
        listOf(
          IllegalStateException::class.java,
          IOException::class.java,
          UnsupportedOperationException::class.java,
          ApiException::class.java,
          ClientException::class.java,
          ServerException::class.java,
        ),
      ).onRetry {
        log.warn(it.lastException) { "Retry attempt ${it.attemptCount} of $maxTries. Last response: ${it.lastResult}" }
      }.onRetriesExceeded {
        log.error(it.exception) { "Retry attempts exceeded." }
      }.withDelay(maxJitter.toJavaDuration())
      .withMaxRetries(maxTries)
      .build()
  }

private val loggingInterceptor: Interceptor =
  Interceptor { chain ->
    val request: Request = chain.request()
    // avoiding spamming ourselves with polling job status.
    val isJobStatus = request.url.toString().contains("api/v1/jobs/get")

    if (!isJobStatus) {
      val requestLogMessage =
        buildString {
          append("Request: ${request.method} ${request.url}")
          request.body?.let {
            val buffer = Buffer()
            it.writeTo(buffer)
            append(" body: ${buffer.readUtf8()}")
          }
        }
      log.info { requestLogMessage }
    }

    val response: Response = chain.proceed(request)

    if (!isJobStatus) {
      // we don't log the response body because it can be very large which can heavily increase the test duration
      // and it doesn't add a lot of information
      log.info { "Response: ${response.code} ${request.url} " }
    }

    response
  }

private fun okHttpClient(headers: Map<String, String> = emptyMap()): OkHttpClient {
  val timeout = 1.minutes
  // set the request logging level based on the log level
//  val requestLogLevel =
//    when {
//      log.isDebugEnabled() -> HttpLoggingInterceptor.Level.BODY
//      log.isInfoEnabled() -> HttpLoggingInterceptor.Level.BASIC
//      else -> HttpLoggingInterceptor.Level.NONE
//    }

  return OkHttpClient
    .Builder()
    .connectTimeout(timeout.toJavaDuration())
    .readTimeout(timeout.toJavaDuration())
    .addInterceptor(loggingInterceptor)
//    .addInterceptor(HttpLoggingInterceptor().setLevel(requestLogLevel))
    .addInterceptor(
      Interceptor { chain ->
        val request = chain.request()
        val builder: Request.Builder = request.newBuilder()
        headers.forEach { builder.addHeader(it.key, it.value) }
        chain.proceed(builder.build())
      },
    ).build()
}

private fun JWTClaimsSet.toAuthHeader(): Map<String, String> {
  val jwtSignatureKey = System.getenv("AB_JWT_SIGNATURE_SECRET")
  if (jwtSignatureKey.isNullOrBlank()) {
    val token = PlainJWT(this).serialize()
    return mapOf("Authorization" to "Bearer $token")
  }

  val secretSignatureConfig =
    SecretSignatureConfiguration("airbyte-internal-api").apply {
      secret = jwtSignatureKey
    }
  val secretSignature = SecretSignature(secretSignatureConfig)
  val token = secretSignature.sign(this).serialize()
  return mapOf("Authorization" to "Bearer $token")
}
