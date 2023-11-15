package io.airbyte.commons.auth

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpHeaders
import jakarta.inject.Singleton
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response
import java.util.Base64

private val LOGGER = KotlinLogging.logger {}

@Singleton
class AuthenticationInterceptor(
  @Value("\${airbyte.workload-api.bearer-token}") private val bearerToken: String,
) : Interceptor {
  override fun intercept(chain: Interceptor.Chain): Response {
    val originalRequest: Request = chain.request()
    val builder: Request.Builder = originalRequest.newBuilder()

    builder.header(HttpHeaders.USER_AGENT, USER_AGENT_VALUE)

    if (bearerToken.isNotBlank()) {
      LOGGER.debug { "Adding authorization header..." }
      val encodedBearerToken = Base64.getEncoder().encodeToString(bearerToken.toByteArray())
      builder.header(HttpHeaders.AUTHORIZATION, "$BEARER_TOKEN_PREFIX $encodedBearerToken")
    } else {
      LOGGER.debug { "Bearer token not provided." }
    }

    return chain.proceed(builder.build())
  }

  companion object {
    const val USER_AGENT_VALUE = "WorkloadLauncherApp"
    const val BEARER_TOKEN_PREFIX = "Bearer"
  }
}
