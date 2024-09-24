package io.airbyte.api.client.auth

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpHeaders
import io.micronaut.security.oauth2.client.clientcredentials.ClientCredentialsClient
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response
import reactor.core.publisher.Mono

private val logger = KotlinLogging.logger {}

/**
 * Interceptor that adds an access token to the request headers using the Keycloak client credentials flow.
 * Only enabled when the current application is configured with a Keycloak oauth2 client.
 * Micronaut will automatically inject the @Named client credentials client bean based on the
 * `micronaut.security.oauth2.clients.keycloak.*` properties that this interceptor requires.
 */
@Singleton
@Requires(property = "micronaut.security.oauth2.clients.keycloak.client-id", pattern = ".+")
@Requires(property = "micronaut.security.oauth2.clients.keycloak.client-secret", pattern = ".+")
@Requires(property = "micronaut.security.oauth2.clients.keycloak.openid.issuer", pattern = ".+")
class KeycloakAccessTokenInterceptor(
  @Named("keycloak") private val clientCredentialsClient: ClientCredentialsClient,
) : AirbyteApiInterceptor {
  override fun intercept(chain: Interceptor.Chain): Response =
    try {
      logger.debug { "Intercepting request to add Keycloak access token..." }
      val originalRequest: Request = chain.request()
      val builder: Request.Builder = originalRequest.newBuilder()
      val tokenResponse = Mono.from(clientCredentialsClient.requestToken()).block()
      val accessToken = tokenResponse?.accessToken
      if (accessToken != null) {
        builder.addHeader(HttpHeaders.AUTHORIZATION, "Bearer $accessToken")
        logger.debug { "Added access token to header $accessToken" }
        chain.proceed(builder.build())
      } else {
        logger.error { "Failed to obtain access token from Keycloak" }
        chain.proceed(originalRequest)
      }
    } catch (e: Exception) {
      logger.error(e) { "Failed to add Keycloak access token to request" }
      // do not throw exception, just proceed with the original request and let the request fail
      // authorization downstream.
      chain.proceed(chain.request())
    }
}
