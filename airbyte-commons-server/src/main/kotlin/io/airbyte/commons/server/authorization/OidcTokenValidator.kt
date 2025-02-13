/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.authorization

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.airbyte.commons.auth.RequiresAuthMode
import io.airbyte.commons.auth.config.AuthMode
import io.airbyte.commons.auth.config.OidcEndpointConfig
import io.airbyte.commons.auth.config.OidcFieldMappingConfig
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.micronaut.cache.annotation.CacheConfig
import io.micronaut.cache.annotation.Cacheable
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Secondary
import io.micronaut.http.HttpRequest
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.token.validator.TokenValidator
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import okhttp3.Request
import org.apache.http.HttpHeaders
import org.reactivestreams.Publisher
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono.empty
import reactor.core.publisher.Mono.just
import java.util.Optional

@Singleton
@Secondary
@RequiresAuthMode(AuthMode.OIDC)
@Requires(property = "airbyte.auth.identity-provider.type", value = "generic-oidc")
@Replaces(KeycloakTokenValidator::class)
@CacheConfig("user-info-endpoint-response")
open class OidcTokenValidator(
  // Config
  private val endpointConfig: OidcEndpointConfig,
  private val fieldMappingConfig: OidcFieldMappingConfig,
  // Clients
  @Named("keycloakTokenValidatorHttpClient")
  private val client: OkHttpClient,
  private val metricClient: Optional<MetricClient>,
  private val tokenRoleResolver: TokenRoleResolver,
) : TokenValidator<HttpRequest<Any>> {
  companion object {
    val log: Logger = LoggerFactory.getLogger(OidcTokenValidator::class.java)
    const val EXTERNAL_USER: String = "external-user"
  }

  /**
   * Validate the Token in the following way:
   * 1. Call the configured UserInfo endpoint with the Token. If that request is not a 200, fail.
   * 2. Take the UserInfo response JSON and map that into our userAttributesMap
   *    * This response is cached for a short amount of time (1m) as Commercial OIDC providers have
   *      rate limits on the UserInfo endpoint, e.g. 10 req/minute by UserId.
   * 3. Find the Roles associated with the user and build the Authentication Response
   */
  override fun validateToken(
    token: String?,
    request: HttpRequest<Any>,
  ): Publisher<Authentication> {
    // Fail fast if the token is null or blank
    if (token.isNullOrBlank()) {
      return empty()
    }

    val authentication: Authentication? = validateTokenWithUserInfoEndpoint(token = token, request = request)
    authentication?.let {
      updateMetric(success = true, request = request)
      return just(authentication)
    }

    // pass to the next validator, if one exists
    log.debug("Token was not a valid token: {}", token)
    updateMetric(success = false, request = request)
    return empty()
  }

  /**
   * Takes the raw token and makes a request to the UserInfo endpoint. The response is then parsed
   * and mapped into our userAttributesMap. If that parsing fails, an Empty Optional is returned instead.
   */
  @Cacheable
  open fun validateTokenWithUserInfoEndpoint(
    token: String,
    request: HttpRequest<Any>,
  ): Authentication? {
    log.debug("Validating token: $token\nwith endpoint ${endpointConfig.userInfoEndpoint}")

    client
      .newCall(
        Request
          .Builder()
          .addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
          .addHeader(HttpHeaders.AUTHORIZATION, "Bearer $token")
          .url(endpointConfig.userInfoEndpoint)
          .get()
          .build(),
      ).execute()
      .use { response ->
        if (response.isSuccessful && response.body != null) {
          return convertUserInfoResponseToAuthentication(response = response.body!!.string(), request = request)
        }
        log.warn("The response from the userinfo endpoint was not valid. The status code was: ${response.code} with body: \n${response.body}")
        return null
      }
  }

  /**
   * Going through UserInfo response and extract fields our backend cares about into an Authentication object.
   */
  private fun convertUserInfoResponseToAuthentication(
    response: String,
    request: HttpRequest<Any>,
  ): Authentication? {
    log.debug("Response from userinfo endpoint: {}", response)
    val userInfoMap: Map<String, String> = jacksonObjectMapper().readValue(response)
    log.debug("Authenticating with jwtmap {}: ", userInfoMap)

    // Validate the payload, everything downstream can assume the map is correct.
    if (userInfoMapIsValid(userInfoMap)) {
      return Authentication.build(
        userInfoMap[fieldMappingConfig.sub],
        tokenRoleResolver.resolveRoles(
          userInfoMap[fieldMappingConfig.sub],
          request,
        ),
        userInfoMap,
      )
    }
    return null
  }

  /**
   * Check that the UserInfo map contains all the key/value pairs we care about.
   */
  private fun userInfoMapIsValid(userInfoMap: Map<String, String>): Boolean =
    if (userInfoMap[fieldMappingConfig.name].isNullOrBlank()) {
      log.warn("The token did not contain a claim for key ${fieldMappingConfig.name}")
      false
    } else if (userInfoMap[fieldMappingConfig.email].isNullOrBlank()) {
      log.warn("The token did not contain a claim for key ${fieldMappingConfig.email}")
      false
    } else if (userInfoMap[fieldMappingConfig.sub].isNullOrBlank()) {
      log.warn("The token did not contain a claim for key ${fieldMappingConfig.sub}")
      false
    } else {
      true
    }

  /**
   * Helper method for reporting success/failure metrics.
   */
  private fun updateMetric(
    success: Boolean,
    request: HttpRequest<Any>,
  ) {
    metricClient.ifPresent { m: MetricClient ->
      m.count(
        metric = OssMetricsRegistry.OIDC_TOKEN_VALIDATION,
        attributes =
          arrayOf(
            if (success) {
              MetricAttribute(MetricTags.AUTHENTICATION_RESPONSE, "success")
            } else {
              MetricAttribute(MetricTags.AUTHENTICATION_RESPONSE, "failure")
            },
            MetricAttribute(MetricTags.USER_TYPE, EXTERNAL_USER),
            MetricAttribute(MetricTags.AUTHENTICATION_REQUEST_URI_ATTRIBUTE_KEY, request.uri.path),
          ),
      )
    }
  }
}
