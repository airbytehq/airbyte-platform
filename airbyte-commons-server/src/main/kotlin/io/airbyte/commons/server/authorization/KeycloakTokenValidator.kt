/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.authorization

import com.nimbusds.jwt.JWT
import com.nimbusds.jwt.JWTClaimsSet
import com.nimbusds.jwt.PlainJWT
import io.airbyte.commons.auth.RequiresAuthMode
import io.airbyte.commons.auth.config.AuthMode
import io.airbyte.commons.auth.support.JwtTokenParser.convertJwtPayloadToUserAttributes
import io.airbyte.commons.auth.support.JwtTokenParser.getJwtPayloadToken
import io.airbyte.commons.json.Jsons
import io.airbyte.data.services.impls.keycloak.AirbyteKeycloakClient
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.MetricTags.AUTHENTICATION_REQUEST_URI_ATTRIBUTE_KEY
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.common.util.StringUtils
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Requires
import io.micronaut.core.order.Ordered
import io.micronaut.http.HttpRequest
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.authentication.AuthenticationException
import io.micronaut.security.token.jwt.validator.JwtAuthenticationFactory
import io.micronaut.security.token.validator.TokenValidator
import jakarta.inject.Singleton
import org.reactivestreams.Publisher
import reactor.core.publisher.Mono
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.Optional

/**
 * Token Validator for Airbyte Cloud and Enterprise. Performs an online validation of the token
 * against the Keycloak server.
 */
@Singleton
@Primary
@RequiresAuthMode(AuthMode.OIDC) // We're not confident about what the identity-provider.type will be when keycloak *should* be
// enabled,
// (we think it's usually "oidc" or "keycloak"). We're more confident about when we definitely
// *don't* want it enabled,
// so here we rule out "generic-oidc" and "simple" explicitly. Otherwise, for now, keycloak is
// enabled.
@Requires(property = "airbyte.auth.identity-provider.type", notEquals = "generic-oidc")
@Requires(property = "airbyte.auth.identity-provider.type", notEquals = "simple")
class KeycloakTokenValidator(
  private val airbyteKeycloakClient: AirbyteKeycloakClient,
  private val authenticationFactory: JwtAuthenticationFactory,
  private val metricClient: Optional<MetricClient>,
) : TokenValidator<HttpRequest<*>> {
  /**
   * KeycloakTokenValidator should run first so that it can validate and add roles before other
   * built-in validators run.
   */
  override fun getOrder(): Int = Ordered.HIGHEST_PRECEDENCE

  override fun validateToken(
    token: String,
    request: HttpRequest<*>,
  ): Publisher<Authentication> =
    try {
      airbyteKeycloakClient.validateToken(token)
      log.debug { "Token is valid, will now getAuthentication for token" }
      Mono.just(getAuthentication(token, request))
    } catch (e: Exception) {
      // Pass to the next validator, if one exists
      log.debug(e) { "Token validation failed, passing to next validator" }
      metricClient.ifPresent { m: MetricClient ->
        m.count(
          OssMetricsRegistry.KEYCLOAK_TOKEN_VALIDATION,
          1L,
          AUTHENTICATION_FAILURE_METRIC_ATTRIBUTE,
          MetricAttribute(AUTHENTICATION_REQUEST_URI_ATTRIBUTE_KEY, request.uri.path),
        )
      }
      Mono.empty()
    }

  private fun getAuthentication(
    token: String,
    request: HttpRequest<*>,
  ): Authentication {
    val payload = getJwtPayloadToken(token)

    try {
      val jwtPayloadString = String(Base64.getUrlDecoder().decode(payload), StandardCharsets.UTF_8)
      val jwtPayload = Jsons.deserialize(jwtPayloadString)

      val userAttributeMap = convertJwtPayloadToUserAttributes(jwtPayload).toMutableMap()

      val authUserId = jwtPayload["sub"].asText()

      if (StringUtils.isNotBlank(authUserId)) {
        metricClient.ifPresent { m: MetricClient ->
          m.count(
            OssMetricsRegistry.KEYCLOAK_TOKEN_VALIDATION,
            1L,
            AUTHENTICATION_SUCCESS_METRIC_ATTRIBUTE,
            MetricAttribute(MetricTags.USER_TYPE, EXTERNAL_USER),
            MetricAttribute(AUTHENTICATION_REQUEST_URI_ATTRIBUTE_KEY, request.uri.path),
          )
        }

        val builder = JWTClaimsSet.Builder().subject(authUserId)
        for ((key, value) in userAttributeMap) {
          builder.claim(key, value)
        }
        val jwt: JWT = PlainJWT(builder.build())
        return authenticationFactory.createAuthentication(jwt).orElseThrow()
      } else {
        throw AuthenticationException("Failed to authenticate the user because the userId was blank.")
      }
    } catch (e: Exception) {
      log.error(e) { "Encountered an exception while validating the token." }
      throw AuthenticationException("Failed to authenticate the user.")
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}

    private const val EXTERNAL_USER = "external-user"
    private val AUTHENTICATION_FAILURE_METRIC_ATTRIBUTE = MetricAttribute(MetricTags.AUTHENTICATION_RESPONSE, "failure")
    private val AUTHENTICATION_SUCCESS_METRIC_ATTRIBUTE = MetricAttribute(MetricTags.AUTHENTICATION_RESPONSE, "success")
  }
}
