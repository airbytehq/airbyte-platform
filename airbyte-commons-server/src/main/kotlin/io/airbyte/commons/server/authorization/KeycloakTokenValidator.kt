/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.authorization

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.nimbusds.jwt.JWT
import com.nimbusds.jwt.JWTClaimsSet
import com.nimbusds.jwt.PlainJWT
import io.airbyte.commons.auth.RequiresAuthMode
import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration
import io.airbyte.commons.auth.config.AuthMode
import io.airbyte.commons.auth.roles.AuthRole.Companion.getInstanceAdminRoles
import io.airbyte.commons.auth.support.JwtTokenParser.JWT_SSO_REALM
import io.airbyte.commons.auth.support.JwtTokenParser.convertJwtPayloadToUserAttributes
import io.airbyte.commons.auth.support.JwtTokenParser.getJwtPayloadToken
import io.airbyte.commons.auth.support.JwtTokenParser.tokenToAttributes
import io.airbyte.commons.json.Jsons
import io.airbyte.data.auth.TokenType
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
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import okhttp3.Request
import org.apache.http.HttpHeaders
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
  @param:Named("keycloakTokenValidatorHttpClient") private val client: OkHttpClient,
  private val keycloakConfiguration: AirbyteKeycloakConfiguration,
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
  ): Publisher<Authentication> {
    return validateTokenWithKeycloak(token)
      .flatMap<Authentication> { valid: Boolean ->
        if (valid) {
          log.debug("Token is valid, will now getAuthentication for token: {}", token)
          return@flatMap Mono.just<Authentication>(
            getAuthentication(
              token,
              request,
            ),
          )
        } else {
          // pass to the next validator, if one exists
          log.debug("Token was not a valid Keycloak token: {}", token)
          metricClient.ifPresent { m: MetricClient ->
            m.count(
              OssMetricsRegistry.KEYCLOAK_TOKEN_VALIDATION,
              1L,
              AUTHENTICATION_FAILURE_METRIC_ATTRIBUTE,
              MetricAttribute(AUTHENTICATION_REQUEST_URI_ATTRIBUTE_KEY, request.uri.path),
            )
          }
          return@flatMap Mono.empty<Authentication>()
        }
      }
  }

  private fun getAuthentication(
    token: String,
    request: HttpRequest<*>,
  ): Authentication {
    val payload = getJwtPayloadToken(token)

    try {
      val jwtPayloadString = String(Base64.getUrlDecoder().decode(payload), StandardCharsets.UTF_8)
      val jwtPayload = Jsons.deserialize(jwtPayloadString)
      log.debug("jwtPayload: {}", jwtPayload)

      val userAttributeMap = convertJwtPayloadToUserAttributes(jwtPayload).toMutableMap()

      if (isInternalServiceAccount(userAttributeMap)) {
        log.debug("Performing authentication for internal service account...")
        val clientName = jwtPayload["azp"].asText()
        val tokenTypeClaim = TokenType.LEGACY_KEYCLOAK_SERVICE_ACCOUNT.toClaim()
        userAttributeMap[tokenTypeClaim.component1()] = tokenTypeClaim.component2()
        metricClient.ifPresent { m: MetricClient ->
          m.count(
            OssMetricsRegistry.KEYCLOAK_TOKEN_VALIDATION,
            1L,
            AUTHENTICATION_SUCCESS_METRIC_ATTRIBUTE,
            MetricAttribute(MetricTags.USER_TYPE, INTERNAL_SERVICE_ACCOUNT),
            MetricAttribute(MetricTags.CLIENT_ID, clientName),
            MetricAttribute(AUTHENTICATION_REQUEST_URI_ATTRIBUTE_KEY, request.uri.path),
          )
        }
        return Authentication.build(clientName, getInstanceAdminRoles(), userAttributeMap)
      }

      val authUserId = jwtPayload["sub"].asText()
      log.debug("Performing authentication for auth user '{}'...", authUserId)

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
      log.error("Encountered an exception while validating the token.", e)
      throw AuthenticationException("Failed to authenticate the user.")
    }
  }

  private fun isInternalServiceAccount(jwtAttributes: Map<String, Any>): Boolean {
    val realm = jwtAttributes[JWT_SSO_REALM] as String?
    return keycloakConfiguration.internalRealm == realm
  }

  private fun validateTokenWithKeycloak(token: String): Mono<Boolean> {
    val realm: String?
    try {
      val jwtAttributes = tokenToAttributes(token)
      realm = jwtAttributes[JWT_SSO_REALM] as String?
      log.debug("Extracted realm {}", realm)
    } catch (e: Exception) {
      log.debug("Failed to parse realm from JWT token: {}", token, e)
      return Mono.just(false)
    }

    if (realm == null) {
      log.debug("Unable to extract realm from token {}", token)
      return Mono.just(false)
    }

    val userInfoEndpoint = keycloakConfiguration.getKeycloakUserInfoEndpointForRealm(realm)
    log.debug("Validating token with Keycloak userinfo endpoint: {}", userInfoEndpoint)

    val request =
      Request
        .Builder()
        .addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        .addHeader(HttpHeaders.AUTHORIZATION, "Bearer $token")
        .url(userInfoEndpoint)
        .get()
        .build()

    try {
      client.newCall(request).execute().use { response ->
        if (response.isSuccessful) {
          checkNotNull(response.body)
          val responseBody = response.body!!.string()
          log.debug("Response from userinfo endpoint: {}", responseBody)
          return validateUserInfo(responseBody)
        } else {
          log.debug("Non-200 response from userinfo endpoint: {}", response.code)
          return Mono.just(false)
        }
      }
    } catch (e: Exception) {
      log.error("Failed to validate access token.", e)
      return Mono.error(e)
    }
  }

  private fun validateUserInfo(responseBody: String): Mono<Boolean> {
    val objectMapper = ObjectMapper()
    try {
      val userInfo = objectMapper.readTree(responseBody)
      val sub = userInfo.path("sub").asText()
      log.debug("validated Keycloak sub: {}", sub)
      return Mono.just(StringUtils.isNotBlank(sub))
    } catch (e: JsonProcessingException) {
      log.error("Failed to process JSON.", e)
      return Mono.error(e)
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}

    private const val EXTERNAL_USER = "external-user"
    private const val INTERNAL_SERVICE_ACCOUNT = "internal-service-account"
    private val AUTHENTICATION_FAILURE_METRIC_ATTRIBUTE = MetricAttribute(MetricTags.AUTHENTICATION_RESPONSE, "failure")
    private val AUTHENTICATION_SUCCESS_METRIC_ATTRIBUTE = MetricAttribute(MetricTags.AUTHENTICATION_RESPONSE, "success")
  }
}
