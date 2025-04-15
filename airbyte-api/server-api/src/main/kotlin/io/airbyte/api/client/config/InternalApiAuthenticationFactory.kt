/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.config

import com.auth0.jwt.JWT
import com.auth0.jwt.JWTCreator
import com.google.auth.oauth2.ServiceAccountCredentials
import io.airbyte.api.client.auth.DataplaneAccessTokenInterceptor
import io.airbyte.api.client.auth.KeycloakAccessTokenInterceptor
import io.airbyte.commons.micronaut.EnvConstants
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Prototype
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.FileInputStream
import java.security.interfaces.RSAPrivateKey
import java.util.Date
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

@Factory
class InternalApiAuthenticationFactory {
  @Primary
  @Singleton
  @Requires(property = "airbyte.acceptance.test.enabled", value = "true", defaultValue = "false")
  @Named(INTERNAL_API_AUTH_TOKEN_BEAN_NAME)
  fun testInternalApiAuthToken(
    @Value("\${airbyte.internal-api.auth-header.value}") airbyteApiAuthHeaderValue: String,
  ): String = airbyteApiAuthHeaderValue

  @Singleton
  @Requires(property = "airbyte.acceptance.test.enabled", value = "false", defaultValue = "false")
  @Requires(env = [EnvConstants.CONTROL_PLANE])
  @Requires(missingBeans = [KeycloakAccessTokenInterceptor::class])
  @Named(INTERNAL_API_AUTH_TOKEN_BEAN_NAME)
  fun controlPlaneInternalApiAuthToken(
    @Value("\${airbyte.internal-api.auth-header.value}") airbyteApiAuthHeaderValue: String,
  ): String = airbyteApiAuthHeaderValue

  /**
   * Generate an auth token based on configs. This is called by the Api Client's requestInterceptor
   * for each request. Using Prototype annotation here to make sure each time it's used it will
   * generate a new JWT Signature if it's on data plane.
   *
   *
   * For Data Plane workers, generate a signed JWT as described here:
   * https://cloud.google.com/endpoints/docs/openapi/service-account-authentication
   */
  @Prototype
  @Requires(property = "airbyte.acceptance.test.enabled", value = "false", defaultValue = "false")
  @Requires(env = [EnvConstants.DATA_PLANE])
  @Requires(missingBeans = [KeycloakAccessTokenInterceptor::class, DataplaneAccessTokenInterceptor::class])
  @Named(INTERNAL_API_AUTH_TOKEN_BEAN_NAME)
  fun dataPlaneInternalApiAuthToken(
    @Value("\${airbyte.control.plane.auth-endpoint}") controlPlaneAuthEndpoint: String,
    @Value("\${airbyte.data.plane.service-account.email}") dataPlaneServiceAccountEmail: String,
    @Value("\${airbyte.data.plane.service-account.credentials-path}") dataPlaneServiceAccountCredentialsPath: String,
    metricClient: MetricClient,
  ): String {
    return try {
      val now = Date()
      val expTime =
        Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(JWT_TTL_MINUTES.toLong()))
      // Build the JWT payload
      val token: JWTCreator.Builder =
        JWT
          .create()
          .withIssuedAt(now)
          .withExpiresAt(expTime)
          .withIssuer(dataPlaneServiceAccountEmail)
          .withAudience(controlPlaneAuthEndpoint)
          .withSubject(dataPlaneServiceAccountEmail)
          .withClaim(CLAIM_NAME, dataPlaneServiceAccountEmail)

      // TODO multi-cloud phase 2: check performance of on-demand token generation in load testing. might
      // need to pull some of this outside of this method which is called for every API request
      val stream = FileInputStream(dataPlaneServiceAccountCredentialsPath)
      val cred = ServiceAccountCredentials.fromStream(stream)
      val key = cred.privateKey as RSAPrivateKey
      val algorithm: com.auth0.jwt.algorithms.Algorithm =
        com.auth0.jwt.algorithms.Algorithm
          .RSA256(null, key)
      val signedToken = token.sign(algorithm)
      metricClient.count(metric = OssMetricsRegistry.API_CLIENT_AUTH_TOKEN_SUCCESS)
      return "Bearer $signedToken"
    } catch (e: Exception) {
      metricClient.count(metric = OssMetricsRegistry.API_CLIENT_AUTH_TOKEN_FAILURE)
      logger.error(e) { "An issue occurred while generating a data plane auth token. Defaulting to empty string." }
      ""
    }
  }

  companion object {
    const val CLAIM_NAME = "email"
    const val INTERNAL_API_AUTH_TOKEN_BEAN_NAME = "internalApiAuthToken"
    const val JWT_TTL_MINUTES = 5
  }
}
