/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.auth

import com.nimbusds.jwt.JWTClaimsSet
import com.nimbusds.jwt.PlainJWT
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.security.token.jwt.signature.secret.SecretSignature
import io.micronaut.security.token.jwt.signature.secret.SecretSignatureConfiguration
import okhttp3.Interceptor
import okhttp3.Response
import java.time.Instant
import java.util.Date

private val log = KotlinLogging.logger {}

/**
 * InternalClientTokenInterceptor is used to provide an Authorization header
 * for internal http clients. It provides a JWT bearer token to each request,
 * using a shared secret key to sign the tokens if available, otherwise it
 * generates unsigned tokens.
 */
class InternalClientTokenInterceptor(
  private val subject: String,
  private val signatureSecret: String?,
) : Interceptor {
  override fun intercept(chain: Interceptor.Chain): Response {
    val token = generateToken()

    return chain.proceed(
      chain
        .request()
        .newBuilder()
        .addHeader("Authorization", "Bearer $token")
        .build(),
    )
  }

  // Generate a JWT token. If "signatureSecret" is available,
  // then the token will be signed using that secret, otherwise
  // it's an unsigned (plain) token.
  //
  // The "typ" claim is a special claim that grants this token
  // instance admin privileges (see the RoleResolver and TokenType).
  //
  // TODO this should be sharing code with TokenType and AirbyteJwtGenerator,
  //      but the code and modules need to be reorganized the make that possible.
  //      Also, we need to move away from internal clients having the instance admin role.
  private fun generateToken(): String {
    val claims =
      JWTClaimsSet
        .Builder()
        .subject(subject)
        .claim("typ", "io.airbyte.auth.internal_client")
        .expirationTime(Date.from(Instant.now().plusSeconds(300)))
        .build()

    if (signatureSecret.isNullOrBlank()) {
      log.debug { "Generating plain JWT – no signature secret available." }
      return PlainJWT(claims).serialize()
    }

    log.debug { "Generating signed JWT." }
    val secretSignatureConfig = SecretSignatureConfiguration(subject)
    secretSignatureConfig.secret = signatureSecret
    val secretSignature = SecretSignature(secretSignatureConfig)
    return secretSignature.sign(claims).serialize()
  }
}
