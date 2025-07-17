/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import com.nimbusds.jwt.JWTClaimsSet
import com.nimbusds.jwt.PlainJWT
import io.airbyte.bootloader.K8sSecretHelper.base64Decode
import io.airbyte.data.auth.TokenType
import io.fabric8.kubernetes.api.model.Secret
import io.fabric8.kubernetes.client.KubernetesClient
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Property
import io.micronaut.security.token.jwt.signature.secret.SecretSignature
import io.micronaut.security.token.jwt.signature.secret.SecretSignatureConfiguration
import jakarta.inject.Singleton

private val log = KotlinLogging.logger {}

@Singleton
class InternalApiTokenInitializer(
  @Property(name = "airbyte.auth.kubernetes-secret.name") private val secretName: String,
  private val kubernetesClient: KubernetesClient,
  private val secretKeysConfig: AuthKubernetesSecretKeysConfig,
  private val secretValuesConfig: AuthKubernetesSecretValuesConfig,
) {
  fun initializeInternalClientToken() {
    val secret = getSecret()

    val tokenKey = secretKeysConfig.internalApiTokenSecretKey
    if (tokenKey.isNullOrBlank()) {
      error("internalApiTokenSecretKey is null or blank")
    }

    // If the token already exists, do nothing.
    if (!secret?.data?.get(tokenKey).isNullOrBlank()) {
      log.info { "Internal client token already exists. " }
      return
    }

    val jwtSignatureKey = getJwtSignatureKey(secret)
    val internalApiToken = generateInternalClientToken(jwtSignatureKey)
    K8sSecretHelper.createOrUpdateSecret(kubernetesClient, secretName, mapOf(tokenKey to internalApiToken))
  }

  private fun getSecret(): Secret? = kubernetesClient.secrets().withName(secretName).get()

  private fun generateInternalClientToken(jwtSignatureValue: String?): String {
    val tokenTypeClaim = TokenType.INTERNAL_CLIENT.toClaim()
    val claims =
      JWTClaimsSet
        .Builder()
        .subject("airbyte-internal-api-client")
        .claim(tokenTypeClaim.first, tokenTypeClaim.second)
        .build()

    if (jwtSignatureValue == null) {
      log.info { "Generating plain JWT – no signature secret available." }
      return PlainJWT(claims).serialize()
    }

    log.info { "Generating signed JWT." }
    val secretSignatureConfig = SecretSignatureConfiguration("airbyte-internal-api-client")
    secretSignatureConfig.secret = jwtSignatureValue
    val secretSignature = SecretSignature(secretSignatureConfig)
    return secretSignature.sign(claims).serialize()
  }

  private fun getJwtSignatureKey(secret: Secret?): String? {
    if (!secretValuesConfig.jwtSignatureSecret.isNullOrBlank()) {
      log.info { "Using JWT signature from config values." }
      return secretValuesConfig.jwtSignatureSecret!!
    }

    if (secret == null) {
      return null
    }

    val jwtSignatureKey = secret.data[secretKeysConfig.jwtSignatureSecretKey]
    if (jwtSignatureKey.isNullOrBlank()) {
      return null
    }

    log.info { "Using JWT signature from secret." }
    return base64Decode(jwtSignatureKey)
  }
}
