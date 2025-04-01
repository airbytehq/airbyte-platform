/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import io.airbyte.commons.random.randomAlphanumeric
import io.fabric8.kubernetes.client.KubernetesClient
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Property
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger {}

const val SECRET_LENGTH = 32

@Singleton
class AuthKubernetesSecretInitializer(
  @Property(name = "airbyte.auth.kubernetes-secret.name") private val secretName: String,
  private val kubernetesClient: KubernetesClient,
  private val secretKeysConfig: AuthKubernetesSecretKeysConfig,
  private val providedSecretValuesConfig: AuthKubernetesSecretValuesConfig,
) {
  fun initializeSecrets() {
    logger.info { "Initializing auth secret in Kubernetes..." }
    K8sSecretHelper.createOrUpdateSecret(kubernetesClient, secretName, getSecretDataMap())
    logger.info { "Finished initializing auth secret." }
  }

  private fun getSecretDataMap(): Map<String, String> {
    val passwordValue =
      getOrCreateSecretValue(
        secretKeysConfig.instanceAdminPasswordSecretKey,
        providedSecretValuesConfig.instanceAdminPassword,
        randomAlphanumeric(SECRET_LENGTH),
      )
    val clientIdValue =
      getOrCreateSecretValue(
        secretKeysConfig.instanceAdminClientIdSecretKey,
        providedSecretValuesConfig.instanceAdminClientId,
        UUID.randomUUID().toString(),
      )
    val clientSecretValue =
      getOrCreateSecretValue(
        secretKeysConfig.instanceAdminClientSecretSecretKey,
        providedSecretValuesConfig.instanceAdminClientSecret,
        randomAlphanumeric(SECRET_LENGTH),
      )
    val jwtSignatureValue =
      getOrCreateSecretValue(
        secretKeysConfig.jwtSignatureSecretKey,
        providedSecretValuesConfig.jwtSignatureSecret,
        randomAlphanumeric(SECRET_LENGTH),
      )
    return mapOf(
      secretKeysConfig.instanceAdminPasswordSecretKey!! to passwordValue,
      secretKeysConfig.instanceAdminClientIdSecretKey!! to clientIdValue,
      secretKeysConfig.instanceAdminClientSecretSecretKey!! to clientSecretValue,
      secretKeysConfig.jwtSignatureSecretKey!! to jwtSignatureValue,
    )
  }

  private fun getOrCreateSecretValue(
    secretKey: String?,
    providedValue: String?,
    defaultValue: String,
  ): String {
    if (!providedValue.isNullOrBlank()) {
      // if a value is provided directly, simply return it, regardless of what may be
      // present in the secret.
      logger.info { "Using provided value for secret key $secretKey" }
      return providedValue
    } else {
      val secret = kubernetesClient.secrets().withName(secretName).get()
      if (secret != null && secretKey != null && secret.data.containsKey(secretKey)) {
        // if a value is present in the secret, simply return it without overwriting it.
        logger.info { "Using existing value for secret key $secretKey" }
        return secret.data[secretKey]!!
      } else {
        // if no value is provided or present in the secret, generate a new value and return it.
        logger.info { "Using generated/default value for secret key $secretKey" }
        return defaultValue
      }
    }
  }
}

@ConfigurationProperties("airbyte.auth.kubernetes-secret.keys")
open class AuthKubernetesSecretKeysConfig {
  var instanceAdminPasswordSecretKey: String? = null
  var instanceAdminClientIdSecretKey: String? = null
  var instanceAdminClientSecretSecretKey: String? = null
  var jwtSignatureSecretKey: String? = null
}

@ConfigurationProperties("airbyte.auth.kubernetes-secret.values")
open class AuthKubernetesSecretValuesConfig {
  var instanceAdminPassword: String? = null
  var instanceAdminClientId: String? = null
  var instanceAdminClientSecret: String? = null
  var jwtSignatureSecret: String? = null
}
