package io.airbyte.bootloader

import io.fabric8.kubernetes.api.model.SecretBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
import jakarta.inject.Singleton
import org.apache.commons.lang3.RandomStringUtils
import java.util.Base64
import java.util.UUID

private val logger = KotlinLogging.logger {}

const val SECRET_LENGTH = 32

@Singleton
@Requires(env = [Environment.KUBERNETES])
@Requires(property = "airbyte.auth.kubernetes-secret.creation-enabled", value = "true")
class AuthKubernetesSecretInitializer(
  @Property(name = "airbyte.auth.kubernetes-secret.name") private val secretName: String,
  private val kubernetesClient: KubernetesClient,
  private val secretKeysConfig: AuthKubernetesSecretKeysConfig,
  private val providedSecretValuesConfig: AuthKubernetesSecretValuesConfig,
) {
  fun initializeSecrets() {
    logger.info { "Initializing auth secret in Kubernetes..." }
    val secretDataMap = getSecretDataMap()
    val secret =
      SecretBuilder()
        .withNewMetadata()
        .withName(secretName)
        .endMetadata()
        .addToData(secretDataMap)
        .build()

    if (kubernetesClient.secrets().withName(secretName).get() == null) {
      logger.info { "No existing secret with name $secretName was found. Creating it..." }
      kubernetesClient.resource(secret).create()
    } else {
      logger.info { "Secret with name $secretName already exists. Updating it..." }
      kubernetesClient.resource(secret).update()
    }
    logger.info { "Finished initializing auth secret." }
  }

  private fun getSecretDataMap(): Map<String, String> {
    val passwordValue =
      getOrCreateSecretEncodedValue(
        secretKeysConfig.instanceAdminPasswordSecretKey,
        providedSecretValuesConfig.instanceAdminPassword,
        RandomStringUtils.randomAlphanumeric(SECRET_LENGTH),
      )
    val clientIdValue =
      getOrCreateSecretEncodedValue(
        secretKeysConfig.instanceAdminClientIdSecretKey,
        providedSecretValuesConfig.instanceAdminClientId,
        UUID.randomUUID().toString(),
      )
    val clientSecretValue =
      getOrCreateSecretEncodedValue(
        secretKeysConfig.instanceAdminClientSecretSecretKey,
        providedSecretValuesConfig.instanceAdminClientSecret,
        RandomStringUtils.randomAlphanumeric(SECRET_LENGTH),
      )
    val jwtSignatureValue =
      getOrCreateSecretEncodedValue(
        secretKeysConfig.jwtSignatureSecretKey,
        providedSecretValuesConfig.jwtSignatureSecret,
        RandomStringUtils.randomAlphanumeric(SECRET_LENGTH),
      )
    return mapOf(
      secretKeysConfig.instanceAdminPasswordSecretKey!! to passwordValue,
      secretKeysConfig.instanceAdminClientIdSecretKey!! to clientIdValue,
      secretKeysConfig.instanceAdminClientSecretSecretKey!! to clientSecretValue,
      secretKeysConfig.jwtSignatureSecretKey!! to jwtSignatureValue,
    )
  }

  private fun getOrCreateSecretEncodedValue(
    secretKey: String?,
    providedValue: String?,
    defaultValue: String,
  ): String {
    if (!providedValue.isNullOrBlank()) {
      // if a value is provided directly, base64 encode it and return it, regardless of what may be
      // present in the secret.
      logger.info { "Using provided value for secret key $secretKey" }
      return providedValue.let { Base64.getEncoder().encodeToString(it.toByteArray()) }
    } else {
      val secret = kubernetesClient.secrets().withName(secretName).get()
      if (secret != null && secretKey != null && secret.data.containsKey(secretKey)) {
        // if a value is present in the secret, just return it because it is already base64 encoded,
        // and will not be overwritten.
        logger.info { "Using existing value for secret key $secretKey" }
        return secret.data[secretKey]!!
      } else {
        // if no value is provided or present in the secret, generate a new value and return it.
        logger.info { "Using generated/default value for secret key $secretKey" }
        return defaultValue.let { Base64.getEncoder().encodeToString(it.toByteArray()) }
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
