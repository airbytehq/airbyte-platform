/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.persistence

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider
import com.amazonaws.secretsmanager.caching.SecretCache
import com.amazonaws.services.secretsmanager.AWSSecretsManager
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import com.amazonaws.services.secretsmanager.model.AWSSecretsManagerException
import com.amazonaws.services.secretsmanager.model.CreateSecretRequest
import com.amazonaws.services.secretsmanager.model.CreateSecretResult
import com.amazonaws.services.secretsmanager.model.DeleteSecretRequest
import com.amazonaws.services.secretsmanager.model.DeleteSecretResult
import com.amazonaws.services.secretsmanager.model.InvalidRequestException
import com.amazonaws.services.secretsmanager.model.ResourceNotFoundException
import com.amazonaws.services.secretsmanager.model.Tag
import com.amazonaws.services.secretsmanager.model.UpdateSecretRequest
import com.amazonaws.services.secretsmanager.model.UpdateSecretResult
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import io.airbyte.commons.json.Jsons
import io.airbyte.config.AwsAccessKeySecretPersistenceConfig
import io.airbyte.config.AwsRoleSecretPersistenceConfig
import io.airbyte.config.SecretPersistenceConfig
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.micronaut.runtime.AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.AwsSecretsManagerConfig
import io.airbyte.micronaut.runtime.SECRET_MANAGER_AWS
import io.airbyte.micronaut.runtime.SECRET_PERSISTENCE
import io.airbyte.micronaut.runtime.SECRET_STORE_AWS_PREFIX
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import kotlin.jvm.optionals.getOrElse

private val logger = KotlinLogging.logger {}

enum class AwsAuthType(
  val value: String,
) {
  ACCESS_KEY("ACCESS_KEY"),
  IAM_ROLE("IAM_ROLE"),
  ;

  fun fromString(value: String): AwsAuthType =
    when (value) {
      ACCESS_KEY.value -> ACCESS_KEY
      IAM_ROLE.value -> IAM_ROLE
      else -> throw IllegalArgumentException("Invalid auth type: $value")
    }
}

/**
 * SecretPersistence implementation for AWS Secret Manager using [Java
 * SDK](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/secretsmanager/package-summary.html) The current implementation doesn't make use of `SecretCoordinate#getVersion` as this
 * version is non-compatible with how AWS secret manager deals with versions. In AWS versions is an
 * internal idiom that can is accessible, but it's a UUID + a tag [more
 * details.](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/secretsmanager/SecretsManagerClient.html#listSecretVersionIds--)
 */
@Singleton
@Requires(property = SECRET_PERSISTENCE, pattern = "(?i)^$SECRET_MANAGER_AWS$")
@Named("secretPersistence")
class AwsSecretManagerPersistence(
  private val awsClient: AwsSecretsManagerClient,
) : SecretPersistence {
  override fun read(coordinate: SecretCoordinate): String = awsClient.getSecret(coordinate)

  override fun write(
    coordinate: AirbyteManagedSecretCoordinate,
    payload: String,
  ) {
    require(payload.isNotEmpty()) { "Payload shouldn't be empty" }
    val existingSecret =
      try {
        read(coordinate)
      } catch (e: AWSSecretsManagerException) {
        // We use tags to control access to secrets.
        // The AWS SDK doesn't differentiate between role access exceptions and secret not found exceptions to prevent leaking information.
        // Because of this we catch the exception and if it's due to the assumed-role not having access, we just ignore it and proceed.
        // In theory, the secret should not exist, and we will go straight to attempting to create which is safe because:
        // 1. Update and create are distinct actions, and we can't create over an already existing secret, so we should get an error and no-op
        // 2. If the secret does exist, we will get an error and no-op
        if (e.localizedMessage.contains("assumed-role")) {
          logger.info { "AWS exception caught - Secret ${coordinate.fullCoordinate} not found" }
          ""
        } else {
          throw e
        }
      }

    if (existingSecret.isNotEmpty()) {
      logger.debug { "Secret ${coordinate.fullCoordinate} found updating payload." }
      try {
        awsClient.updateSecret(coordinate.fullCoordinate, payload, "Airbyte secret.")
        return // We can return early here because we successfully updated
      } catch (_: ResourceNotFoundException) {
        logger.warn { "Secret ${coordinate.fullCoordinate} not found for update" }
        // Otherwise we want to continue onwards.
      }
    }

    // If we couldn't find a secret to update, we should create one.
    awsClient.createSecret(coordinate.fullCoordinate, payload, "Airbyte secret.")
  }

  /**
   * Utility to clean up after integration tests.
   *
   * @param coordinate SecretCoordinate to delete.
   */
  override fun delete(coordinate: AirbyteManagedSecretCoordinate) {
    // Clean up the old bad secrets we might have left behind
    deleteSecretId(coordinate.coordinateBase)
    // Clean up the actual versioned secrets we left behind
    deleteSecretId(coordinate.fullCoordinate)
  }

  private fun deleteSecretId(secretId: String) {
    try {
      awsClient.deleteSecret(secretId)
    } catch (_: ResourceNotFoundException) {
      logger.warn { "Secret $secretId not found" }
    } catch (_: InvalidRequestException) {
      // Was deleted in the UI.
      logger.warn { "Secret $secretId is already deleted" }
    }
  }
}

sealed class AwsSecretsManagerRuntimeConfiguration {
  abstract val region: String

  data class AccessKeyConfig(
    override val region: String,
    val accessKey: String,
    val secretAccessKey: String,
  ) : AwsSecretsManagerRuntimeConfiguration()

  data class AssumeRoleConfig(
    override val region: String,
    val roleArn: String,
    val kmsKeyArn: String?,
    val externalId: String,
    val tagKey: String?,
  ) : AwsSecretsManagerRuntimeConfiguration()

  companion object {
    fun fromSecretPersistenceConfig(config: SecretPersistenceConfig): AwsSecretsManagerRuntimeConfiguration {
      val authType = config.configuration["auth_type"]?.uppercase() ?: AwsAuthType.ACCESS_KEY.value
      when (AwsAuthType.valueOf(authType)) {
        AwsAuthType.ACCESS_KEY -> {
          val accessKeyConfig =
            Jsons.tryObject(Jsons.jsonNode(config.configuration), AwsAccessKeySecretPersistenceConfig::class.java).getOrElse {
              throw IllegalStateException("Invalid configuration for AWS Access Key secret manager")
            }

          return AccessKeyConfig(
            region = accessKeyConfig.awsRegion,
            accessKey = accessKeyConfig.awsAccessKey,
            secretAccessKey = accessKeyConfig.awsSecretAccessKey,
          )
        }

        AwsAuthType.IAM_ROLE -> {
          val assumeRoleConfig =
            Jsons.tryObject(Jsons.jsonNode(config.configuration), AwsRoleSecretPersistenceConfig::class.java).getOrElse {
              throw IllegalStateException("Invalid configuration for AWS Role secret manager")
            }

          return AssumeRoleConfig(
            region = assumeRoleConfig.awsRegion,
            roleArn = assumeRoleConfig.roleArn,
            kmsKeyArn = assumeRoleConfig.kmsKeyArn,
            externalId = assumeRoleConfig.externalId,
            tagKey = assumeRoleConfig.tagKey,
          )
        }
      }
    }
  }
}

interface AwsSecretsManagerClient {
  fun getClient(): AWSSecretsManager

  fun getCache(): SecretCache = SecretCache(getClient())

  fun getKmsKeyArn(): String?

  fun getSecret(coordinate: SecretCoordinate): String {
    var secretString = ""
    val cache = getCache()

    try {
      logger.debug { "Reading secret ${coordinate.fullCoordinate}" }
      secretString = cache.getSecretString(coordinate.fullCoordinate)
    } catch (_: ResourceNotFoundException) {
      logger.warn { "Secret ${coordinate.fullCoordinate} not found" }

      if (coordinate is AirbyteManagedSecretCoordinate) {
        // Attempt to use up old bad secrets
        // If this is just a read, this should work
        // If this is for an update, we should read the secret, return it, and then create a new correctly versioned one and delete the bad one.
        try {
          secretString = cache.getSecretString(coordinate.coordinateBase)
        } catch (_: ResourceNotFoundException) {
          logger.warn { "Secret ${coordinate.coordinateBase} not found" }
        }
      }
    }

    return secretString
  }

  fun createSecret(
    name: String,
    payload: String,
    description: String,
  ): CreateSecretResult

  fun updateSecret(
    id: String,
    payload: String,
    description: String,
  ): UpdateSecretResult

  fun deleteSecret(secretId: String): DeleteSecretResult =
    getClient().deleteSecret(
      DeleteSecretRequest()
        .withSecretId(secretId)
        .withForceDeleteWithoutRecovery(true),
    )

  fun parseTags(tags: String?): Map<String, String> {
    if (tags.isNullOrBlank()) return emptyMap()

    // Define the regex pattern for the whole string validation
    val pattern = "^[\\w\\s._:/=+-@]+=[\\w\\s._:/=+-@]+(,\\s*[\\w\\s._:/=+-@]+=[\\w\\s._:/=+-@]+)*$".toRegex()
    require(pattern.matches(tags)) {
      "AWS Secrets Manager tags do not match the expected format \"key1=value2,key2=value2\": $tags." +
        " Please update the ${SECRET_STORE_AWS_PREFIX}.tags configurations."
    }

    return tags
      .split(",")
      .associate { part ->
        part.trim().split("=", limit = 2).let { (key, value) -> key to value }
      }
  }

  companion object {
    fun fromRuntimeConfig(
      config: AwsSecretsManagerRuntimeConfiguration,
      assumeRoleAccessKey: String?,
      assumeRoleSecretKey: String?,
    ): AwsSecretsManagerClient {
      when (config) {
        is AwsSecretsManagerRuntimeConfiguration.AccessKeyConfig -> {
          return RuntimeAwsSecretsManagerClient(
            AWSSecretsManagerClientBuilder
              .standard()
              .withRegion(config.region)
              .apply {
                if (config.accessKey.isNotEmpty() && config.secretAccessKey.isNotEmpty()) {
                  withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials(config.accessKey, config.secretAccessKey)))
                }
              }.build(),
            config,
          )
        }
        is AwsSecretsManagerRuntimeConfiguration.AssumeRoleConfig -> {
          val stsClient =
            AWSSecurityTokenServiceClientBuilder
              .standard()
              .withRegion(config.region)
              .apply {
                if (!assumeRoleAccessKey.isNullOrBlank() && !assumeRoleSecretKey.isNullOrBlank()) {
                  withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials(assumeRoleAccessKey, assumeRoleSecretKey)))
                }
              }.build()

          val credentialsProvider =
            STSAssumeRoleSessionCredentialsProvider
              .Builder(config.roleArn, "airbyte")
              .withStsClient(stsClient)
              .withExternalId(config.externalId)
              .build()

          return RuntimeAwsSecretsManagerClient(
            AWSSecretsManagerClientBuilder
              .standard()
              .withCredentials(credentialsProvider)
              .withRegion(config.region)
              .build(),
            config,
          )
        }
      }
    }
  }
}

class RuntimeAwsSecretsManagerClient(
  private val delegateClient: AWSSecretsManager,
  private val config: AwsSecretsManagerRuntimeConfiguration,
) : AwsSecretsManagerClient {
  override fun getClient(): AWSSecretsManager = delegateClient

  override fun getKmsKeyArn(): String? =
    when (config) {
      is AwsSecretsManagerRuntimeConfiguration.AssumeRoleConfig -> config.kmsKeyArn
      else -> null
    }

  override fun createSecret(
    name: String,
    payload: String,
    description: String,
  ): CreateSecretResult {
    val request =
      CreateSecretRequest()
        .withName(name)
        .withSecretString(payload)
        .withDescription(description)
        .apply {
          when (config) {
            is AwsSecretsManagerRuntimeConfiguration.AssumeRoleConfig -> {
              withKmsKeyId(config.kmsKeyArn)
            }
            else -> Unit
          }
        }.apply {
          when (config) {
            is AwsSecretsManagerRuntimeConfiguration.AssumeRoleConfig -> {
              if (!config.tagKey.isNullOrBlank()) {
                withTags(Tag().withKey(config.tagKey).withValue("true"))
              }
            }
            else -> Unit
          }
        }

    return getClient().createSecret(request)
  }

  override fun updateSecret(
    id: String,
    payload: String,
    description: String,
  ): UpdateSecretResult {
    val request =
      UpdateSecretRequest()
        .withSecretId(id)
        .withSecretString(payload)
        .withDescription(description)
        .apply {
          if (config is AwsSecretsManagerRuntimeConfiguration.AssumeRoleConfig) {
            withKmsKeyId(config.kmsKeyArn)
          }
        }

    // NOTE: No tags on update

    return getClient().updateSecret(request)
  }
}

@Singleton
@Requires(property = SECRET_PERSISTENCE, pattern = "(?i)^$SECRET_MANAGER_AWS$")
class SystemAwsSecretsManagerClient(
  internal val config: AwsSecretsManagerConfig,
) : AwsSecretsManagerClient {
  private val delegateClient: AWSSecretsManager by lazy {
    AWSSecretsManagerClientBuilder
      .standard()
      .withRegion(config.region)
      .apply {
        if (config.accessKey.isNotEmpty() && config.secretKey.isNotEmpty()) {
          withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials(config.accessKey, config.secretKey)))
        }
      }.build()
  }

  override fun getClient(): AWSSecretsManager = delegateClient

  override fun getKmsKeyArn(): String = config.kmsKeyArn

  override fun createSecret(
    name: String,
    payload: String,
    description: String,
  ): CreateSecretResult {
    val request =
      CreateSecretRequest()
        .withName(name)
        .withSecretString(payload)
        .withDescription(description)
        .withKmsKeyId(getKmsKeyArn())
        .apply {
          for (tag in parseTags(config.tags)) {
            withTags(Tag().withKey(tag.key).withValue(tag.value))
          }
        }

    return getClient().createSecret(request)
  }

  override fun updateSecret(
    id: String,
    payload: String,
    description: String,
  ): UpdateSecretResult {
    val request =
      UpdateSecretRequest()
        .withSecretId(id)
        .withSecretString(payload)
        .withDescription(description)

    // NOTE: No tags on update

    return getClient().updateSecret(request)
  }
}
