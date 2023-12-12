/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config.secrets.persistence

import io.airbyte.commons.json.Jsons
import io.airbyte.config.AwsAccessKeySecretPersistenceConfig
import io.airbyte.config.AwsRoleSecretPersistenceConfig
import io.airbyte.config.SecretPersistenceConfig
import io.airbyte.config.secrets.SecretCoordinate
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Property
import kotlin.jvm.optionals.getOrElse

private const val AWS_ASSUME_ROLE_ACCESS_KEY_ID = "AWS_ASSUME_ROLE_ACCESS_KEY_ID"
private const val AWS_ASSUME_ROLE_SECRET_ACCESS_KEY = "AWS_ASSUME_ROLE_SECRET_ACCESS_KEY"

/**
 * Class representing a RuntimeSecretPersistence to be used for BYO secrets customers.
 */
class RuntimeSecretPersistence(private val secretPersistenceConfig: SecretPersistenceConfig) : SecretPersistence {
  private val log = KotlinLogging.logger {}

  @Property(name = "airbyte.secret.store.aws.access-key")
  private val awsAccessKey: String? = System.getenv(AWS_ASSUME_ROLE_ACCESS_KEY_ID)

  @Property(name = "airbyte.secret.store.aws.secret-key")
  private val awsSecretKey: String? = System.getenv(AWS_ASSUME_ROLE_SECRET_ACCESS_KEY)

  private fun buildSecretPersistence(secretPersistenceConfig: SecretPersistenceConfig): SecretPersistence {
    return when (secretPersistenceConfig.secretPersistenceType) {
      SecretPersistenceConfig.SecretPersistenceType.TESTING -> {
        throw IllegalStateException("Testing secret persistence is not supported")
      }

      SecretPersistenceConfig.SecretPersistenceType.GOOGLE -> {
        GoogleSecretManagerPersistence(
          secretPersistenceConfig.configuration["gcpProjectId"]!!,
          GoogleSecretManagerServiceClient(
            secretPersistenceConfig.configuration["gcpCredentialsJson"]!!,
          ),
        )
      }

      SecretPersistenceConfig.SecretPersistenceType.VAULT -> {
        VaultSecretPersistence(
          VaultClient(
            secretPersistenceConfig.configuration["address"]!!,
            secretPersistenceConfig.configuration["token"]!!,
          ),
          secretPersistenceConfig.configuration["prefix"]!!,
        )
      }

      SecretPersistenceConfig.SecretPersistenceType.AWS -> {
        buildAwsSecretManager(secretPersistenceConfig.configuration)
      }

      else -> throw IllegalStateException(
        "Unexpected value: " + secretPersistenceConfig.secretPersistenceType,
      )
    }
  }

  override fun read(coordinate: SecretCoordinate): String {
    val secretPersistence = buildSecretPersistence(secretPersistenceConfig)
    return secretPersistence.read(coordinate)
  }

  override fun write(
    coordinate: SecretCoordinate,
    payload: String,
  ) {
    log.debug { "Writing secret to secret persistence: $coordinate" }
    log.debug { "Config: $secretPersistenceConfig" }
    val secretPersistence: SecretPersistence = buildSecretPersistence(secretPersistenceConfig)
    secretPersistence.write(coordinate, payload)
  }

  private fun buildAwsSecretManager(configuration: Map<String, String>): AwsSecretManagerPersistence {
    // We default to ACCESS_KEY auth
    val authType = configuration["auth_type"]?.uppercase() ?: AwsAuthType.ACCESS_KEY.value
    return when (AwsAuthType.valueOf(authType)) {
      AwsAuthType.ACCESS_KEY -> buildAwsAccessKeySecretManager(configuration)
      AwsAuthType.IAM_ROLE -> buildAwsRoleSecretManager(configuration)
    }
  }

  private fun buildAwsAccessKeySecretManager(configuration: Map<String, String>): AwsSecretManagerPersistence {
    val serializedConfig =
      Jsons.tryObject(Jsons.jsonNode(configuration), AwsAccessKeySecretPersistenceConfig::class.java).getOrElse {
        throw IllegalStateException("Invalid configuration for AWS Access Key secret manager")
      }
    val client =
      AwsClient(
        serializedConfig.awsAccessKey,
        serializedConfig.awsSecretAccessKey,
        serializedConfig.awsRegion,
      )
    val cache = AwsCache(client)
    return AwsSecretManagerPersistence(client, cache)
  }

  private fun buildAwsRoleSecretManager(configuration: Map<String, String>): AwsSecretManagerPersistence {
    val serializedConfig =
      Jsons.tryObject(Jsons.jsonNode(configuration), AwsRoleSecretPersistenceConfig::class.java).getOrElse {
        throw IllegalStateException("Invalid configuration for AWS Role secret manager")
      }
    val client = AwsClient(serializedConfig, awsAccessKey!!, awsSecretKey!!)
    val cache = AwsCache(client)
    return AwsSecretManagerPersistence(client, cache)
  }
}

enum class AwsAuthType(val value: String) {
  ACCESS_KEY("ACCESS_KEY"),
  IAM_ROLE("IAM_ROLE"),
  ;

  fun fromString(value: String): AwsAuthType {
    return when (value) {
      ACCESS_KEY.value -> ACCESS_KEY
      IAM_ROLE.value -> IAM_ROLE
      else -> throw IllegalArgumentException("Invalid auth type: $value")
    }
  }
}
