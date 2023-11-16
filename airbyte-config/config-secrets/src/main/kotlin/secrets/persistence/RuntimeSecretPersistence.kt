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
import kotlin.jvm.optionals.getOrElse

/**
 * Class representing a RuntimeSecretPersistence to be used for BYO secrets customers.
 */
class RuntimeSecretPersistence(private val secretPersistenceConfig: SecretPersistenceConfig) : SecretPersistence {

  private val log = KotlinLogging.logger {}

  private fun buildSecretPersistence(secretPersistenceConfig: SecretPersistenceConfig): SecretPersistence? {
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
    return secretPersistence!!.read(coordinate)
  }

  override fun write(
    coordinate: SecretCoordinate,
    payload: String,
  ) {
    log.debug { "Writing secret to secret persistence: $coordinate" }
    log.debug { "Config: $secretPersistenceConfig" }
    val secretPersistence = buildSecretPersistence(secretPersistenceConfig)
    secretPersistence!!.write(coordinate, payload)
  }

  private fun buildAwsSecretManager(configuration: Map<String, String>): AwsSecretManagerPersistence {
    // We default to ACCESS_KEY auth
    val authType = configuration["auth_type"]?.uppercase() ?: AwsAuthType.ACCESS_KEY.value
    return when (AwsAuthType.valueOf(authType)) {
      AwsAuthType.ACCESS_KEY -> buildAwsAccessKeySecretManager(configuration)
      AwsAuthType.IAM_ROLE -> buildAwsRoleSecretManager(configuration)
    }
  }

  // TODO - codify configurations in objects
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

  fun buildAwsRoleSecretManager(configuration: Map<String, String>): AwsSecretManagerPersistence {
    val serializedConfig =
      Jsons.tryObject(Jsons.jsonNode(configuration), AwsRoleSecretPersistenceConfig::class.java).getOrElse {
        throw IllegalStateException("Invalid configuration for AWS Role secret manager")
      }
    TODO()
  }
}

enum class AwsAuthType {
  ACCESS_KEY("ACCESS_KEY"),
  IAM_ROLE("IAM_ROLE"),
  ;

  val value: String
  constructor(value: String) {
    this.value = value
  }

  fun fromString(value: String): AwsAuthType {
    return when (value) {
      ACCESS_KEY.value -> ACCESS_KEY
      IAM_ROLE.value -> IAM_ROLE
      else -> throw IllegalArgumentException("Invalid auth type: $value")
    }
  }
}
