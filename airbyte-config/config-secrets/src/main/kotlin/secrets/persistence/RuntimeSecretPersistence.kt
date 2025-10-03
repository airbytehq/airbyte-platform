/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.persistence

import io.airbyte.config.SecretPersistenceConfig
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.metrics.MetricClient
import io.github.oshai.kotlinlogging.KotlinLogging
import secrets.persistence.AzureKeyVaultClient
import secrets.persistence.AzureKeyVaultPersistence
import secrets.persistence.AzureKeyVaultRuntimeConfiguration

private const val AWS_ASSUME_ROLE_ACCESS_KEY_ID = "AWS_ASSUME_ROLE_ACCESS_KEY_ID"
private const val AWS_ASSUME_ROLE_SECRET_ACCESS_KEY = "AWS_ASSUME_ROLE_SECRET_ACCESS_KEY"

private val log = KotlinLogging.logger {}

/**
 * Class representing a RuntimeSecretPersistence to be used for BYO secrets customers.
 */
class RuntimeSecretPersistence(
  private val secretPersistenceConfig: SecretPersistenceConfig,
  private val metricClient: MetricClient,
) : SecretPersistence {
  private val awsAssumeRoleAccessKey: String? = System.getenv(AWS_ASSUME_ROLE_ACCESS_KEY_ID)
  private val awsAssumeRoleSecretKey: String? = System.getenv(AWS_ASSUME_ROLE_SECRET_ACCESS_KEY)

  private fun buildSecretPersistence(secretPersistenceConfig: SecretPersistenceConfig): SecretPersistence =
    when (secretPersistenceConfig.secretPersistenceType) {
      SecretPersistenceConfig.SecretPersistenceType.AWS -> {
        AwsSecretManagerPersistence(
          AwsSecretsManagerClient.fromRuntimeConfig(
            AwsSecretsManagerRuntimeConfiguration.fromSecretPersistenceConfig(secretPersistenceConfig),
            awsAssumeRoleAccessKey,
            awsAssumeRoleSecretKey,
          ),
        )
      }

      SecretPersistenceConfig.SecretPersistenceType.AZURE -> {
        AzureKeyVaultPersistence(
          AzureKeyVaultClient.fromRuntimeConfig(
            AzureKeyVaultRuntimeConfiguration.fromSecretPersistenceConfig(secretPersistenceConfig),
          ),
        )
      }

      SecretPersistenceConfig.SecretPersistenceType.GOOGLE -> {
        GoogleSecretManagerPersistence(
          GoogleSecretManagerClient.fromRuntimeConfig(
            GoogleSecretsManagerRuntimeConfig.fromSecretPersistenceConfig(secretPersistenceConfig),
          ),
          metricClient,
        )
      }

      SecretPersistenceConfig.SecretPersistenceType.VAULT -> {
        VaultSecretPersistence(
          VaultClient.fromRuntimeConfig(
            VaultSecretsManagerRuntimeConfiguration.fromSecretPersistenceConfig(secretPersistenceConfig),
          ),
        )
      }

      SecretPersistenceConfig.SecretPersistenceType.TESTING -> {
        throw IllegalStateException("Testing secret persistence is not supported")
      }

      else -> throw IllegalStateException(
        "Unexpected value: " + secretPersistenceConfig.secretPersistenceType,
      )
    }

  override fun read(coordinate: SecretCoordinate): String {
    val secretPersistence = buildSecretPersistence(secretPersistenceConfig)
    return secretPersistence.read(coordinate)
  }

  override fun write(
    coordinate: AirbyteManagedSecretCoordinate,
    payload: String,
  ) {
    log.debug { "Writing secret to secret persistence: $coordinate" }
    log.debug { "Config: $secretPersistenceConfig" }
    val secretPersistence: SecretPersistence = buildSecretPersistence(secretPersistenceConfig)
    secretPersistence.write(coordinate, payload)
  }

  override fun delete(coordinate: AirbyteManagedSecretCoordinate) {
    return
  }
}
