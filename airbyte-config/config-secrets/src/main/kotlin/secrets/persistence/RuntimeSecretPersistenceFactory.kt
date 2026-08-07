/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.persistence

import io.airbyte.config.SecretPersistenceConfig
import io.airbyte.metrics.MetricClient
import jakarta.inject.Singleton
import secrets.persistence.AzureKeyVaultClient
import secrets.persistence.AzureKeyVaultPersistence
import secrets.persistence.AzureKeyVaultRuntimeConfiguration

private const val AWS_ASSUME_ROLE_ACCESS_KEY_ID = "AWS_ASSUME_ROLE_ACCESS_KEY_ID"
private const val AWS_ASSUME_ROLE_SECRET_ACCESS_KEY = "AWS_ASSUME_ROLE_SECRET_ACCESS_KEY"

/**
 * Builds the concrete [SecretPersistence] for a BYO-secrets customer from their runtime
 * [SecretPersistenceConfig]. The persistence type and its backing cloud client are resolved once
 * per [create] call, and the returned instance is reused for the lifetime the caller holds it.
 */
@Singleton
class RuntimeSecretPersistenceFactory(
  private val metricClient: MetricClient,
) {
  private val awsAssumeRoleAccessKey: String? = System.getenv(AWS_ASSUME_ROLE_ACCESS_KEY_ID)
  private val awsAssumeRoleSecretKey: String? = System.getenv(AWS_ASSUME_ROLE_SECRET_ACCESS_KEY)

  fun create(secretPersistenceConfig: SecretPersistenceConfig): SecretPersistence =
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
}
