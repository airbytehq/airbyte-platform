/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package secrets.persistence

import io.airbyte.config.SecretPersistenceConfig
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.persistence.AwsCache
import io.airbyte.config.secrets.persistence.AwsClient
import io.airbyte.config.secrets.persistence.AwsSecretManagerPersistence
import io.airbyte.config.secrets.persistence.GoogleSecretManagerPersistence
import io.airbyte.config.secrets.persistence.GoogleSecretManagerServiceClient
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.config.secrets.persistence.VaultClient
import io.airbyte.config.secrets.persistence.VaultSecretPersistence
import jakarta.inject.Singleton

@Singleton
class RuntimeSecretPersistence {
  fun read(
    coordinate: SecretCoordinate,
    secretPersistenceConfig: SecretPersistenceConfig,
  ): String {
    val secretPersistence = buildSecretPersistence(secretPersistenceConfig)
    return secretPersistence!!.read(coordinate)
  }

  fun write(
    coordinate: SecretCoordinate,
    payload: String,
    secretPersistenceConfig: SecretPersistenceConfig,
  ) {
    val secretPersistence = buildSecretPersistence(secretPersistenceConfig)
    secretPersistence!!.write(coordinate, payload)
  }

  private fun buildSecretPersistence(secretPersistenceConfig: SecretPersistenceConfig): SecretPersistence? {
    return when (secretPersistenceConfig.secretPersistenceType) {
      SecretPersistenceConfig.SecretPersistenceType.TESTING -> {
        null
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
        val awsClient =
          AwsClient(
            secretPersistenceConfig.configuration["awsAccessKey"]!!,
            secretPersistenceConfig.configuration["awsSecretAccessKey"]!!,
          )

        AwsSecretManagerPersistence(
          awsClient,
          AwsCache(awsClient),
        )
      }

      else -> throw IllegalStateException(
        "Unexpected value: " + secretPersistenceConfig.secretPersistenceType,
      )
    }
  }
}
