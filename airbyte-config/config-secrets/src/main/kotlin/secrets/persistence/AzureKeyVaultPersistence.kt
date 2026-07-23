/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package secrets.persistence

import com.azure.identity.ClientSecretCredentialBuilder
import com.azure.security.keyvault.secrets.SecretClient
import com.azure.security.keyvault.secrets.SecretClientBuilder
import com.azure.security.keyvault.secrets.models.KeyVaultSecret
import com.azure.security.keyvault.secrets.models.SecretProperties
import io.airbyte.config.SecretPersistenceConfig
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.micronaut.runtime.AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.AzureKeyVaultSecretsManagerConfig
import io.airbyte.micronaut.runtime.SECRET_MANAGER_AZURE_KEY_VAULT
import io.airbyte.micronaut.runtime.SECRET_PERSISTENCE
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration

/**
 * SecretPersistence implementation for Azure Key Vault
 */
@Singleton
@Requires(property = SECRET_PERSISTENCE, pattern = "(?i)^$SECRET_MANAGER_AZURE_KEY_VAULT$")
@Named("secretPersistence")
class AzureKeyVaultPersistence(
  private val secretClient: AzureKeyVaultClient,
) : SecretPersistence {
  override fun read(coordinate: SecretCoordinate): String = secretClient.getSecret(coordinate)

  override fun write(
    coordinate: AirbyteManagedSecretCoordinate,
    payload: String,
  ) = secretClient.createSecret(coordinate, payload)

  override fun delete(coordinate: AirbyteManagedSecretCoordinate) = secretClient.deleteSecret(coordinate)
}

class AzureKeyVaultRuntimeConfiguration(
  val vaultUrl: String,
  val tenantId: String,
  val clientId: String,
  val clientSecret: String,
  val tags: String?,
) {
  companion object {
    fun fromSecretPersistenceConfig(config: SecretPersistenceConfig): AzureKeyVaultRuntimeConfiguration =
      AzureKeyVaultRuntimeConfiguration(
        vaultUrl = config.configuration["vaultUrl"]!!,
        tenantId = config.configuration["tenantId"]!!,
        clientId = config.configuration["clientId"]!!,
        clientSecret = config.configuration["clientSecret"]!!,
        tags = config.configuration["tags"],
      )
  }
}

interface AzureKeyVaultClient {
  fun getClient(): SecretClient

  fun getTags(): String?

  fun parseTags(tags: String?): Map<String, String> {
    // Define the regex pattern for the whole string validation
    val pattern = "^[\\w\\s._:/=+-@]+=[\\w\\s._:/=+-@]+(,\\s*[\\w\\s._:/=+-@]+=[\\w\\s._:/=+-@]+)*$".toRegex()

    // Check if unparsedTags is not null, not blank, and matches the pattern
    return if (!tags.isNullOrBlank() && pattern.matches(tags)) {
      tags.split(",").associate { part ->
        val (key, value) = part.trim().split("=")
        key to value
      }
    } else if (tags.isNullOrBlank()) {
      emptyMap() // Return an empty map if unparsedTags is null or blank
    } else {
      // If the string doesn't match the pattern, throw an error
      throw IllegalArgumentException(
        "AB_SECRET_MANAGER_SECRET_TAGS does not match the expected format \"key1=value2,key2=value2\": $tags." +
          " Please update the AB_SECRET_MANAGER_SECRET_TAGS env var configurations.",
      )
    }
  }

  fun getSecret(coordinate: SecretCoordinate): String {
    val key = coordinate.fullCoordinate.replace("_", "-")
    return getClient()
      .getSecret(
        key,
      ).value
  }

  fun createSecret(
    coordinate: SecretCoordinate,
    payload: String,
  ) {
    val key = coordinate.fullCoordinate.replace("_", "-")
    val secret =
      KeyVaultSecret(
        key,
        payload,
      ).apply {
        val rawTags = getTags()
        if (rawTags != null) {
          properties = SecretProperties().setTags(parseTags(rawTags))
        }
      }

    getClient().setSecret(secret)
  }

  fun deleteSecret(coordinate: SecretCoordinate) {
    val key = coordinate.fullCoordinate.replace("_", "-")
    getClient()
      .beginDeleteSecret(key)
      .waitForCompletion(Duration.ofSeconds(5))
    getClient()
      .purgeDeletedSecret(key)
  }

  companion object {
    fun fromRuntimeConfig(config: AzureKeyVaultRuntimeConfiguration): AzureKeyVaultClient =
      RuntimeAzureKeyVaultClient(
        delegateClient =
          SecretClientBuilder()
            .vaultUrl(config.vaultUrl)
            .credential(
              ClientSecretCredentialBuilder()
                .clientSecret(config.clientSecret)
                .clientId(config.clientId)
                .tenantId(config.tenantId)
                .build(),
            ).buildClient(),
        tags = config.tags,
      )
  }
}

class RuntimeAzureKeyVaultClient(
  private val delegateClient: SecretClient,
  private val tags: String?,
) : AzureKeyVaultClient {
  override fun getClient(): SecretClient = delegateClient

  override fun getTags(): String? = tags
}

@Singleton
@Requires(property = SECRET_PERSISTENCE, pattern = "(?i)^$SECRET_MANAGER_AZURE_KEY_VAULT$")
class SystemAzureKeyVaultClient(
  private val systemConfig: AzureKeyVaultSecretsManagerConfig,
) : AzureKeyVaultClient {
  val delegateClient: SecretClient by lazy {
    SecretClientBuilder()
      .vaultUrl(systemConfig.vaultUrl)
      .credential(
        ClientSecretCredentialBuilder()
          .clientSecret(systemConfig.clientSecret)
          .clientId(systemConfig.clientId)
          .tenantId(systemConfig.tenantId)
          .build(),
      ).buildClient()
  }

  override fun getClient(): SecretClient = delegateClient

  override fun getTags(): String = systemConfig.tags
}
