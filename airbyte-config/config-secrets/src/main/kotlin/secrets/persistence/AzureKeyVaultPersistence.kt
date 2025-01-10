/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package secrets.persistence

import com.azure.identity.ClientSecretCredentialBuilder
import com.azure.security.keyvault.secrets.SecretClient
import com.azure.security.keyvault.secrets.SecretClientBuilder
import com.azure.security.keyvault.secrets.models.KeyVaultSecret
import com.azure.security.keyvault.secrets.models.SecretProperties
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration

/**
 * SecretPersistence implementation for Azure Key Vault
 */
@Singleton
@Requires(property = "airbyte.secret.persistence", pattern = "(?i)^azure_key_vault$")
@Named("secretPersistence")
class AzureKeyVaultPersistence(private val secretClient: AzureKeyVaultClient) : SecretPersistence {
  override fun read(coordinate: SecretCoordinate): String {
    val key = coordinate.fullCoordinate.replace("_", "-")
    return secretClient.client.getSecret(
      key,
    ).value
  }

  override fun write(
    coordinate: SecretCoordinate,
    payload: String,
  ) {
    val key = coordinate.fullCoordinate.replace("_", "-")
    val secret =
      KeyVaultSecret(
        key,
        payload,
      )

    if (secretClient.tags.isNotEmpty()) {
      secret.setProperties(SecretProperties().setTags(secretClient.tags))
    }

    secretClient.client.setSecret(secret)
  }

  override fun delete(coordinate: SecretCoordinate) {
    val key = coordinate.fullCoordinate.replace("_", "-")
    secretClient.client
      .beginDeleteSecret(key)
      .waitForCompletion(Duration.ofSeconds(5))
    secretClient.client
      .purgeDeletedSecret(key)
  }
}

@Singleton
class AzureKeyVaultClient(
  @Value("\${airbyte.secret.store.azure.vault-url}") private val vaultUrl: String,
  @Value("\${airbyte.secret.store.azure.tenant-id}") private val tenantId: String,
  @Value("\${airbyte.secret.store.azure.client-id}") private val clientId: String,
  @Value("\${airbyte.secret.store.azure.client-secret}") private val clientSecret: String,
  @Value("\${airbyte.secret.store.azure.tags}") val unparsedTags: String?,
) {
  val tags: Map<String, String> = parseTags(unparsedTags)

  private fun parseTags(tags: String?): Map<String, String> {
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

  val client: SecretClient by lazy {
    SecretClientBuilder()
      .vaultUrl(vaultUrl)
      .credential(
        ClientSecretCredentialBuilder()
          .clientSecret(clientSecret)
          .clientId(clientId)
          .tenantId(tenantId)
          .build(),
      )
      .buildClient()
  }
}
