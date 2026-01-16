/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.persistence

import com.bettercloud.vault.Vault
import com.bettercloud.vault.VaultConfig
import com.bettercloud.vault.VaultException
import io.airbyte.config.SecretPersistenceConfig
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.micronaut.runtime.AirbyteSecretsManagerConfig.AirbyteSecretsManagerStoreConfig.VaultSecretsManagerConfig
import io.airbyte.micronaut.runtime.SECRET_MANAGER_VAULT
import io.airbyte.micronaut.runtime.SECRET_PERSISTENCE
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

const val VAULT_SECRET_VALUE_KEY = "value"

/**
 * Vault implementation for the secret persistence.
 */
@Singleton
@Requires(property = SECRET_PERSISTENCE, pattern = "(?i)^$SECRET_MANAGER_VAULT$")
@Named("secretPersistence")
class VaultSecretPersistence(
  val vaultClient: VaultClient,
) : SecretPersistence {
  override fun read(coordinate: SecretCoordinate): String = vaultClient.getSecret(coordinate)

  override fun write(
    coordinate: AirbyteManagedSecretCoordinate,
    payload: String,
  ) = vaultClient.createSecret(coordinate, payload)

  // TODO: Implement
  override fun delete(coordinate: AirbyteManagedSecretCoordinate) {
    return
  }
}

class VaultSecretsManagerRuntimeConfiguration(
  val address: String,
  val token: String,
  val prefix: String,
) {
  companion object {
    fun fromSecretPersistenceConfig(config: SecretPersistenceConfig): VaultSecretsManagerRuntimeConfiguration =
      VaultSecretsManagerRuntimeConfiguration(
        address = config.configuration["address"]!!,
        token = config.configuration["token"]!!,
        prefix = config.configuration["prefix"]!!,
      )
  }
}

interface VaultClient {
  fun getClient(): Vault

  fun getPrefix(): String

  fun getSecret(coordinate: SecretCoordinate): String {
    try {
      val response = getClient().logical().read(getPrefix() + coordinate.fullCoordinate)
      val restResponse = response.restResponse
      val responseCode = restResponse.status
      val isErrorResponse = responseCode / 100 != 2
      if (isErrorResponse) {
        logger.error { "Vault failed on read. Response code: $responseCode" }
        logger.error { String(restResponse.body) }
        return ""
      }
      val data = response.data
      return data[VAULT_SECRET_VALUE_KEY] ?: ""
    } catch (e: VaultException) {
      logger.warn(e) { "Unable to read secret from Vault for coordinate ${coordinate.fullCoordinate}." }
      return ""
    }
  }

  fun createSecret(
    coordinate: SecretCoordinate,
    payload: String,
  ) {
    try {
      val newSecret = HashMap<String, Any>()
      newSecret[VAULT_SECRET_VALUE_KEY] = payload
      getClient().logical().write(getPrefix() + coordinate.fullCoordinate, newSecret)
    } catch (e: VaultException) {
      logger.error(e) { "Vault failed on write for coordinate ${coordinate.fullCoordinate}." }
    }
  }

  companion object {
    fun fromRuntimeConfig(config: VaultSecretsManagerRuntimeConfiguration): VaultClient =
      RuntimeVaultClient(
        delegateClient =
          Vault(
            VaultConfig()
              .address(config.address)
              .token(config.token)
              .build(),
          ),
        vaultPrefix = config.prefix,
      )
  }
}

class RuntimeVaultClient(
  private val delegateClient: Vault,
  private val vaultPrefix: String,
) : VaultClient {
  override fun getClient(): Vault = delegateClient

  override fun getPrefix(): String = vaultPrefix
}

@Singleton
@Requires(property = SECRET_PERSISTENCE, pattern = "(?i)^$SECRET_MANAGER_VAULT$")
class SystemVaultClient(
  private val systemConfig: VaultSecretsManagerConfig,
) : VaultClient {
  private val delegateClient: Vault by lazy {
    val config =
      VaultConfig()
        .address(systemConfig.address)
        .token(systemConfig.token)
        .engineVersion(2)
        .build()
    Vault(config)
  }
  private val vaultPrefix: String = systemConfig.prefix

  override fun getClient(): Vault = delegateClient

  override fun getPrefix(): String = vaultPrefix
}
