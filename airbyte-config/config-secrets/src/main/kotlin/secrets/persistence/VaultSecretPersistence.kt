/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.persistence

import com.bettercloud.vault.Vault
import com.bettercloud.vault.VaultConfig
import com.bettercloud.vault.VaultException
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.persistence.SecretPersistence.ImplementationTypes.VAULT
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

/**
 * Vault implementation for the secret persistence.
 */
@Singleton
@Requires(property = "airbyte.secret.persistence", pattern = "(?i)^$VAULT$")
@Named("secretPersistence")
class VaultSecretPersistence(
  val vaultClient: VaultClient,
  @Value("\${airbyte.secret.store.vault.prefix}") val pathPrefix: String,
) : SecretPersistence {
  override fun read(coordinate: SecretCoordinate): String {
    try {
      val response = vaultClient.client.logical().read(pathPrefix + coordinate.fullCoordinate)
      val restResponse = response.restResponse
      val responseCode = restResponse.status
      val isErrorResponse = responseCode / 100 != 2
      if (isErrorResponse) {
        logger.error { "Vault failed on read. Response code: $responseCode" }
        logger.error { String(restResponse.body) }
        return ""
      }
      val data = response.data
      return data[SECRET_KEY] ?: ""
    } catch (e: VaultException) {
      logger.warn(e) { "Unable to read secret from Vault for coordinate ${coordinate.fullCoordinate}." }
      return ""
    }
  }

  override fun write(
    coordinate: AirbyteManagedSecretCoordinate,
    payload: String,
  ) {
    try {
      val newSecret = HashMap<String, Any>()
      newSecret[SECRET_KEY] = payload
      vaultClient.client.logical().write(pathPrefix + coordinate.fullCoordinate, newSecret)
    } catch (e: VaultException) {
      logger.error(e) { "Vault failed on write for coordinate ${coordinate.fullCoordinate}." }
    }
  }

  override fun delete(coordinate: AirbyteManagedSecretCoordinate) {
    return
  }

  companion object {
    private const val SECRET_KEY = "value"
  }
}

@Singleton
@Requires(property = "airbyte.secret.persistence", pattern = "(?i)^vault$")
class VaultClient(
  @Value("\${airbyte.secret.store.vault.address}") address: String,
  @Value("\${airbyte.secret.store.vault.token}") token: String,
) {
  val client: Vault by lazy {
    val config =
      VaultConfig()
        .address(address)
        .token(token)
        .engineVersion(2)
        .build()
    Vault(config)
  }
}
