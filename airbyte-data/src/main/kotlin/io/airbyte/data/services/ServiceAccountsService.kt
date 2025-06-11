/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.commons.auth.config.TokenExpirationConfig
import io.airbyte.data.TokenType
import io.airbyte.data.auth.AirbyteJwtGenerator
import io.airbyte.data.repositories.ServiceAccountsRepository
import io.airbyte.domain.models.ServiceAccount
import jakarta.inject.Singleton
import org.springframework.security.crypto.password.Pbkdf2PasswordEncoder
import java.security.MessageDigest
import java.security.SecureRandom
import java.time.Clock
import java.util.UUID
import io.airbyte.data.repositories.entities.ServiceAccount as ServiceAccountEntity

@Singleton
class ServiceAccountsService internal constructor(
  private val jwtTokenGenerator: AirbyteJwtGenerator,
  private val repo: ServiceAccountsRepository,
  private val tokenExpirationConfig: TokenExpirationConfig,
) {
  var clock: Clock = Clock.systemUTC()

  fun create(
    id: UUID = UUID.randomUUID(),
    name: String,
    managed: Boolean = false,
  ): ServiceAccount {
    val (secret, encodedSecret) = SecretUtil.generate()

    repo.save(
      ServiceAccountEntity(
        id = id,
        name = name,
        secret = encodedSecret,
        managed = managed,
      ),
    )

    return ServiceAccount(
      id = id,
      name = name,
      secret = secret,
      managed = managed,
    )
  }

  fun getAndVerify(
    id: UUID,
    secret: String,
  ): ServiceAccount {
    val serviceAccount = get(id) ?: throw ServiceAccountNotFound(id)
    if (!SecretUtil.matches(secret, serviceAccount.secret)) {
      throw ServiceAccountNotFound(id)
    }

    return serviceAccount
  }

  fun get(id: UUID): ServiceAccount? = repo.findById(id).orElse(null)?.toModel()

  /**
   * Get a managed service account by name.
   *
   * Managed service accounts are the ones we create internally (i.e. from the bootloader),
   * so we have hard-coded names, but we might not know the ID.
   */
  fun getManagedByName(name: String): ServiceAccount? = repo.findOne(name, true)?.toModel()

  fun generateToken(
    id: UUID,
    secret: String,
  ): String {
    getAndVerify(id, secret)

    return jwtTokenGenerator
      .generateToken(
        tokenSubject = id.toString(),
        tokenType = TokenType.SERVICE_ACCOUNT,
        tokenExpirationLength = tokenExpirationConfig.serviceAccountTokenExpirationInMinutes,
      )
  }

  fun delete(id: UUID) {
    repo.deleteById(id)
  }
}

class ServiceAccountNotFound(
  id: UUID,
) : Exception("Service account not found: $id")

private fun ServiceAccountEntity.toModel() =
  ServiceAccount(
    id = this.id!!,
    name = this.name,
    secret = this.secret,
    managed = this.managed,
  )

private object SecretUtil {
  private const val SECRET_LENGTH = 2096
  private val encoder = Pbkdf2PasswordEncoder.defaultsForSpringSecurity_v5_8()
  private val random = SecureRandom.getInstanceStrong()

  /**
   * Generates a client secret and returns it.
   */
  @OptIn(ExperimentalStdlibApi::class)
  fun generate(): Pair<String, String> {
    val bytes = ByteArray(SECRET_LENGTH)
    random.nextBytes(bytes)

    val digest =
      MessageDigest
        .getInstance("SHA3-256")
        .digest(bytes)
        .toHexString()

    return Pair(digest, encoder.encode(digest))
  }

  fun matches(
    clientSecret: String,
    encodedSecret: String,
  ): Boolean = encoder.matches(clientSecret, encodedSecret)
}
