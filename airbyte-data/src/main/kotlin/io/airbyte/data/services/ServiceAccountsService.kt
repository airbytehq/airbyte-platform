/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.commons.auth.config.TokenExpirationConfig
import io.airbyte.data.repositories.ServiceAccountsRepository
import io.airbyte.domain.models.ServiceAccount
import io.micronaut.context.annotation.Property
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator
import jakarta.inject.Singleton
import org.springframework.security.crypto.password.Pbkdf2PasswordEncoder
import java.security.MessageDigest
import java.security.SecureRandom
import java.time.Clock
import java.time.temporal.ChronoUnit
import java.util.UUID
import io.airbyte.data.repositories.entities.ServiceAccount as ServiceAccountEntity

@Singleton
class ServiceAccountsService internal constructor(
  private val jwtTokenGenerator: JwtTokenGenerator,
  private val repo: ServiceAccountsRepository,
  @Property(name = "airbyte.auth.token-issuer") private val tokenIssuer: String = "airbyte",
  private val tokenExpirationConfig: TokenExpirationConfig,
) {
  var clock: Clock = Clock.systemUTC()

  companion object {
    // TOKEN_TYPE is the value used to identity a token as a service account.
    const val SERVICE_ACCOUNT_TOKEN_TYPE = "io.airbyte.auth.service_account"
  }

  fun create(
    name: String,
    managed: Boolean = false,
  ): ServiceAccount =
    repo
      .save(
        ServiceAccountEntity(
          name = name,
          secret = SecretUtil.generate(),
          managed = managed,
        ),
      ).toModel()

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
    repo.findOne(id, secret) ?: throw ServiceAccountNotFound(id)

    return jwtTokenGenerator
      .generateToken(
        mapOf(
          "iss" to tokenIssuer,
          "aud" to "airbyte-server",
          "sub" to id.toString(),
          "typ" to SERVICE_ACCOUNT_TOKEN_TYPE,
          "exp" to clock.instant().plus(tokenExpirationConfig.serviceAccountTokenExpirationInMinutes, ChronoUnit.MINUTES).epochSecond,
        ),
      ).orElseThrow {
        ServiceAccountTokenGenerationError()
      }
  }

  fun delete(id: UUID) {
    repo.deleteById(id)
  }
}

class ServiceAccountTokenGenerationError : Exception("Unknown error while generating token")

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
  fun generate(): String {
    val bytes = ByteArray(SECRET_LENGTH)
    random.nextBytes(bytes)

    val digest =
      MessageDigest
        .getInstance("SHA3-256")
        .digest(bytes)
        .toHexString()

    return encoder.encode(digest)
  }
}
