/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.commons.auth.AuthRole
import io.airbyte.commons.auth.OrganizationAuthRole
import io.airbyte.commons.auth.WorkspaceAuthRole
import io.airbyte.commons.auth.config.TokenExpirationConfig
import io.airbyte.data.helpers.DataplanePasswordEncoder
import io.airbyte.data.repositories.DataplaneClientCredentialsRepository
import io.airbyte.data.repositories.entities.DataplaneClientCredentials
import io.airbyte.data.services.DataplaneAuthService
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator
import jakarta.inject.Singleton
import java.security.MessageDigest
import java.security.SecureRandom
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

private val logger = KotlinLogging.logger {}

@Singleton
@Requires(property = "micronaut.security.enabled", value = "true")
@Requires(property = "micronaut.security.token.jwt.enabled", value = "true")
@Replaces(DataplaneAuthServiceNoAuthImpl::class)
class DataplaneAuthServiceDataImpl(
  private val dataplaneClientCredentialsRepository: DataplaneClientCredentialsRepository,
  private val jwtTokenGenerator: JwtTokenGenerator,
  private val dataplanePasswordEncoder: DataplanePasswordEncoder,
  private val tokenExpirationConfig: TokenExpirationConfig,
  @Property(name = "airbyte.auth.token-issuer") private val tokenIssuer: String,
) : DataplaneAuthService {
  companion object {
    const val SECRET_LENGTH = 2096
  }

  /**
   * Create the credentials for the dataplane.
   * @param dataplaneId The ID of the dataplane associated with the credentials
   * @return The newly created credentials as a domain object
   */
  override fun createCredentials(
    dataplaneId: UUID,
    createdById: UUID,
  ): io.airbyte.config.DataplaneClientCredentials {
    logger.debug { "Creating credentials for dataplane $dataplaneId" }

    val rawSecret = generateClientSecret()
    val dataplaneClient =
      dataplaneClientCredentialsRepository.save(
        DataplaneClientCredentials(
          id = UUID.randomUUID(),
          dataplaneId = dataplaneId,
          clientId = generateClientId(),
          clientSecret = dataplanePasswordEncoder.encode(rawSecret),
          createdBy = createdById,
        ),
      )
    return toDomain(dataplaneClient.apply { clientSecret = rawSecret })
  }

  /**
   * Deletes credentials for a dataplane.
   * @param dataplaneClientCredentialsId The id of the credentials to be deleted
   * @return The credentials that were deleted if the deletion was successful, otherwise empty
   */
  override fun deleteCredentials(dataplaneClientCredentialsId: UUID): io.airbyte.config.DataplaneClientCredentials {
    logger.debug { "Deleting DataplaneClientCredentials $dataplaneClientCredentialsId" }
    val dataplaneClientCredentials = dataplaneClientCredentialsRepository.findById(dataplaneClientCredentialsId)
    dataplaneClientCredentialsRepository.delete(
      dataplaneClientCredentials.orElseThrow {
        IllegalArgumentException(
          "dataplaneClientCredentials was not found with the Id $dataplaneClientCredentialsId",
        )
      },
    )
    return toDomain(dataplaneClientCredentials.get())
  }

  /**
   * Lists credentials for a dataplane.
   * @param dataplaneId The id of the dataplane whose credentials we want
   * @return The credentials for the dataplane
   */
  override fun listCredentialsByDataplaneId(dataplaneId: UUID): List<io.airbyte.config.DataplaneClientCredentials> {
    val dataplaneClientCredentials = dataplaneClientCredentialsRepository.findByDataplaneId(dataplaneId)
    return dataplaneClientCredentials.map { toDomain(it) }
  }

  /**
   * Gets a token
   * @param clientId The client ID for the dataplane
   * @param clientSecret The client secret for the dataplane
   * @return A JWT that authorizes the user to make requests
   */
  override fun getToken(
    clientId: String,
    clientSecret: String,
  ): String {
    val dataplaneClientCredentials =
      dataplaneClientCredentialsRepository.findByClientId(clientId)
        ?: throw IllegalArgumentException("Credentials were not found with the clientId provided. clientId: $clientId")

    if (!dataplanePasswordEncoder.matches(clientSecret, dataplaneClientCredentials.clientSecret)) {
      throw IllegalArgumentException("Invalid clientSecret provided.")
    }
    return jwtTokenGenerator
      .generateToken(
        mapOf(
          "iss" to tokenIssuer,
          "sub" to dataplaneClientCredentials.dataplaneId,
          "roles" to // TODO: grant no roles; use scopes instead to allow specific endpoints
            buildSet {
              addAll(AuthRole.buildAuthRolesSet(AuthRole.ADMIN))
              addAll(WorkspaceAuthRole.buildWorkspaceAuthRolesSet(WorkspaceAuthRole.WORKSPACE_ADMIN))
              addAll(OrganizationAuthRole.buildOrganizationAuthRolesSet(OrganizationAuthRole.ORGANIZATION_ADMIN))
            },
          "exp" to Instant.now().plus(tokenExpirationConfig.dataplaneTokenExpirationInMinutes, ChronoUnit.MINUTES).epochSecond,
        ),
      ).orElseThrow {
        IllegalStateException(
          "Could not generate token",
        )
      }
  }

  override fun getDataplaneId(clientId: String): UUID =
    dataplaneClientCredentialsRepository
      .findByClientId(clientId)
      ?.dataplaneId
      ?: throw IllegalArgumentException("Credentials were not found with the clientId provided. clientId: $clientId")

  /**
   * Generates a client id string and returns it
   */
  private fun generateClientId(): String = UUID.randomUUID().toString()

  /**
   * Generates a client secret and returns it.
   */
  @OptIn(ExperimentalStdlibApi::class)
  private fun generateClientSecret(): String {
    val bytes = ByteArray(SECRET_LENGTH)
    SecureRandom.getInstanceStrong().nextBytes(bytes)

    return MessageDigest
      .getInstance("SHA3-256")
      .digest(bytes)
      .toHexString()
  }

  /**
   * Converts the Entity to a Domain object.
   * @param dataplaneClientCredentials The Entity to convert
   * @return The domain object
   */
  private fun toDomain(dataplaneClientCredentials: DataplaneClientCredentials): io.airbyte.config.DataplaneClientCredentials {
    val dataplaneClientCredentialsDomain =
      io.airbyte.config.DataplaneClientCredentials(
        id = dataplaneClientCredentials.id,
        dataplaneId = dataplaneClientCredentials.dataplaneId,
        clientId = dataplaneClientCredentials.clientId,
        clientSecret = dataplaneClientCredentials.clientSecret,
        createdAt = dataplaneClientCredentials.createdAt,
        createdBy = dataplaneClientCredentials.createdBy,
      )
    return dataplaneClientCredentialsDomain
  }
}
