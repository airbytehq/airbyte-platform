/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.helpers.DataplanePasswordEncoder
import io.airbyte.data.repositories.DataplaneClientCredentialsRepository
import io.airbyte.data.repositories.entities.DataplaneClientCredentials
import io.airbyte.data.services.DataplaneCredentialsService
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.UUID

private val logger = KotlinLogging.logger {}

@Singleton
class DataplaneCredentialsServiceDataImpl(
  private val dataplaneClientCredentialsRepository: DataplaneClientCredentialsRepository,
  private val dataplanePasswordEncoder: DataplanePasswordEncoder,
) : DataplaneCredentialsService {
  companion object {
    const val SECRET_LENGTH = 2096
  }

  /**
   * Create the credentials for the dataplane.
   * @param dataplaneId The ID of the dataplane associated with the credentials
   * @return The newly created credentials as a domain object
   */
  override fun createCredentials(dataplaneId: UUID): io.airbyte.config.DataplaneClientCredentials {
    logger.debug { "Creating credentials for dataplane $dataplaneId" }

    val rawSecret = generateClientSecret()
    val dataplaneClient =
      dataplaneClientCredentialsRepository.save(
        DataplaneClientCredentials(
          id = UUID.randomUUID(),
          dataplaneId = dataplaneId,
          clientId = generateClientId(),
          clientSecret = dataplanePasswordEncoder.encode(rawSecret),
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
      )
    return dataplaneClientCredentialsDomain
  }
}
