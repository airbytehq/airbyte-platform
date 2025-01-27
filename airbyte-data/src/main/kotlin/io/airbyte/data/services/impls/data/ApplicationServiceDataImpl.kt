/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.commons.auth.AuthRole
import io.airbyte.commons.auth.OrganizationAuthRole
import io.airbyte.commons.auth.WorkspaceAuthRole
import io.airbyte.config.AuthenticatedUser
import io.airbyte.data.repositories.ApplicationRepository
import io.airbyte.data.repositories.entities.Application
import io.airbyte.data.services.ApplicationService
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator
import jakarta.inject.Singleton
import java.security.MessageDigest
import java.security.SecureRandom
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import kotlin.time.Duration.Companion.days
import io.airbyte.config.Application as ApplicationDomain

@Singleton
@Requires(property = "airbyte.application.type", value = "database")
class ApplicationServiceDataImpl(
  private val applicationRepository: ApplicationRepository,
  private val jwtTokenGenerator: JwtTokenGenerator,
) : ApplicationService {
  companion object {
    const val SECRET_LENGTH = 2096
    val TOKEN_EXPIRATION_LENGTH = 1.days.inWholeMinutes
    private val logger = KotlinLogging.logger {}
  }

  /**
   * Create the application with the name provided for the user.
   * @param user The User to create the application for
   * @param name The name provided for the new application
   * @return The newly created application as a domain object
   */
  override fun createApplication(
    user: AuthenticatedUser,
    name: String,
  ): ApplicationDomain {
    logger.debug { "Creating application $name" }

    val application =
      applicationRepository.save(
        Application(
          id = UUID.randomUUID(),
          userId = user.userId,
          name = name,
          clientId = generateClientId(),
          clientSecret = generateClientSecret(),
        ),
      )
    return toDomain(application)
  }

  /**
   * Lists all applications associated with the user
   * @param user The user to filter applications by
   * @return The list of Applications that the User has
   */
  override fun listApplicationsByUser(user: AuthenticatedUser): List<ApplicationDomain> {
    logger.debug { "Listing applications" }
    return applicationRepository
      .findByUserId(userId = user.userId)
      .map { application -> toDomain(application) }
      .toList()
  }

  /**
   * Deletes an application. The userId of the application must match the userId passed in as a param
   * @param user The user to verify the userId of the application against
   * @param applicationId The id of the application to be deleted
   * @return The Application that was deleted if the deletion was successful, otherwise empty
   */
  override fun deleteApplication(
    user: AuthenticatedUser,
    applicationId: String,
  ): ApplicationDomain {
    logger.debug { "Deleting application $applicationId" }
    val application: Application =
      applicationRepository.findByUserIdAndId(
        userId = user.userId,
        applicationId = UUID.fromString(applicationId),
      )
        ?: throw IllegalArgumentException("application was not found with the userId and applicationId provided")
    if (application.userId != user.userId) throw IllegalArgumentException("applicationId must be owned by the user")
    applicationRepository.delete(application)
    return toDomain(application)
  }

  /**
   * Generates an Access Token if the clientId and clientSecret are correct
   * @param clientId The Client Id of the Application that is requesting the Access Token
   * @param clientSecret The Client Secret of the Application that is requesting the Access Token
   * @return An Access Token if the information provided was correct.
   */
  override fun getToken(
    clientId: String,
    clientSecret: String,
  ): String {
    logger.debug { "Generating token for client $clientId" }
    val application =
      applicationRepository.findByClientIdAndClientSecret(clientId, clientSecret)
        ?: throw IllegalArgumentException("application was not found with the clientId and clientSecret provided")
    val roles =
      buildSet {
        addAll(AuthRole.buildAuthRolesSet(AuthRole.ADMIN))
        addAll(WorkspaceAuthRole.buildWorkspaceAuthRolesSet(WorkspaceAuthRole.WORKSPACE_ADMIN))
        addAll(OrganizationAuthRole.buildOrganizationAuthRolesSet(OrganizationAuthRole.ORGANIZATION_ADMIN))
      }
    return jwtTokenGenerator
      .generateToken(
        mapOf(
          "iss" to "airbyte-server",
          "sub" to application.userId,
          "roles" to roles,
          "exp" to Instant.now().plus(TOKEN_EXPIRATION_LENGTH, ChronoUnit.MINUTES).epochSecond,
        ),
      ) // Necessary now that this is no longer optional, but I don't know under what conditions we could
      // end up here.
      .orElseThrow {
        IllegalStateException(
          "Could not generate token",
        )
      }
  }

  /**
   * Converts the Entity to a Domain object.
   * @param application The Entity to convert
   * @return The domain object
   */
  private fun toDomain(application: Application): ApplicationDomain {
    val applicationDomain =
      ApplicationDomain().apply {
        id = application.id.toString()
        name = application.name
        clientId = application.clientId
        clientSecret = application.clientSecret
        createdOn = application.createdAt.toString()
      }
    return applicationDomain
  }

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
}
