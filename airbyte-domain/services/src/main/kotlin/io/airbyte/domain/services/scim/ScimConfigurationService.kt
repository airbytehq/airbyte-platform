/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.scim

import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.ScimConfigurationRepository
import io.airbyte.data.repositories.entities.ScimConfiguration
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.scim.ScimAccessDeniedException
import io.airbyte.domain.models.scim.ScimConfigurationConflictException
import io.airbyte.domain.models.scim.ScimConfigurationRead
import io.airbyte.domain.models.scim.ScimConfigurationStatus
import io.airbyte.domain.models.scim.ScimIdpProvider
import io.airbyte.domain.models.scim.ScimOrganizationNotFoundException
import io.micronaut.transaction.TransactionOperations
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.sql.Connection
import java.time.OffsetDateTime
import java.time.ZoneOffset

/**
 * Coordinates SCIM configuration lifecycle transitions.
 *
 * Transactions that need both rows must acquire the organization row lock before the SCIM
 * configuration row lock. Token-authenticated flows may read the configuration to identify the
 * organization, but must then follow this lock order and revalidate the token and configuration
 * state after acquiring both locks.
 */
@Singleton
open class ScimConfigurationService(
  private val scimAccessGate: ScimAccessGate,
  private val organizationRepository: OrganizationRepository,
  private val scimConfigurationRepository: ScimConfigurationRepository,
  private val tokenService: ScimTokenService,
  @param:Named("config") private val configTransactionOperations: TransactionOperations<Connection>,
) {
  open fun getConfiguration(organizationId: OrganizationId): ScimConfigurationRead =
    scimConfigurationRepository.findByOrganizationId(organizationId.value)?.toDomainRead()
      ?: ScimConfigurationRead(status = ScimConfigurationStatus.NOT_CONFIGURED)

  open fun enable(
    organizationId: OrganizationId,
    idpProvider: ScimIdpProvider,
    userId: UserId,
  ): ScimConfigurationRead {
    ensureAccessAllowed(organizationId)

    return configTransactionOperations.executeWrite { _ ->
      lockOrganization(organizationId)
      val existing = scimConfigurationRepository.findByOrganizationIdForUpdate(organizationId.value)
      if (existing != null) {
        if (!existing.enabled) {
          throw ScimConfigurationConflictException("Disabled SCIM configurations cannot be re-enabled by this operation")
        }
        if (existing.idpProvider != idpProvider.storageValue) {
          throw ScimConfigurationConflictException("SCIM is already enabled with a different identity provider")
        }
        return@executeWrite existing.toDomainRead()
      }

      val rawToken = tokenService.generateToken()
      val now = OffsetDateTime.now(ZoneOffset.UTC)
      val saved =
        scimConfigurationRepository.save(
          ScimConfiguration(
            organizationId = organizationId.value,
            tokenHash = tokenService.hashToken(rawToken),
            idpProvider = idpProvider.storageValue,
            enabled = true,
            createdByUserId = userId.value,
            tokenIssuedAt = now,
            tokenIssuedByUserId = userId.value,
          ),
        )

      saved.toDomainRead(token = rawToken)
    }
  }

  open fun rotateToken(
    organizationId: OrganizationId,
    userId: UserId,
  ): ScimConfigurationRead {
    ensureAccessAllowed(organizationId)

    return configTransactionOperations.executeWrite { _ ->
      lockOrganization(organizationId)
      val configuration =
        scimConfigurationRepository
          .findByOrganizationIdForUpdate(organizationId.value)
          ?.takeIf { it.enabled }
          ?: throw ScimConfigurationConflictException("SCIM must be enabled before its token can be rotated")

      val rawToken = tokenService.generateToken()
      val tokenHash = tokenService.hashToken(rawToken)
      val now = OffsetDateTime.now(ZoneOffset.UTC)
      val updatedRows =
        scimConfigurationRepository.rotateTokenByIdAndOrganizationId(
          id = checkNotNull(configuration.id) { "Persisted SCIM configuration must have an id" },
          organizationId = organizationId.value,
          tokenHash = tokenHash,
          tokenIssuedAt = now,
          tokenIssuedByUserId = userId.value,
          updatedAt = now,
        )
      check(updatedRows == 1L) {
        "Expected to rotate one SCIM configuration for organization ${organizationId.value}, updated $updatedRows"
      }

      configuration.tokenHash = tokenHash
      configuration.tokenIssuedAt = now
      configuration.tokenIssuedByUserId = userId.value
      configuration.updatedAt = now
      configuration.toDomainRead(token = rawToken)
    }
  }

  open fun disable(
    organizationId: OrganizationId,
    userId: UserId,
  ) {
    configTransactionOperations.executeWrite { _ ->
      lockOrganization(organizationId)
      val configuration =
        scimConfigurationRepository.findByOrganizationIdForUpdate(organizationId.value)
          ?: return@executeWrite

      if (!configuration.enabled) {
        return@executeWrite
      }

      val now = OffsetDateTime.now(ZoneOffset.UTC)
      val updatedRows =
        scimConfigurationRepository.disableByIdAndOrganizationId(
          id = checkNotNull(configuration.id) { "Persisted SCIM configuration must have an id" },
          organizationId = organizationId.value,
          disabledAt = now,
          disabledByUserId = userId.value,
          updatedAt = now,
        )
      check(updatedRows == 1L) {
        "Expected to disable one SCIM configuration for organization ${organizationId.value}, updated $updatedRows"
      }

      configuration.enabled = false
      configuration.tokenHash = null
      configuration.tokenIssuedAt = null
      configuration.tokenIssuedByUserId = null
      configuration.disabledAt = now
      configuration.disabledByUserId = userId.value
      configuration.updatedAt = now
    }
  }

  private fun ensureAccessAllowed(organizationId: OrganizationId) {
    if (!scimAccessGate.isAllowed(organizationId)) {
      throw ScimAccessDeniedException("SCIM is not available for organization ${organizationId.value}")
    }
  }

  private fun lockOrganization(organizationId: OrganizationId) {
    if (organizationRepository.findByIdForUpdate(organizationId.value).isEmpty) {
      throw ScimOrganizationNotFoundException(organizationId.value)
    }
  }

  private fun ScimConfiguration.toDomainRead(token: String? = null): ScimConfigurationRead =
    ScimConfigurationRead(
      status = if (enabled) ScimConfigurationStatus.ENABLED else ScimConfigurationStatus.DISABLED,
      idpProvider = idpProvider?.let(ScimIdpProvider::fromStorageValue),
      createdAt = createdAt,
      updatedAt = updatedAt,
      token = token,
    )
}
