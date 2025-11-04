/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.data.repositories.OrganizationDomainVerificationRepository
import io.airbyte.data.services.impls.data.mappers.toDomainModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.airbyte.data.services.impls.data.mappers.toEntityEnum
import io.airbyte.domain.models.DomainVerificationMethod
import io.airbyte.domain.models.DomainVerificationStatus
import io.airbyte.domain.models.OrganizationDomainVerification
import jakarta.inject.Singleton
import java.time.OffsetDateTime
import java.util.UUID

@Singleton
open class OrganizationDomainVerificationService(
  private val repository: OrganizationDomainVerificationRepository,
) {
  /**
   * Creates a new domain verification record.
   */
  fun createDomainVerification(
    organizationId: UUID,
    domain: String,
    createdBy: UUID?,
  ): OrganizationDomainVerification {
    val verificationToken = UUID.randomUUID().toString()
    val dnsRecordPrefix = "_airbyte-verification"
    val dnsRecordName = "$dnsRecordPrefix.$domain"
    val expiresAt = OffsetDateTime.now().plusDays(14)

    val domainVerification =
      OrganizationDomainVerification(
        id = null,
        organizationId = organizationId,
        domain = domain,
        verificationMethod = DomainVerificationMethod.DNS_TXT,
        status = DomainVerificationStatus.PENDING,
        verificationToken = verificationToken,
        dnsRecordName = dnsRecordName,
        dnsRecordPrefix = dnsRecordPrefix,
        expiresAt = expiresAt,
        attempts = 0,
        lastCheckedAt = null,
        createdBy = createdBy,
        verifiedAt = null,
        createdAt = null,
        updatedAt = null,
      )

    // Check if domain already exist
    val existing =
      findByOrganizationIdAndDomain(
        domainVerification.organizationId,
        domainVerification.domain,
      )
    if (existing != null) {
      when (existing.status) {
        DomainVerificationStatus.VERIFIED -> {
          throw IllegalArgumentException(
            "Domain '${domainVerification.domain}' is already verified for this organization.",
          )
        }

        DomainVerificationStatus.PENDING -> {
          throw IllegalArgumentException(
            "Domain '${domainVerification.domain}' has a pending verification. " +
              "Check your DNS records: Add TXT record '${existing.dnsRecordName}' " +
              "with value 'airbyte-domain-verification=${existing.verificationToken}'",
          )
        }

        DomainVerificationStatus.FAILED -> {
          throw IllegalArgumentException(
            "Domain '${domainVerification.domain}' verification failed. " +
              "Please double-check the TXT record you added. " +
              "Expected: TXT record '${existing.dnsRecordName}' with value 'airbyte-domain-verification=${existing.verificationToken}'. " +
              "Once fixed, the verification will automatically retry.",
          )
        }

        DomainVerificationStatus.EXPIRED -> {
          throw IllegalArgumentException(
            "Domain '${domainVerification.domain}' verification expired. " +
              "Please delete or reset the expired verification before creating a new one.",
          )
        }
      }
    }

    val entity = domainVerification.toEntity()
    val savedEntity = repository.save(entity)
    return savedEntity.toDomainModel()
  }

  /**
   * Finds all domain verifications for an organization.
   */
  fun findByOrganizationId(organizationId: UUID): List<OrganizationDomainVerification> =
    repository.findByOrganizationId(organizationId).map { it.toDomainModel() }

  /**
   * Gets a specific domain verification by ID.
   */
  fun getDomainVerification(id: UUID): OrganizationDomainVerification {
    val entity =
      repository.findById(id).orElse(null)
        ?: throw IllegalArgumentException("Domain verification not found with id: $id")
    return entity.toDomainModel()
  }

  /**
   * Finds a specific domain verification by organization and domain.
   */
  fun findByOrganizationIdAndDomain(
    organizationId: UUID,
    domain: String,
  ): OrganizationDomainVerification? = repository.findByOrganizationIdAndDomain(organizationId, domain)?.toDomainModel()

  /**
   * Finds verifications by status (for background verification job).
   */
  fun findByStatus(status: DomainVerificationStatus): List<OrganizationDomainVerification> =
    repository.findByStatus(status.toEntityEnum()).map { it.toDomainModel() }

  /**
   * Updates domain verification status (called by cron job)
   */
  fun updateVerificationStatus(
    id: UUID,
    status: DomainVerificationStatus,
    verifiedAt: OffsetDateTime? = null,
  ): OrganizationDomainVerification {
    val entity =
      repository.findById(id).orElse(null)
        ?: throw IllegalArgumentException("Domain verification not found with id: $id")

    val domainModel = entity.toDomainModel()

    // Update the entity fields
    entity.status = status.toEntityEnum()
    entity.attempts = domainModel.attempts + 1
    entity.lastCheckedAt = OffsetDateTime.now()

    if (status == DomainVerificationStatus.VERIFIED) {
      entity.verifiedAt = verifiedAt ?: OffsetDateTime.now()
    }

    // Check for expiration
    if (domainModel.expiresAt?.isBefore(OffsetDateTime.now()) == true &&
      status == DomainVerificationStatus.PENDING
    ) {
      entity.status = DomainVerificationStatus.EXPIRED.toEntityEnum()
    }

    val savedEntity = repository.save(entity)
    return savedEntity.toDomainModel()
  }
}
