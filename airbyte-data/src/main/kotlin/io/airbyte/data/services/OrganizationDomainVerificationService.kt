/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.OrganizationEmailDomain
import io.airbyte.config.SsoConfigStatus
import io.airbyte.data.repositories.OrganizationDomainVerificationRepository
import io.airbyte.data.services.impls.data.mappers.toDomainModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.airbyte.data.services.impls.data.mappers.toEntityEnum
import io.airbyte.domain.models.DomainVerificationMethod
import io.airbyte.domain.models.DomainVerificationStatus
import io.airbyte.domain.models.OrganizationDomainVerification
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import java.time.OffsetDateTime
import java.util.UUID

private val logger = KotlinLogging.logger { }

@Singleton
open class OrganizationDomainVerificationService(
  private val repository: OrganizationDomainVerificationRepository,
  private val dnsVerificationService: DnsVerificationService,
  private val ssoConfigService: SsoConfigService,
  private val organizationEmailDomainService: OrganizationEmailDomainService,
) {
  companion object {
    const val DNS_RECORD_PREFIX = "_airbyte-verification"
    const val DNS_RECORD_VALUE_PREFIX = "airbyte-domain-verification="
    const val EXPIRY_DAYS = 14L
  }

  /**
   * Creates a new domain verification record.
   * Wrapped in transaction to prevent race condition where two concurrent requests
   * could both pass the existence check and create duplicate records.
   */
  @Transactional("config")
  open fun createDomainVerification(
    organizationId: UUID,
    domain: String,
    createdBy: UUID?,
  ): OrganizationDomainVerification {
    val verificationToken = UUID.randomUUID().toString()
    val dnsRecordName = "$DNS_RECORD_PREFIX.$domain"
    val expiresAt = OffsetDateTime.now().plusDays(EXPIRY_DAYS)

    val domainVerification =
      OrganizationDomainVerification(
        id = null,
        organizationId = organizationId,
        domain = domain,
        verificationMethod = DomainVerificationMethod.DNS_TXT,
        status = DomainVerificationStatus.PENDING,
        verificationToken = verificationToken,
        dnsRecordName = dnsRecordName,
        dnsRecordPrefix = DNS_RECORD_PREFIX,
        expiresAt = expiresAt,
        attempts = 0,
        lastCheckedAt = null,
        createdBy = createdBy,
        verifiedAt = null,
        createdAt = null,
        updatedAt = null,
        tombstone = false,
      )

    // Check if domain already exists
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
              "with value '$DNS_RECORD_VALUE_PREFIX${existing.verificationToken}'",
          )
        }

        DomainVerificationStatus.FAILED -> {
          throw IllegalArgumentException(
            "Domain '${domainVerification.domain}' verification failed due to DNS misconfiguration. " +
              "A TXT record was found at '${existing.dnsRecordName}' but with the wrong value. " +
              "Please update the TXT record to: '$DNS_RECORD_VALUE_PREFIX${existing.verificationToken}'. " +
              "Once corrected, you can retry the verification check.",
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
  fun findByOrganizationId(
    organizationId: UUID,
    includeDeleted: Boolean = false,
  ): List<OrganizationDomainVerification> = repository.findByOrganizationId(organizationId, includeDeleted).map { it.toDomainModel() }

  /**
   * Gets a specific domain verification by ID.
   */
  fun getDomainVerification(id: UUID): OrganizationDomainVerification {
    val entity =
      repository.findById(id).orElse(null)
        ?: throw IllegalArgumentException("Domain verification not found with id: $id")

    if (entity.tombstone) {
      throw IllegalArgumentException("Domain verification with id $id was deleted")
    }
    return entity.toDomainModel()
  }

  /**
   * Finds a specific domain verification by organization and domain.
   */
  fun findByOrganizationIdAndDomain(
    organizationId: UUID,
    domain: String,
    includeDeleted: Boolean = false,
  ): OrganizationDomainVerification? = repository.findByOrganizationIdAndDomain(organizationId, domain, includeDeleted)?.toDomainModel()

  /**
   * Finds verifications by status (for background verification job).
   */
  fun findByStatus(
    status: DomainVerificationStatus,
    includeDeleted: Boolean = false,
  ): List<OrganizationDomainVerification> = repository.findByStatus(status.toEntityEnum(), includeDeleted).map { it.toDomainModel() }

  /**
   * Updates domain verification status (called by cron job).
   * Note: When called from handler methods, inherits transaction from applyVerificationResult.
   */
  @Transactional("config")
  open fun updateVerificationStatus(
    id: UUID,
    status: DomainVerificationStatus,
    verifiedAt: OffsetDateTime? = null,
  ): OrganizationDomainVerification {
    val entity =
      repository.findById(id).orElse(null)
        ?: throw IllegalArgumentException("Domain verification not found with id: $id")

    if (entity.tombstone) {
      throw IllegalArgumentException("Domain verification with id $id is no longer active")
    }

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

    val updatedEntity = repository.update(entity)
    return updatedEntity.toDomainModel()
  }

  /**
   * Check a domain verification and update status accordingly.
   *
   * Business logic:
   * 1. Fetch verification record
   * 2. Perform DNS lookup to verify domain ownership
   * 3. If DNS matches (VERIFIED):
   *    - Mark as VERIFIED
   *    - Check if org has active SSO config and create enforcement record if so
   * 4. If DNS record found but wrong value (MISCONFIGURED):
   *    - Mark as FAILED (requires user intervention)
   * 5. If no DNS record found (NOT_FOUND):
   *    - Check if expired (14 days) -> mark as EXPIRED
   *    - Otherwise stay PENDING and increment attempts
   * 6. Save and return updated verification
   *
   * @param verificationId The ID of the verification to check
   * @return The updated verification record
   */
  open fun checkAndUpdateVerification(verificationId: UUID): OrganizationDomainVerification {
    val verification = getDomainVerification(verificationId)
    val dnsResult = performDnsCheck(verification)

    logger.info {
      "Processing domain verification ${verification.id} for domain ${verification.domain} " +
        "(attempt ${verification.attempts}, status ${verification.status}, result=$dnsResult)"
    }

    return applyVerificationResult(verificationId, verification, dnsResult)
  }

  /**
   * Performs DNS check without holding a database transaction.
   * DNS lookups can take up to 5 seconds, so we don't want to hold DB connections during network I/O.
   */
  private fun performDnsCheck(verification: OrganizationDomainVerification): DnsVerificationResult {
    val expectedDnsValue = "$DNS_RECORD_VALUE_PREFIX${verification.verificationToken}"
    val dnsRecordName =
      verification.dnsRecordName
        ?: throw IllegalStateException("Domain verification ${verification.id} has no DNS record name")

    return dnsVerificationService.checkDomainVerification(dnsRecordName, expectedDnsValue)
  }

  /**
   * Applies the DNS verification result and updates the database within a transaction.
   */
  @Transactional("config")
  open fun applyVerificationResult(
    verificationId: UUID,
    verification: OrganizationDomainVerification,
    dnsResult: DnsVerificationResult,
  ): OrganizationDomainVerification =
    when (dnsResult) {
      is DnsVerificationResult.Verified -> handleSuccessfulVerification(verificationId, verification)
      is DnsVerificationResult.Misconfigured -> handleMisconfiguredVerification(verificationId, verification, dnsResult)
      is DnsVerificationResult.NotFound -> handleNotFoundVerification(verificationId, verification)
    }

  /**
   * Soft-deletes a domain verification by marking it as tombstoned.
   * The record remains in the database for audit purposes but is excluded from normal queries.
   * This operation is idempotent - calling it on an already deleted record is safe.
   */
  @Transactional("config")
  open fun deleteDomainVerification(domainVerificationId: UUID) {
    val entity =
      repository
        .findById(domainVerificationId)
        .orElseThrow {
          IllegalArgumentException("Domain verification not found with id $domainVerificationId")
        }

    if (entity.tombstone) {
      logger.warn { "Domain verification $domainVerificationId is already deleted" }
      return
    }

    val domainModel = entity.toDomainModel()

    entity.tombstone = true
    repository.update(entity)
    logger.info {
      "Soft-deleted domain verification $domainVerificationId for domain ${domainModel.domain} " +
        "in organization ${domainModel.organizationId}"
    }

    // Remove SSO enforcement if it exists
    organizationEmailDomainService.deleteByOrganizationIdAndDomain(
      domainModel.organizationId,
      domainModel.domain,
    )
    logger.info {
      "Cascaded deletion: removed SSO enforcement for domain ${domainModel.domain} " +
        "in organization ${domainModel.organizationId}"
    }
  }

  /**
   * Resets a failed or expired domain verification back to pending status.
   * This allows users to retry verification after fixing DNS configuration.
   *
   * Only FAILED and EXPIRED verifications can be reset.
   */
  @Transactional("config")
  open fun resetDomainVerification(domainVerificationId: UUID): OrganizationDomainVerification {
    val entity =
      repository
        .findById(domainVerificationId)
        .orElseThrow {
          IllegalArgumentException("Domain verification not found with id $domainVerificationId")
        }

    if (entity.tombstone) {
      throw IllegalArgumentException(
        "Cannot reset a deleted domain verification. Please create a new verification instead.",
      )
    }

    val domainModel = entity.toDomainModel()

    // Validate that domain is in a resettable state
    when (domainModel.status) {
      DomainVerificationStatus.FAILED, DomainVerificationStatus.EXPIRED -> {
        logger.info {
          "Resetting domain verification $domainVerificationId for ${domainModel.domain} " +
            "from status ${domainModel.status} to PENDING"
        }
      }
      DomainVerificationStatus.PENDING -> {
        throw IllegalArgumentException(
          "Domain verification for '${domainModel.domain}' is already pending. " +
            "The verification check is running automatically.",
        )
      }
      DomainVerificationStatus.VERIFIED -> {
        throw IllegalArgumentException(
          "Domain verification for '${domainModel.domain}' is already verified. " +
            "To reverify, please delete the verification and create a new one.",
        )
      }
    }

    // Reset to pending state
    entity.status = DomainVerificationStatus.PENDING.toEntityEnum()
    entity.attempts = 0
    entity.lastCheckedAt = null
    entity.expiresAt = OffsetDateTime.now().plusDays(EXPIRY_DAYS)

    val updatedEntity = repository.update(entity)

    logger.info {
      "Reset domain verification $domainVerificationId for ${domainModel.domain} " +
        "in organization ${domainModel.organizationId}. Cron will resume DNS checks."
    }

    return updatedEntity.toDomainModel()
  }

  private fun handleSuccessfulVerification(
    verificationId: UUID,
    verification: OrganizationDomainVerification,
  ): OrganizationDomainVerification {
    logger.info { "Domain verification SUCCESS for ${verification.domain}" }

    val updated =
      updateVerificationStatus(
        id = verificationId,
        status = DomainVerificationStatus.VERIFIED,
        verifiedAt = OffsetDateTime.now(),
      )

    val ssoConfig = ssoConfigService.getSsoConfig(verification.organizationId)
    if (ssoConfig?.status == SsoConfigStatus.ACTIVE) {
      logger.info {
        "Organization ${verification.organizationId} has active SSO. " +
          "Creating email domain enforcement for ${verification.domain}"
      }

      val domainExists =
        organizationEmailDomainService.existsByOrganizationIdAndDomain(
          verification.organizationId,
          verification.domain,
        )

      if (!domainExists) {
        organizationEmailDomainService.createEmailDomain(
          OrganizationEmailDomain()
            .withOrganizationId(verification.organizationId)
            .withEmailDomain(verification.domain),
        )
        logger.info {
          "Created email domain enforcement for ${verification.domain} " +
            "in organization ${verification.organizationId}"
        }
      }
    }

    return updated
  }

  private fun handleMisconfiguredVerification(
    verificationId: UUID,
    verification: OrganizationDomainVerification,
    result: DnsVerificationResult.Misconfigured,
  ): OrganizationDomainVerification {
    logger.warn {
      "Domain verification MISCONFIGURED for ${verification.domain}. " +
        "Found incorrect TXT records: ${result.foundRecords}. " +
        "Expected value starts with: $DNS_RECORD_VALUE_PREFIX"
    }

    return updateVerificationStatus(
      id = verificationId,
      status = DomainVerificationStatus.FAILED,
    )
  }

  private fun handleNotFoundVerification(
    verificationId: UUID,
    verification: OrganizationDomainVerification,
  ): OrganizationDomainVerification {
    logger.debug { "No DNS TXT record found for ${verification.domain}" }

    val now = OffsetDateTime.now()

    return when {
      verification.expiresAt != null && now.isAfter(verification.expiresAt) -> {
        logger.info {
          "Domain verification EXPIRED for ${verification.domain} " +
            "(expires_at: ${verification.expiresAt})"
        }
        updateVerificationStatus(
          id = verificationId,
          status = DomainVerificationStatus.EXPIRED,
        )
      }
      else -> {
        logger.debug {
          "Domain verification continues for ${verification.domain} " +
            "(attempt ${verification.attempts + 1})"
        }
        updateVerificationStatus(
          id = verificationId,
          status = DomainVerificationStatus.PENDING,
        )
      }
    }
  }
}
