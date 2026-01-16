/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

typealias EntityOrganizationDomainVerification = io.airbyte.data.repositories.entities.OrganizationDomainVerification
typealias DomainOrganizationDomainVerification = io.airbyte.domain.models.OrganizationDomainVerification
typealias EntityDomainVerificationMethod = io.airbyte.db.instance.configs.jooq.generated.enums.DomainVerificationMethod
typealias EntityDomainVerificationStatus = io.airbyte.db.instance.configs.jooq.generated.enums.DomainVerificationStatus
typealias DomainDomainVerificationMethod = io.airbyte.domain.models.DomainVerificationMethod
typealias DomainDomainVerificationStatus = io.airbyte.domain.models.DomainVerificationStatus

// Entity → Domain Model
fun EntityOrganizationDomainVerification.toDomainModel(): DomainOrganizationDomainVerification =
  DomainOrganizationDomainVerification(
    id = this.id,
    organizationId = this.organizationId,
    domain = this.domain,
    verificationMethod = this.verificationMethod.toDomainEnum(),
    status = this.status.toDomainEnum(),
    verificationToken = this.verificationToken,
    dnsRecordName = this.dnsRecordName,
    dnsRecordPrefix = this.dnsRecordPrefix,
    attempts = this.attempts,
    lastCheckedAt = this.lastCheckedAt,
    expiresAt = this.expiresAt,
    createdBy = this.createdBy,
    verifiedAt = this.verifiedAt,
    createdAt = this.createdAt,
    updatedAt = this.updatedAt,
    tombstone = this.tombstone,
  )

// Domain Model → Entity
fun DomainOrganizationDomainVerification.toEntity(): EntityOrganizationDomainVerification =
  EntityOrganizationDomainVerification(
    id = this.id,
    organizationId = this.organizationId,
    domain = this.domain,
    verificationMethod = this.verificationMethod.toEntityEnum(),
    status = this.status.toEntityEnum(),
    verificationToken = this.verificationToken,
    dnsRecordName = this.dnsRecordName,
    dnsRecordPrefix = this.dnsRecordPrefix,
    attempts = this.attempts,
    lastCheckedAt = this.lastCheckedAt,
    expiresAt = this.expiresAt,
    createdBy = this.createdBy,
    verifiedAt = this.verifiedAt,
    createdAt = this.createdAt,
    updatedAt = this.updatedAt,
    tombstone = this.tombstone,
  )

// Entity Enum → Domain Enum
fun EntityDomainVerificationMethod.toDomainEnum(): DomainDomainVerificationMethod =
  when (this) {
    EntityDomainVerificationMethod.dns_txt -> DomainDomainVerificationMethod.DNS_TXT
    EntityDomainVerificationMethod.legacy -> DomainDomainVerificationMethod.LEGACY
  }

fun EntityDomainVerificationStatus.toDomainEnum(): DomainDomainVerificationStatus =
  when (this) {
    EntityDomainVerificationStatus.pending -> DomainDomainVerificationStatus.PENDING
    EntityDomainVerificationStatus.verified -> DomainDomainVerificationStatus.VERIFIED
    EntityDomainVerificationStatus.failed -> DomainDomainVerificationStatus.FAILED
    EntityDomainVerificationStatus.expired -> DomainDomainVerificationStatus.EXPIRED
  }

// Domain Enum → Entity Enum
fun DomainDomainVerificationMethod.toEntityEnum(): EntityDomainVerificationMethod =
  when (this) {
    DomainDomainVerificationMethod.DNS_TXT -> EntityDomainVerificationMethod.dns_txt
    DomainDomainVerificationMethod.LEGACY -> EntityDomainVerificationMethod.legacy
  }

fun DomainDomainVerificationStatus.toEntityEnum(): EntityDomainVerificationStatus =
  when (this) {
    DomainDomainVerificationStatus.PENDING -> EntityDomainVerificationStatus.pending
    DomainDomainVerificationStatus.VERIFIED -> EntityDomainVerificationStatus.verified
    DomainDomainVerificationStatus.FAILED -> EntityDomainVerificationStatus.failed
    DomainDomainVerificationStatus.EXPIRED -> EntityDomainVerificationStatus.expired
  }
