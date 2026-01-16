/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import java.time.OffsetDateTime
import java.util.UUID

enum class DomainVerificationMethod {
  DNS_TXT,
  LEGACY,
}

enum class DomainVerificationStatus {
  PENDING,
  VERIFIED,
  EXPIRED,
  FAILED,
}

data class OrganizationDomainVerification(
  val id: UUID? = null,
  val organizationId: UUID,
  val domain: String,
  val verificationMethod: DomainVerificationMethod,
  val status: DomainVerificationStatus,
  val verificationToken: String?,
  val dnsRecordName: String?,
  val dnsRecordPrefix: String?,
  val attempts: Int = 0,
  val lastCheckedAt: OffsetDateTime?,
  val expiresAt: OffsetDateTime?,
  val createdBy: UUID?,
  val verifiedAt: OffsetDateTime?,
  val createdAt: OffsetDateTime?,
  val updatedAt: OffsetDateTime?,
  val tombstone: Boolean = false,
)
