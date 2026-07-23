/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.airbyte.db.instance.configs.jooq.generated.enums.DomainVerificationMethod
import io.airbyte.db.instance.configs.jooq.generated.enums.DomainVerificationStatus
import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import java.util.UUID

@MappedEntity("organization_domain_verification")
data class OrganizationDomainVerification(
  @field:Id
  @AutoPopulated
  var id: UUID? = null,
  var organizationId: UUID,
  var domain: String,
  @field:TypeDef(type = DataType.OBJECT)
  var verificationMethod: DomainVerificationMethod,
  @field:TypeDef(type = DataType.OBJECT)
  var status: DomainVerificationStatus,
  var verificationToken: String? = null,
  var dnsRecordName: String? = null,
  var dnsRecordPrefix: String? = null,
  var attempts: Int = 0,
  var lastCheckedAt: java.time.OffsetDateTime? = null,
  var expiresAt: java.time.OffsetDateTime? = null,
  var createdBy: UUID? = null,
  var verifiedAt: java.time.OffsetDateTime? = null,
  @DateCreated
  var createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: java.time.OffsetDateTime? = null,
  var tombstone: Boolean = false,
)
