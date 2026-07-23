/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.airbyte.db.instance.configs.jooq.generated.enums.DataSubjectDeletionStatus
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.annotation.event.PrePersist
import io.micronaut.data.model.DataType
import java.time.OffsetDateTime
import java.util.UUID

/**
 * Row that tracks the two-phase GDPR / DSR deletion lifecycle.
 *
 * See `DsrDeletionService` and `V2_1_0_028__CreateDataSubjectDeletionRequestTable` for usage
 * and lifecycle semantics.
 */
@MappedEntity("data_subject_deletion_request")
open class DataSubjectDeletionRequest(
  @field:Id
  var id: UUID? = null,
  var email: String,
  var emailHash: String,
  var datagrailId: String,
  @field:TypeDef(type = DataType.OBJECT)
  var status: DataSubjectDeletionStatus,
  var userId: UUID? = null,
  var requestedBy: String,
  var oncallIssueNumber: String,
  var confirmedBy: String? = null,
  @field:TypeDef(type = DataType.JSON)
  var manifest: String,
  @field:TypeDef(type = DataType.JSON)
  var prepareWarnings: String? = null,
  @field:TypeDef(type = DataType.JSON)
  var confirmErrors: String? = null,
  @field:TypeDef(type = DataType.JSON)
  var executionCounts: String? = null,
  @DateCreated
  var preparedAt: OffsetDateTime? = null,
  var confirmedAt: OffsetDateTime? = null,
  var completedAt: OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: OffsetDateTime? = null,
) {
  // Use @PrePersist so callers may assign a deterministic UUID upfront and we still default it
  // when omitted.
  @PrePersist
  fun prePersist() {
    if (id == null) {
      id = UUID.randomUUID()
    }
  }
}
