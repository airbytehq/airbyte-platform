/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import com.fasterxml.jackson.databind.JsonNode
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.annotation.event.PrePersist
import io.micronaut.data.model.DataType
import java.util.UUID

@MappedEntity("workspace")
open class Workspace(
  @field:Id
  var id: UUID? = null,
  var customerId: UUID? = null,
  var name: String,
  var slug: String,
  var email: String? = null,
  var initialSetupComplete: Boolean = false,
  var anonymousDataCollection: Boolean? = null,
  var sendNewsletter: Boolean? = null,
  var displaySetupWizard: Boolean? = null,
  var tombstone: Boolean = false,
  @field:TypeDef(type = DataType.JSON)
  var notifications: JsonNode? = null,
  var firstSyncComplete: Boolean? = null,
  var feedbackComplete: Boolean? = null,
  var dataplaneGroupId: UUID,
  @field:TypeDef(type = DataType.JSON)
  var webhookOperationConfigs: JsonNode? = null,
  var notificationSettings: JsonNode? = null,
  var organizationId: UUID,
  @DateCreated
  var createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: java.time.OffsetDateTime? = null,
) {
  // Use @PrePersist instead of @AutoPopulated so that we can set the id field
  // if desired prior to insertion.
  @PrePersist
  fun prePersist() {
    if (id == null) {
      id = UUID.randomUUID()
    }
  }
}
