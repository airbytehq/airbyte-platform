/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.db.instance.configs.jooq.generated.enums.ScimResourceType
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.annotation.event.PrePersist
import io.micronaut.data.model.DataType
import java.time.OffsetDateTime
import java.util.UUID

@MappedEntity("scim_resource_mapping")
data class ScimResourceMapping(
  @field:Id
  var id: UUID? = null,
  var scimConfigurationId: UUID,
  var organizationId: UUID,
  @field:TypeDef(type = DataType.OBJECT)
  var resourceType: ScimResourceType,
  var userId: UUID? = null,
  var groupId: UUID? = null,
  var externalId: String? = null,
  var userName: String? = null,
  var primaryEmail: String? = null,
  var userActive: Boolean? = null,
  @field:TypeDef(type = DataType.JSON)
  var attributes: JsonNode,
  @DateCreated
  var createdAt: OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: OffsetDateTime? = null,
) {
  @PrePersist
  fun prePersist() {
    if (id == null) {
      id = UUID.randomUUID()
    }
  }
}
