/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.airbyte.db.instance.configs.jooq.generated.enums.PrivateLinkStatus
import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import java.time.OffsetDateTime
import java.util.UUID

@MappedEntity("private_link")
data class PrivateLink(
  @field:Id
  @AutoPopulated
  var id: UUID? = null,
  var workspaceId: UUID,
  var dataplaneGroupId: UUID,
  var name: String,
  @field:TypeDef(type = DataType.OBJECT)
  var status: PrivateLinkStatus,
  var serviceRegion: String,
  var serviceName: String,
  var endpointId: String? = null,
  var dnsName: String? = null,
  var scopedConfigurationId: UUID? = null,
  @DateCreated
  var createdAt: OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: OffsetDateTime? = null,
)
