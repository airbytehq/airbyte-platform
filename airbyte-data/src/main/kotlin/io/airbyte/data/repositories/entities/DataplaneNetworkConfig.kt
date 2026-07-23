/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.airbyte.db.instance.configs.jooq.generated.enums.CloudProvider
import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import java.time.OffsetDateTime
import java.util.UUID

@MappedEntity("dataplane_network_config")
data class DataplaneNetworkConfig(
  @field:Id
  @AutoPopulated
  var id: UUID? = null,
  var dataplaneGroupId: UUID,
  @field:TypeDef(type = DataType.OBJECT)
  var provider: CloudProvider,
  @field:TypeDef(type = DataType.JSON)
  var config: String,
  @DateCreated
  var createdAt: OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: OffsetDateTime? = null,
)
