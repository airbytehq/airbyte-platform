/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import java.util.UUID

@MappedEntity("permission")
data class Permission(
  @field:Id
  @AutoPopulated
  var id: UUID? = null,
  var userId: UUID? = null,
  var workspaceId: UUID? = null,
  var organizationId: UUID? = null,
  @field:TypeDef(type = DataType.OBJECT)
  var permissionType: PermissionType,
  @DateCreated
  var createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: java.time.OffsetDateTime? = null,
  var serviceAccountId: UUID? = null,
)
