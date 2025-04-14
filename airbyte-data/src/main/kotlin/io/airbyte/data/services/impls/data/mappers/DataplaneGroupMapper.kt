/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.data.repositories.entities.DataplaneGroup
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

typealias EntityDataplaneGroup = DataplaneGroup
typealias ModelDataplaneGroup = io.airbyte.config.DataplaneGroup

object DataplaneGroupMapper {
  fun EntityDataplaneGroup.toConfigModel(): ModelDataplaneGroup =
    ModelDataplaneGroup()
      .withId(this.id)
      .withOrganizationId(this.organizationId)
      .withName(this.name)
      .withEnabled(this.enabled)
      .withCreatedAt(this.createdAt?.toEpochSecond())
      .withUpdatedAt(this.updatedAt?.toEpochSecond())
      .withTombstone(this.tombstone)

  fun ModelDataplaneGroup.toEntity(): EntityDataplaneGroup =
    EntityDataplaneGroup(
      id = this.id,
      organizationId = this.organizationId,
      name = this.name,
      enabled = this.enabled,
      createdAt = this.createdAt?.let { OffsetDateTime.ofInstant(Instant.ofEpochSecond(it), ZoneOffset.UTC) },
      updatedAt = this.updatedAt?.let { OffsetDateTime.ofInstant(Instant.ofEpochSecond(it), ZoneOffset.UTC) },
      tombstone = this.tombstone ?: false,
    )
}
