/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.data.repositories.entities.Dataplane
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

typealias EntityDataplane = Dataplane
typealias ModelDataplane = io.airbyte.config.Dataplane

object DataplaneMapper {
  fun EntityDataplane.toConfigModel(): ModelDataplane =
    ModelDataplane()
      .withId(this.id)
      .withDataplaneGroupId(this.dataplaneGroupId)
      .withName(this.name)
      .withEnabled(this.enabled)
      .withCreatedAt(this.createdAt?.toEpochSecond())
      .withUpdatedAt(this.updatedAt?.toEpochSecond())
      .withTombstone(this.tombstone)
      .withServiceAccountId(this.serviceAccountId)

  fun ModelDataplane.toEntity(): EntityDataplane =
    EntityDataplane(
      id = this.id,
      dataplaneGroupId = this.dataplaneGroupId,
      name = this.name,
      enabled = this.enabled,
      createdAt = this.createdAt?.let { OffsetDateTime.ofInstant(Instant.ofEpochSecond(it), ZoneOffset.UTC) },
      updatedAt = this.updatedAt?.let { OffsetDateTime.ofInstant(Instant.ofEpochSecond(it), ZoneOffset.UTC) },
      tombstone = this.tombstone ?: false,
      serviceAccountId = this.serviceAccountId,
    )
}
