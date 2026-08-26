/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.time.OffsetDateTime
import java.util.UUID

/**
 * Data Worker capacity allocated to an organization within a single dataplane group (region).
 *
 * `allocated_capacity` is a Postgres `REAL`, mapped here to [Double] to match the sibling
 * `data_worker_usage` and `data_worker_usage_reservation` entities and the `Double`-based Data
 * Worker services that consume it.
 *
 * [DateCreated] and [DateUpdated] only apply to repository `save`/`update` calls. Hand-written
 * `@Query` statements must set `updated_at` themselves.
 */
@MappedEntity("data_worker_allocated_capacity")
data class DataWorkerAllocatedCapacity(
  @field:Id
  @AutoPopulated
  var id: UUID? = null,
  var organizationId: UUID,
  var dataplaneGroupId: UUID,
  var allocatedCapacity: Double,
  @DateCreated
  var createdAt: OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: OffsetDateTime? = null,
)
