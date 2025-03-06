/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.repository.domain

import io.micronaut.core.annotation.Nullable
import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.Relation
import io.micronaut.data.annotation.sql.JoinColumn
import java.util.UUID

@MappedEntity("workload_label")
data class WorkloadLabel(
  @field:Id
  @AutoPopulated
  var id: UUID? = null,
  var key: String,
  var value: String,
  @Relation(value = Relation.Kind.MANY_TO_ONE)
  @JoinColumn(name = "workload_id")
  @Nullable
  var workload: Workload? = null,
) {
  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false

    other as WorkloadLabel

    if (id != other.id) return false
    if (key != other.key) return false
    return value == other.value
  }

  override fun hashCode(): Int {
    var result = id?.hashCode() ?: 0
    result = 31 * result + key.hashCode()
    result = 31 * result + value.hashCode()
    return result
  }

  override fun toString(): String = "WorkloadLabel(id=$id, key='$key', value='$value')"
}
