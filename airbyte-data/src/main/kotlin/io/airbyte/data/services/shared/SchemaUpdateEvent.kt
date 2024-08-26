package io.airbyte.data.services.shared

import com.fasterxml.jackson.annotation.JsonInclude
import io.airbyte.api.model.generated.CatalogDiff

@JsonInclude(JsonInclude.Include.NON_NULL)
class SchemaUpdateEvent(
  private val startTimeEpochSeconds: Long,
  private val catalogDiff: CatalogDiff,
  private val updateReason: String? = null,
) : ConnectionEvent {
  fun getStartTimeEpochSeconds(): Long {
    return startTimeEpochSeconds
  }

  fun getCatalogDiff(): CatalogDiff {
    return catalogDiff
  }

  fun getUpdateReason(): String? {
    return updateReason
  }

  override fun getEventType(): ConnectionEvent.Type {
    return ConnectionEvent.Type.SCHEMA_UPDATE
  }
}
