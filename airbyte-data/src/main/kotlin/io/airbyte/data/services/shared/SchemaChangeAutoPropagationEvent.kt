package io.airbyte.data.services.shared

import com.fasterxml.jackson.annotation.JsonInclude
import io.airbyte.api.model.generated.CatalogDiff

@JsonInclude(JsonInclude.Include.NON_NULL)
class SchemaChangeAutoPropagationEvent(
  private val catalogDiff: CatalogDiff,
) : ConnectionEvent {
  fun getCatalogDiff(): CatalogDiff {
    return catalogDiff
  }

  fun getUpdateReason(): String? {
    return ConnectionAutoUpdatedReason.SCHEMA_CHANGE_AUTO_PROPAGATE.name
  }

  override fun getEventType(): ConnectionEvent.Type {
    return ConnectionEvent.Type.SCHEMA_UPDATE
  }
}
