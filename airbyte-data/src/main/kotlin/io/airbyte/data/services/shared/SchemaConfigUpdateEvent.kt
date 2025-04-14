/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import com.fasterxml.jackson.annotation.JsonInclude
import io.airbyte.api.model.generated.AirbyteCatalogDiff

@JsonInclude(JsonInclude.Include.NON_NULL)
class SchemaConfigUpdateEvent(
  private val airbyteCatalogDiff: AirbyteCatalogDiff,
) : ConnectionEvent {
  fun getAirbyteCatalogDiff(): AirbyteCatalogDiff = airbyteCatalogDiff

  override fun getEventType(): ConnectionEvent.Type = ConnectionEvent.Type.SCHEMA_CONFIG_UPDATE
}
