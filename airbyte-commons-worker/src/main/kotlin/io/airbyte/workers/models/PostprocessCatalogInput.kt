package io.airbyte.workers.models

import java.util.UUID

data class PostprocessCatalogInput(val catalogId: UUID?, val connectionId: UUID?) {
  constructor() : this(null, null)
}
