/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.models

import java.util.UUID

data class PostprocessCatalogInput(
  val catalogId: UUID?,
  val connectionId: UUID?,
) {
  // This constructor is required for Jackson deserialization.
  @Suppress("unused")
  constructor() : this(null, null)
}
