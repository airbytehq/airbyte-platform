/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import io.airbyte.domain.models.DestinationCatalogId

data class DestinationCatalog(
  val operations: List<DestinationOperation>,
)

data class DestinationCatalogWithId(
  val catalogId: DestinationCatalogId,
  val catalog: DestinationCatalog,
)
