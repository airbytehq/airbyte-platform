/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import io.airbyte.domain.models.DestinationCatalogId
import io.airbyte.protocol.models.v0.DestinationCatalog as ProtocolDestinationCatalog

data class DestinationCatalog(
  val operations: List<DestinationOperation>,
)

fun DestinationCatalog.toProtocol(): ProtocolDestinationCatalog =
  ProtocolDestinationCatalog()
    .withOperations(operations.map { it.toProtocol() })

fun ProtocolDestinationCatalog.toModel(): DestinationCatalog =
  DestinationCatalog(
    operations = operations.map { it.toModel() },
  )

data class DestinationCatalogWithId(
  val catalogId: DestinationCatalogId,
  val catalog: DestinationCatalog,
)
