/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import io.airbyte.api.model.generated.DestinationDiscoverRead
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.commons.enums.convertTo
import io.airbyte.config.DestinationCatalogWithId
import io.airbyte.api.model.generated.DestinationCatalog as ApiDestinationCatalog
import io.airbyte.api.model.generated.DestinationOperation as ApiDestinationOperation
import io.airbyte.config.DestinationCatalog as DestinationCatalogModel
import io.airbyte.config.DestinationOperation as DestinationOperationModel

fun DestinationCatalogWithId.toApi(): DestinationDiscoverRead =
  DestinationDiscoverRead()
    .catalog(this.catalog.toApi())
    .catalogId(this.catalogId.value)

fun DestinationCatalogModel.toApi(): ApiDestinationCatalog =
  ApiDestinationCatalog()
    .operations(this.operations.map { it.toApi() })

fun DestinationOperationModel.toApi(): ApiDestinationOperation =
  ApiDestinationOperation()
    .objectName(this.objectName)
    .syncMode(this.syncMode.convertTo<DestinationSyncMode>())
    .schema(this.jsonSchema)
    .matchingKeys(this.matchingKeys)

fun ApiDestinationOperation.toModel(): DestinationOperationModel =
  DestinationOperationModel(
    objectName = objectName,
    syncMode = syncMode.convertTo<io.airbyte.config.DestinationSyncMode>(),
    jsonSchema = schema,
    matchingKeys = matchingKeys,
  )

fun ApiDestinationCatalog.toModel(): DestinationCatalogModel =
  DestinationCatalogModel(
    operations = operations.map { it.toModel() },
  )
