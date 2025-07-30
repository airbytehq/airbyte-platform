/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters

import io.airbyte.commons.enums.convertTo
import io.airbyte.api.client.model.generated.DestinationCatalog as ApiDestinationCatalog
import io.airbyte.api.client.model.generated.DestinationOperation as ApiDestinationOperation
import io.airbyte.protocol.models.v0.DestinationCatalog as ProtocolDestinationCatalog
import io.airbyte.protocol.models.v0.DestinationOperation as ProtocolDestinationOperation

fun ProtocolDestinationCatalog.toClientApi(): ApiDestinationCatalog =
  ApiDestinationCatalog(
    operations = operations.map { it.toClientApi() },
  )

fun ProtocolDestinationOperation.toClientApi(): ApiDestinationOperation =
  ApiDestinationOperation(
    objectName = objectName,
    syncMode = syncMode.convertTo<io.airbyte.api.client.model.generated.DestinationSyncMode>(),
    schema = jsonSchema,
    matchingKeys = matchingKeys,
  )
