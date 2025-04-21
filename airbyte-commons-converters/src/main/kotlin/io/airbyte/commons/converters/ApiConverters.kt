/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters

import io.airbyte.commons.enums.Enums
import io.airbyte.api.model.generated.AirbyteStream as ApiAirbyteStream
import io.airbyte.api.model.generated.StreamDescriptor as ApiStreamDescriptor
import io.airbyte.api.model.generated.SyncMode as ApiSyncMode
import io.airbyte.config.AirbyteStream as InternalAirbyteStream
import io.airbyte.config.StreamDescriptor as InternalStreamDescriptor

class ApiConverters {
  companion object {
    @JvmStatic
    fun InternalAirbyteStream.toApi(): ApiAirbyteStream =
      ApiAirbyteStream()
        .name(name)
        .jsonSchema(jsonSchema)
        .supportedSyncModes(
          Enums.convertListTo(supportedSyncModes, ApiSyncMode::class.java),
        ).sourceDefinedCursor(if (sourceDefinedCursor != null) sourceDefinedCursor else false)
        .defaultCursorField(defaultCursorField)
        .sourceDefinedPrimaryKey(sourceDefinedPrimaryKey)
        .namespace(namespace)
        .isFileBased(isFileBased)
        .isResumable(isResumable)

    @JvmStatic
    fun ApiStreamDescriptor.toInternal(): InternalStreamDescriptor = InternalStreamDescriptor().withName(name).withNamespace(namespace)

    @JvmStatic
    fun io.airbyte.config.StreamDescriptor.toApi(): ApiStreamDescriptor = ApiStreamDescriptor().name(name).namespace(namespace)
  }
}
