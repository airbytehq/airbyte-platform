/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import io.airbyte.commons.enums.Enums
import io.airbyte.config.AirbyteStream as InternalAirbyteStream
import io.airbyte.config.StreamDescriptor as InternalStreamDescriptor
import io.airbyte.config.SyncMode as InternalSyncMode
import io.airbyte.protocol.models.v0.AirbyteStream as ProtocolAirbyteStream
import io.airbyte.protocol.models.v0.StreamDescriptor as ProtocolStreamDescriptor
import io.airbyte.protocol.models.v0.SyncMode as ProtocolSyncMode

class ProtocolConverters {
  companion object {
    @JvmStatic
    fun ProtocolAirbyteStream.toInternal(): InternalAirbyteStream =
      InternalAirbyteStream(
        name = name,
        jsonSchema = jsonSchema,
        supportedSyncModes = Enums.convertListTo(supportedSyncModes, InternalSyncMode::class.java),
        sourceDefinedCursor = sourceDefinedCursor,
        defaultCursorField = defaultCursorField,
        sourceDefinedPrimaryKey = sourceDefinedPrimaryKey,
        namespace = namespace,
        isResumable = isResumable,
        isFileBased = isFileBased,
      )

    @JvmStatic
    fun InternalAirbyteStream.toProtocol(): ProtocolAirbyteStream =
      ProtocolAirbyteStream()
        .withName(name)
        .withJsonSchema(jsonSchema)
        .withSupportedSyncModes(Enums.convertListTo(supportedSyncModes, ProtocolSyncMode::class.java))
        .withSourceDefinedCursor(sourceDefinedCursor)
        .withDefaultCursorField(defaultCursorField)
        .withSourceDefinedPrimaryKey(sourceDefinedPrimaryKey)
        .withNamespace(namespace)
        .withIsResumable(isResumable)
        .withIsFileBased(isFileBased)

    @JvmStatic
    fun ProtocolStreamDescriptor.toInternal(): InternalStreamDescriptor = InternalStreamDescriptor().withName(name).withNamespace(namespace)

    @JvmStatic
    fun InternalStreamDescriptor.toProtocol(): ProtocolStreamDescriptor = ProtocolStreamDescriptor().withName(name).withNamespace(namespace)
  }
}
