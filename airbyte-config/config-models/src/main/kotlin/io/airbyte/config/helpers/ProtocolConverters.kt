package io.airbyte.config.helpers

import io.airbyte.commons.enums.Enums
import io.airbyte.config.AirbyteStream as InternalAirbyteStream
import io.airbyte.config.StreamDescriptor as InternalStreamDescriptor
import io.airbyte.config.SyncMode as InternalSyncMode
import io.airbyte.protocol.models.AirbyteStream as ProtocolAirbyteStream
import io.airbyte.protocol.models.StreamDescriptor as ProtocolStreamDescriptor
import io.airbyte.protocol.models.SyncMode as ProtocolSyncMode

class ProtocolConverters {
  companion object {
    @JvmStatic
    fun ProtocolAirbyteStream.toInternal(): InternalAirbyteStream =
      InternalAirbyteStream()
        .withName(name)
        .withJsonSchema(jsonSchema)
        .withSupportedSyncModes(Enums.convertListTo(supportedSyncModes, InternalSyncMode::class.java))
        .withSourceDefinedCursor(sourceDefinedCursor)
        .withDefaultCursorField(defaultCursorField)
        .withSourceDefinedPrimaryKey(sourceDefinedPrimaryKey)
        .withNamespace(namespace)
        .withIsResumable(isResumable)

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

    @JvmStatic
    fun ProtocolStreamDescriptor.toInternal(): InternalStreamDescriptor = InternalStreamDescriptor().withName(name).withNamespace(namespace)

    @JvmStatic
    fun InternalStreamDescriptor.toProtocol(): ProtocolStreamDescriptor = ProtocolStreamDescriptor().withName(name).withNamespace(namespace)
  }
}
