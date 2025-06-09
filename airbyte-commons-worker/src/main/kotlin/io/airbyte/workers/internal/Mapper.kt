/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteMessage.Type
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.github.oshai.kotlinlogging.KotlinLogging

/**
 * Interface to allow map operations on data as they pass from Source to Destination. This interface
 * will be updated in Protocol V2.
 */
interface AirbyteMapper {
  fun mapCatalog(catalog: ConfiguredAirbyteCatalog): ConfiguredAirbyteCatalog

  fun mapMessage(message: AirbyteMessage): AirbyteMessage

  fun revertMap(message: AirbyteMessage): AirbyteMessage
}

private val logger = KotlinLogging.logger { }

/**
 * We apply some transformations on the fly on the catalog (same should be done on records too) from
 * the source before it reaches the destination. One of the transformation is to define the
 * destination namespace where data will be stored and how to mirror (or not) the namespace used in
 * the source (if any). This is configured in the UI through the syncInput.
 *
 * @param namespaceDefinition optional type of namespace definition defined
 * @param namespaceFormat optional formatting string, if defined expects `${SOURCE_NAMESPACE}` to be present in the value
 * @param streamPrefix optional prefix to prepend to the stream names
 * @param destinationToSource map of destination [NamespaceStreamName] to source [NamespaceStreamName], only exposed for testing purposes
 */
class NamespacingMapper(
  private val namespaceDefinition: NamespaceDefinitionType?,
  private val namespaceFormat: String?,
  private val streamPrefix: String?,
  private val destinationToSource: MutableMap<NamespaceStreamName, NamespaceStreamName> = mutableMapOf(),
) : AirbyteMapper {
  override fun mapCatalog(catalog: ConfiguredAirbyteCatalog): ConfiguredAirbyteCatalog {
    val catalogCopy: ConfiguredAirbyteCatalog = Jsons.clone(catalog)
    catalogCopy.streams.forEach { configuredStream ->
      val stream = configuredStream.stream

      when (namespaceDefinition) {
        NamespaceDefinitionType.DESTINATION -> stream.withNamespace(null)
        NamespaceDefinitionType.CUSTOMFORMAT -> {
          val namespace = formatNamespace(stream.namespace, namespaceFormat)
          if (namespace == null) {
            logger.error {
              "Namespace Format cannot be blank for Stream ${stream.name}. Falling back to default namespace from destination settings"
            }
          }
          stream.withNamespace(namespace)
        }
        else -> Unit
      }
      stream.withName(transformStreamName(stream.name, streamPrefix))
    }
    return catalogCopy
  }

  override fun mapMessage(message: AirbyteMessage): AirbyteMessage {
    when (message.type) {
      Type.RECORD ->
        with(message.record) {
          this.withNamespace(transformNamespace(message.record.namespace))
          this.stream = transformStreamName(message.record.stream, streamPrefix)
        }
      Type.STATE ->
        with(message.state) {
          if (this.type != AirbyteStateType.STREAM) {
            return@with
          }
          val streamDescriptor = this.stream.streamDescriptor
          val sourceNamespace = streamDescriptor.namespace
          val sourceStreamName = streamDescriptor.name
          val destinationNamespace = transformNamespace(sourceNamespace)
          val destinationStreamName = transformStreamName(sourceStreamName, streamPrefix)

          destinationToSource[NamespaceStreamName(namespace = destinationNamespace, streamName = destinationStreamName)] =
            NamespaceStreamName(namespace = sourceNamespace, streamName = sourceStreamName)

          streamDescriptor.namespace = destinationNamespace
          streamDescriptor.name = destinationStreamName
        }
      Type.TRACE ->
        with(message.trace) {
          if (this.type != AirbyteTraceMessage.Type.STREAM_STATUS) {
            return@with
          }
          val streamDescriptor = this.streamStatus.streamDescriptor
          streamDescriptor.name = transformStreamName(streamDescriptor.name, streamPrefix)
          streamDescriptor.namespace = transformNamespace(streamDescriptor.namespace)
        }
      else -> Unit
    }

    return message
  }

  /**
   * Reverts changes made to state messages by {@link #mapMessage(AirbyteMessage)}. In other words,
   * the state message namespace and stream name is set back to the values set by the source.
   *
   * If the given message is not of type STATE then the message is returned unchanged.
   */
  override fun revertMap(message: AirbyteMessage): AirbyteMessage {
    when (message.type) {
      Type.STATE ->
        with(message.state) {
          if (this.type != AirbyteStateType.STREAM) {
            return@with
          }
          val streamDescriptor = this.stream.streamDescriptor
          destinationToSource[NamespaceStreamName(namespace = streamDescriptor.namespace, streamName = streamDescriptor.name)]
            ?.let {
              streamDescriptor.namespace = it.namespace
              streamDescriptor.name = it.streamName
            }
          // TODO what should happen if the entry wasn't found in the map?
        }
      else -> Unit
    }

    return message
  }

  private fun transformNamespace(sourceNamespace: String?): String? =
    when (namespaceDefinition) {
      NamespaceDefinitionType.DESTINATION -> null
      NamespaceDefinitionType.CUSTOMFORMAT -> formatNamespace(sourceNamespace, namespaceFormat)
      // default behavior is to follow SOURCE
      else -> sourceNamespace
    }
}

@VisibleForTesting
data class NamespaceStreamName(
  val namespace: String?,
  val streamName: String,
)

private fun formatNamespace(
  sourceNamespace: String?,
  namespaceFormat: String?,
): String? {
  var result: String? = ""
  namespaceFormat
    ?.takeIf { it.isNotBlank() }
    ?.let { format ->
      val replaceWith = sourceNamespace?.takeIf { it.isNotBlank() } ?: ""
      result = format.replace("\${SOURCE_NAMESPACE}", replaceWith)
    }

  return if (result.isNullOrBlank()) {
    null
  } else {
    result
  }
}

private fun transformStreamName(
  streamName: String,
  streamPrefix: String?,
): String =
  if (streamPrefix.isNullOrBlank()) {
    streamName
  } else {
    streamPrefix + streamName
  }
