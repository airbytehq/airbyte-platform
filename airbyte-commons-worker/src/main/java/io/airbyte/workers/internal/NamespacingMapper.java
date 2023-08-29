/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.StreamDescriptor;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * We apply some transformations on the fly on the catalog (same should be done on records too) from
 * the source before it reaches the destination. One of the transformation is to define the
 * destination namespace where data will be stored and how to mirror (or not) the namespace used in
 * the source (if any). This is configured in the UI through the syncInput.
 */
public class NamespacingMapper implements AirbyteMapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(NamespacingMapper.class);

  private final NamespaceDefinitionType namespaceDefinition;
  private final String namespaceFormat;
  private final String streamPrefix;
  private final Map<NamespaceAndStreamName, NamespaceAndStreamName> destinationToSourceNamespaceAndStreamName;

  @VisibleForTesting
  record NamespaceAndStreamName(String namespace, String streamName) {}

  public NamespacingMapper(
                           final NamespaceDefinitionType namespaceDefinition,
                           final String namespaceFormat,
                           final String streamPrefix) {
    this(namespaceDefinition, namespaceFormat, streamPrefix, new HashMap<>());
  }

  @VisibleForTesting
  NamespacingMapper(
                    final NamespaceDefinitionType namespaceDefinition,
                    final String namespaceFormat,
                    final String streamPrefix,
                    final Map<NamespaceAndStreamName, NamespaceAndStreamName> destinationToSourceNamespaceAndStreamName) {
    this.namespaceDefinition = namespaceDefinition;
    this.namespaceFormat = namespaceFormat;
    this.streamPrefix = streamPrefix;
    this.destinationToSourceNamespaceAndStreamName = destinationToSourceNamespaceAndStreamName;
  }

  @Override
  public ConfiguredAirbyteCatalog mapCatalog(final ConfiguredAirbyteCatalog inputCatalog) {
    final ConfiguredAirbyteCatalog catalog = Jsons.clone(inputCatalog);
    catalog.getStreams().forEach(s -> {
      final AirbyteStream stream = s.getStream();
      if (namespaceDefinition != null) {
        if (namespaceDefinition.equals(NamespaceDefinitionType.DESTINATION)) {
          stream.withNamespace(null);
        } else if (namespaceDefinition.equals(NamespaceDefinitionType.CUSTOMFORMAT)) {
          final String namespace = formatNamespace(stream.getNamespace(), namespaceFormat);
          if (namespace == null) {
            LOGGER.error("Namespace Format cannot be blank for Stream {}. Falling back to default namespace from destination settings",
                stream.getName());
          }
          stream.withNamespace(namespace);
        }
      }
      stream.withName(transformStreamName(stream.getName(), streamPrefix));
    });
    return catalog;
  }

  @Override
  public AirbyteMessage mapMessage(final AirbyteMessage message) {
    if (message.getType() == Type.RECORD) {
      final AirbyteRecordMessage recordMessage = message.getRecord();

      final String sourceNamespace = recordMessage.getNamespace();
      final String destinationNamespace = transformNamespace(sourceNamespace);
      final String destinationStreamName = transformStreamName(recordMessage.getStream(), streamPrefix);

      recordMessage.withNamespace(destinationNamespace);
      recordMessage.setStream(destinationStreamName);
    }

    if (message.getType() == Type.STATE) {
      final AirbyteStateMessage state = message.getState();
      if (state.getType() == AirbyteStateMessage.AirbyteStateType.STREAM) {
        final StreamDescriptor streamDescriptor = message.getState().getStream().getStreamDescriptor();

        final String sourceNamespace = streamDescriptor.getNamespace();
        final String sourceStreamName = streamDescriptor.getName();
        final String destinationNamespace = transformNamespace(sourceNamespace);
        final String destinationStreamName = transformStreamName(sourceStreamName, streamPrefix);

        destinationToSourceNamespaceAndStreamName.put(
            new NamespaceAndStreamName(destinationNamespace, destinationStreamName),
            new NamespaceAndStreamName(sourceNamespace, sourceStreamName));

        streamDescriptor.setNamespace(destinationNamespace);
        streamDescriptor.setName(destinationStreamName);
      }
    }
    return message;
  }

  /**
   * Reverts changes made to state messages by {@link #mapMessage(AirbyteMessage)}. In other words,
   * the state message namespace and stream name is set back to the values set by the source.
   *
   * If the given message is not of type STATE then the message is returned unchanged.
   */
  @Override
  public AirbyteMessage revertMap(final AirbyteMessage message) {
    if (message.getType() == Type.STATE) {
      final AirbyteStateMessage state = message.getState();
      if (state.getType() == AirbyteStateMessage.AirbyteStateType.STREAM) {
        final StreamDescriptor streamDescriptor = state.getStream().getStreamDescriptor();
        final NamespaceAndStreamName sourceNamespaceAndStreamName = destinationToSourceNamespaceAndStreamName.get(
            new NamespaceAndStreamName(streamDescriptor.getNamespace(), streamDescriptor.getName()));

        streamDescriptor.setNamespace(sourceNamespaceAndStreamName.namespace);
        streamDescriptor.setName(sourceNamespaceAndStreamName.streamName);
      }
    }
    return message;
  }

  private static String formatNamespace(final String sourceNamespace, final String namespaceFormat) {
    String result = "";
    if (Strings.isNotBlank(namespaceFormat)) {
      final String regex = Pattern.quote("${SOURCE_NAMESPACE}");
      result = namespaceFormat.replaceAll(regex, Strings.isNotBlank(sourceNamespace) ? sourceNamespace : "");
    }
    if (Strings.isBlank(result)) {
      result = null;
    }
    return result;
  }

  private String transformNamespace(final String sourceNamespace) {
    if (namespaceDefinition != null) {
      if (namespaceDefinition.equals(NamespaceDefinitionType.DESTINATION)) {
        return null;
      } else if (namespaceDefinition.equals(NamespaceDefinitionType.CUSTOMFORMAT)) {
        return formatNamespace(sourceNamespace, namespaceFormat);
      }
    }
    // Default behavior is to follow SOURCE
    return sourceNamespace;
  }

  private static String transformStreamName(final String streamName, final String streamPrefix) {
    if (Strings.isNotBlank(streamPrefix)) {
      return streamPrefix + streamName;
    } else {
      return streamName;
    }
  }

}
