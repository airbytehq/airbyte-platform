/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters;

import io.airbyte.api.model.generated.StreamDescriptor;

/**
 * Utilities that convert protocol types into API or client representations of the protocol type.
 */
@SuppressWarnings("LineLength")
public class ProtocolConverters {

  public static StreamDescriptor streamDescriptorToApi(final io.airbyte.protocol.models.StreamDescriptor protocolStreamDescriptor) {
    return new StreamDescriptor().name(protocolStreamDescriptor.getName()).namespace(protocolStreamDescriptor.getNamespace());
  }

  /**
   * Convert protocol stream descriptor to api stream descriptor.
   *
   * @param protocolStreamDescriptor protocol stream descriptor
   * @return api stream descriptor
   */
  public static io.airbyte.api.client.model.generated.StreamDescriptor streamDescriptorToClient(final io.airbyte.protocol.models.StreamDescriptor protocolStreamDescriptor) {
    return new io.airbyte.api.client.model.generated.StreamDescriptor(protocolStreamDescriptor.getName(), protocolStreamDescriptor.getNamespace());
  }

  public static io.airbyte.protocol.models.StreamDescriptor streamDescriptorToProtocol(final StreamDescriptor apiStreamDescriptor) {
    return new io.airbyte.protocol.models.StreamDescriptor().withName(apiStreamDescriptor.getName())
        .withNamespace(apiStreamDescriptor.getNamespace());
  }

  public static io.airbyte.config.StreamDescriptor streamDescriptorToDomain(final io.airbyte.protocol.models.StreamDescriptor protocolStreamDescriptor) {
    return new io.airbyte.config.StreamDescriptor()
        .withName(protocolStreamDescriptor.getName())
        .withNamespace(protocolStreamDescriptor.getNamespace());
  }

  public static io.airbyte.config.StreamDescriptor clientStreamDescriptorToDomain(final io.airbyte.api.client.model.generated.StreamDescriptor clientStreamDescriptor) {
    return new io.airbyte.config.StreamDescriptor().withName(clientStreamDescriptor.getName())
        .withNamespace(clientStreamDescriptor.getNamespace());
  }

  public static io.airbyte.protocol.models.StreamDescriptor clientStreamDescriptorToProtocol(final io.airbyte.api.client.model.generated.StreamDescriptor clientStreamDescriptor) {
    return new io.airbyte.protocol.models.StreamDescriptor().withName(clientStreamDescriptor.getName())
        .withNamespace(clientStreamDescriptor.getNamespace());
  }

}
