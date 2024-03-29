/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol;

import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;

/**
 * Protocol serialization interface.
 */
public interface ProtocolSerializer {

  String serialize(final ConfiguredAirbyteCatalog configuredAirbyteCatalog);

}
