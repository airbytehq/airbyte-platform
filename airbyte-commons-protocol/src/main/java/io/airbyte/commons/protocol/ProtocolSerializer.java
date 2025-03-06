/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol;

import io.airbyte.config.ConfiguredAirbyteCatalog;

/**
 * Protocol serialization interface.
 */
public interface ProtocolSerializer {

  String serialize(final ConfiguredAirbyteCatalog configuredAirbyteCatalog, final boolean supportsRefreshes);

}
