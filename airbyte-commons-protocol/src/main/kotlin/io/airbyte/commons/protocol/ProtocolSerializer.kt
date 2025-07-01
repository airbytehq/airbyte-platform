/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol

import io.airbyte.config.ConfiguredAirbyteCatalog

/**
 * Protocol serialization interface.
 */
interface ProtocolSerializer {
  fun serialize(
    configuredAirbyteCatalog: ConfiguredAirbyteCatalog,
    supportsRefreshes: Boolean,
    target: SerializationTarget,
  ): String
}
