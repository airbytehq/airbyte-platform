/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.serde

import io.airbyte.commons.version.Version

/**
 * Airbyte Protocol serialization interface.
 *
 * @param <T> protocol type
</T> */
interface AirbyteMessageSerializer<T : Any> {
  fun serialize(message: T): String

  fun getTargetVersion(): Version?
}
