/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.serde

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.version.Version
import java.util.Optional

/**
 * Default Airbyte Protocol deserializer.
 *
 * @param <T> object type
</T> */
open class AirbyteMessageGenericDeserializer<T : Any>(
  private val targetVersion: Version,
  private val typeClass: Class<T>,
) : AirbyteMessageDeserializer<T> {
  override fun deserializeExact(json: String): Optional<T> = Jsons.tryDeserializeExact(json, typeClass)

  override fun getTargetVersion(): Version = targetVersion
}
