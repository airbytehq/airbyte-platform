/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.serde

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.version.Version

/**
 * Default Airbyte Protocol serializer.
 *
 * @param <T> object type
</T> */
open class AirbyteMessageGenericSerializer<T : Any>(
  private val targetVersion: Version,
) : AirbyteMessageSerializer<T> {
  override fun serialize(message: T): String = Jsons.serialize(message)

  override fun getTargetVersion(): Version? = targetVersion
}
