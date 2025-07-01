/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data

import io.airbyte.config.ConfigNotFoundType
import java.util.UUID

/**
 * Exception when the requested config cannot be found.
 */
class ConfigNotFoundException(
  @JvmField val type: String?,
  @JvmField val configId: String?,
) : Exception(String.format("config type: %s id: %s", type, configId)) {
  constructor(type: ConfigNotFoundType, configId: String?) : this(type.toString(), configId)

  constructor(type: ConfigNotFoundType, uuid: UUID) : this(type.toString(), uuid.toString())

  companion object {
    private const val serialVersionUID: Long = 836273627
  }
}
