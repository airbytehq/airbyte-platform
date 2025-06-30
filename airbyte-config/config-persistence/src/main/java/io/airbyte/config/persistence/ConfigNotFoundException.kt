/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.config.ConfigNotFoundType
import java.util.UUID

/**
 * Exception when the requested config cannot be found.
 */
class ConfigNotFoundException(
  val type: String?,
  val configId: String?,
) : Exception(String.format("config type: %s id: %s", type, configId)) {
  constructor(type: ConfigNotFoundType, configId: String?) : this(type.toString(), configId)

  constructor(type: ConfigNotFoundType, uuid: UUID) : this(type.toString(), uuid.toString())

  companion object {
    // This is a specific error type that is used when an organization cannot be found
    // from a given workspace. Workspaces will soon require an organization, so this
    // error is temporary and will be removed once the requirement is enforced.
    const val NO_ORGANIZATION_FOR_WORKSPACE: String = "NO_ORGANIZATION_FOR_WORKSPACE"

    private const val serialVersionUID: Long = 836273627
  }
}
