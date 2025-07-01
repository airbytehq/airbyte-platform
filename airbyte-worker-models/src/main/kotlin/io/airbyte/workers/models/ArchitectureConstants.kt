/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.models

object ArchitectureConstants {
  const val SOCKET_PATH = "/var/run/sockets"
  const val SOCKET_FILE_PREFIX = "airbyte_socket_"
  const val SOCKET_FILE_POSTFIX = ".sock"

  const val DATA_CHANNEL_FORMAT = "DATA_CHANNEL_FORMAT"
  const val DATA_CHANNEL_MEDIUM = "DATA_CHANNEL_MEDIUM"
  const val DATA_CHANNEL_SOCKET_PATHS = "DATA_CHANNEL_SOCKET_PATHS"
  const val PLATFORM_MODE = "PLATFORM_MODE"

  const val BOOKKEEPER = "BOOKKEEPER"
  const val ORCHESTRATOR = "ORCHESTRATOR"
}
