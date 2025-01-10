/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.client

enum class RolloutCommand(val command: String) {
  START("start"),
  FIND("find"),
  GET("get"),
  ROLLOUT("rollout"),
  PROMOTE("promote"),
  FAIL("fail"),
  CANCEL("cancel"),
  ;

  companion object {
    fun fromString(command: String): RolloutCommand {
      return entries.find { it.command.equals(command, ignoreCase = true) }
        ?: throw IllegalArgumentException("Unknown command: $command")
    }
  }
}
