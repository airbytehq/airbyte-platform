/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.commands

import io.airbyte.config.ConnectorJobOutput

interface ConnectorCommand<Input> {
  val name: String

  fun start(
    input: Input,
    signalPayload: String?,
  ): String

  fun isTerminal(id: String): Boolean

  fun getOutput(id: String): ConnectorJobOutput

  fun cancel(id: String)
}
