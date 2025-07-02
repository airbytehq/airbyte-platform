/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.commandrunner

import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import java.io.IOException

/**
 * Exposes a way of running synchronous processes via an Airbyte `read` command.
 */
interface SynchronousCdkCommandRunner {
  /**
   * Launch a CDK process responsible for handling requests.
   */
  @Throws(IOException::class)
  fun runCommand(
    command: String,
    config: String,
    catalog: String,
    state: String,
  ): AirbyteRecordMessage
}
