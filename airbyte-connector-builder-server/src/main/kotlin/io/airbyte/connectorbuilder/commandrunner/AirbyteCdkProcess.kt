/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.commandrunner

import java.io.IOException

/**
 * Interface for wrapping a process launched for CDK requests.
 */
interface AirbyteCdkProcess : AutoCloseable {
  fun getProcess(): Process

  fun start(): Process

  override fun close()
}
