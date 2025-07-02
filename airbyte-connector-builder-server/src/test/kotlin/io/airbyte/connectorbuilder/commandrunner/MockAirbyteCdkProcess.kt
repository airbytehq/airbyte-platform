/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.commandrunner

import java.io.IOException

/**
 * Mock class for wrapping a process.
 */
class MockAirbyteCdkProcess(
  private val process: Process,
) : AirbyteCdkProcess {
  @Throws(IOException::class)
  override fun start(): Process = this.getProcess()

  override fun getProcess(): Process = this.process

  override fun close() {}
}
