/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.config

import jakarta.inject.Singleton

@Singleton
class TemporalQueueConfiguration {
  val uiCommandsQueue = "ui_commands"
}
