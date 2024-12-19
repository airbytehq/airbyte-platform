package io.airbyte.commons.temporal.config

import jakarta.inject.Singleton

@Singleton
class TemporalQueueConfiguration {
  val uiCommandsQueue = "ui_commands"
}
