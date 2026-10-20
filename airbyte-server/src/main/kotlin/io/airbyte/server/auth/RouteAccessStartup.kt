/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.micronaut.context.event.StartupEvent
import io.micronaut.runtime.event.annotation.EventListener
import jakarta.inject.Singleton

@Singleton
class RouteAccessStartup(
  private val inventory: RouteAccessInventory,
) {
  @EventListener
  fun validate(event: StartupEvent) {
    inventory.validate()
  }
}
