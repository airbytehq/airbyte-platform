/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte

import io.airbyte.commons.server.handlers.DiagnosticToolHandler
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import io.mockk.mockk
import jakarta.inject.Singleton

@Factory
class TestFactory {
  @Singleton
  @Replaces(DiagnosticToolHandler::class)
  fun diagnosticToolHandler(): DiagnosticToolHandler = mockk()
}
