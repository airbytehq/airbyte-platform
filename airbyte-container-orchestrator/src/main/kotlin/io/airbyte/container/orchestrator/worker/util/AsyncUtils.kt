/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.util

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.withContext

object AsyncUtils {
  fun runAsync(
    dispatcher: CoroutineDispatcher,
    scope: CoroutineScope,
    mdc: Map<String, String>,
    block: suspend () -> Unit,
  ) = scope.async(dispatcher) {
    withContext(MDCContext(mdc)) {
      block()
    }
  }

  fun runLaunch(
    dispatcher: CoroutineDispatcher,
    scope: CoroutineScope,
    mdc: Map<String, String>,
    block: suspend () -> Unit,
  ) = scope.launch(dispatcher) {
    withContext(MDCContext(mdc)) {
      block()
    }
  }
}
