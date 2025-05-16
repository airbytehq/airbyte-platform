/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.util

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import org.slf4j.MDC

object AsyncUtils {
  fun runAsync(
    dispatcher: CoroutineDispatcher,
    scope: CoroutineScope,
    mdc: Map<String, String>,
    block: suspend () -> Unit,
  ) = scope.async(dispatcher) {
    MDC.setContextMap(mdc)
    block()
  }

  fun runLaunch(
    dispatcher: CoroutineDispatcher,
    scope: CoroutineScope,
    mdc: Map<String, String>,
    block: suspend () -> Unit,
  ) = scope.launch(dispatcher) {
    MDC.setContextMap(mdc)
    block()
  }
}
