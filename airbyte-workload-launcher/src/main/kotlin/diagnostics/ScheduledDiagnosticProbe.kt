/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.diagnostics

import io.airbyte.api.client.model.generated.DataplaneDiagnosticResult
import io.airbyte.workload.launcher.model.DataplaneConfig
import io.micronaut.context.event.ApplicationEventListener

/**
 * A [DiagnosticProbe] that runs on its own schedule and keeps its latest results for the heartbeat.
 */
abstract class ScheduledDiagnosticProbe :
  DiagnosticProbe,
  ApplicationEventListener<DataplaneConfig> {
  @Volatile
  protected var dataplane: DataplaneConfig? = null
    private set

  @Volatile
  private var latestResults: List<DataplaneDiagnosticResult> = emptyList()

  final override fun onApplicationEvent(event: DataplaneConfig) {
    dataplane = event
  }

  final override fun latestCompletedResults(): List<DataplaneDiagnosticResult> = latestResults

  protected fun publish(results: List<DataplaneDiagnosticResult>) {
    latestResults = results
  }
}
