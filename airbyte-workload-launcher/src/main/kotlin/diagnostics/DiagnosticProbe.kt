/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.diagnostics

import io.airbyte.api.client.model.generated.DataplaneDiagnosticResult

/**
 * A health check that runs on its own schedule. [latestCompletedResults] runs on the heartbeat thread, so it must return a
 * snapshot of the saved results and never run the check. An empty list keeps the results Airbyte already saved.
 */
interface DiagnosticProbe {
  fun latestCompletedResults(): List<DataplaneDiagnosticResult>
}
