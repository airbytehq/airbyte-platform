/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.diagnostics

import io.airbyte.api.client.model.generated.ScopeType
import io.airbyte.workload.launcher.model.DataplaneConfig
import java.util.UUID

/**
 * A secret store to check, and the workspace or organization it is configured for.
 */
data class SecretStorageProbeTarget(
  val secretStorageId: UUID,
  val scopeType: ScopeType,
  val scopeId: UUID,
)

/**
 * Supplies the distinct secret stores this data plane's workloads use.
 */
interface SecretStorageProbeTargets {
  fun current(dataplane: DataplaneConfig): List<SecretStorageProbeTarget>
}
