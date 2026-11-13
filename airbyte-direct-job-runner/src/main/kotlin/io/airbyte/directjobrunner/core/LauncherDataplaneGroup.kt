/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.core

import java.util.UUID

/**
 * The data plane group of the workload launcher that dispatched a job, sent with each dispatch. The runner reads
 * per-data-plane-group feature flags with it, the way the launcher does.
 */
sealed interface LauncherDataplaneGroup {
  data class Known(
    val dataplaneGroupId: UUID,
  ) : LauncherDataplaneGroup

  /** The launcher sent no group: it hasn't registered its data plane yet, or it predates sending one. */
  data object Unknown : LauncherDataplaneGroup
}
