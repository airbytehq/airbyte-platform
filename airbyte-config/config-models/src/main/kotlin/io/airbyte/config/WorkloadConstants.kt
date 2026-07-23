/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import java.util.UUID

class WorkloadConstants {
  companion object {
    const val LAUNCH_ERROR_SOURCE = "launch"
    val PUBLIC_ORG_ID = UUID.fromString("00000000-0000-0000-0000-000000000000")
  }
}
