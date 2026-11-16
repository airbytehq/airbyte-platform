/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.constants

object ApiConstants {
  const val AIRBYTE_VERSION_HEADER = "X-Airbyte-Version"

  /**
   * The data plane ID a launcher sends on token requests so Airbyte can attribute a rejected exchange. It never authenticates
   * the caller.
   */
  const val DIAGNOSTICS_DATAPLANE_ID_HEADER = "X-Airbyte-Diagnostics-Dataplane-Id"
}
