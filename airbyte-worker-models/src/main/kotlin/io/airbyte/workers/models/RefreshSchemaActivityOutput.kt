/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.models

import io.airbyte.config.CatalogDiff

/**
 * A class holding the output to the Temporal schema refresh activity.
 */
data class RefreshSchemaActivityOutput(
  val appliedDiff: CatalogDiff? = null,
)
