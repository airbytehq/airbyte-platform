/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

/**
 * Type of Airbyte workflow that runs in temporal.
 */
enum class TemporalJobType {
  GET_SPEC,
  CHECK_CONNECTION,
  DISCOVER_SCHEMA,
  SYNC,
  RESET_CONNECTION,
  CONNECTION_UPDATER,
}
