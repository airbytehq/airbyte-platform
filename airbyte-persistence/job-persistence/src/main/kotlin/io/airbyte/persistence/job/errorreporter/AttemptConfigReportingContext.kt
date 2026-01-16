/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.errorreporter

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.State

/**
 * Connector Attempt Config Reporting context.
 *
 * @param sourceConfig source configuration used for the attempt
 * @param destinationConfig destination configuration used for the attempt
 * @param state state during the attempt
 */

data class AttemptConfigReportingContext(
  val sourceConfig: JsonNode?,
  val destinationConfig: JsonNode?,
  val state: State?,
)
