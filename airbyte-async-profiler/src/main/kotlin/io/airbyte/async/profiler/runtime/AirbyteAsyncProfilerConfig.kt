/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.async.profiler.runtime

import io.airbyte.micronaut.runtime.AIRBYTE_PREFIX
import io.micronaut.context.annotation.ConfigurationProperties

internal const val DEFAULT_PROFILER_DESTINATION_KEYWORD = "destination"
internal const val DEFAULT_PROFILER_ORCHESTRATOR_KEYWORD = "orchestrator"
internal const val DEFAULT_PROFILER_MODE = "cpu"
internal const val DEFAULT_PROFILER_SOURCE_KEYWORD = "source"

@ConfigurationProperties("$AIRBYTE_PREFIX.async-profiler")
data class AirbyteAsyncProfilerConfig(
  val destinationKeyword: String = DEFAULT_PROFILER_DESTINATION_KEYWORD,
  val orchestratorKeyword: String = DEFAULT_PROFILER_ORCHESTRATOR_KEYWORD,
  val profilingMode: String = DEFAULT_PROFILER_MODE,
  val sourceKeyword: String = DEFAULT_PROFILER_SOURCE_KEYWORD,
)
