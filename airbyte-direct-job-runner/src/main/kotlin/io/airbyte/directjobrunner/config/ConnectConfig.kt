/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.config

import io.micronaut.context.annotation.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties("airbyte.direct-job-runner.connect")
data class ConnectConfig(
  val blockedCidrs: List<String> = emptyList(),
  val overallTimeout: Duration = Duration.ofSeconds(10),
) {
  val extraBlockedCidrs: List<String> get() = blockedCidrs.map { it.trim() }.filter { it.isNotEmpty() }
}
