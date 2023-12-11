/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.fixtures

import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.pipeline.stages.StageName
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.github.oshai.kotlinlogging.KotlinLogging
import io.mockk.mockk
import java.lang.Exception

private val logger = KotlinLogging.logger {}

class TestStage(
  private val name: StageName,
  private val msgTemplate: (name: StageName, input: LaunchStageIO) -> String,
  private val shouldThrow: Boolean = false,
  metricPublisher: CustomMetricPublisher = mockk(relaxed = true),
) : LaunchStage(metricPublisher) {
  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    logger.info { msgTemplate(name, input) }

    if (shouldThrow) throw Exception()

    return input
  }

  override fun getStageName(): StageName = name
}
