/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.io

import dev.failsafe.RetryPolicy
import io.airbyte.commons.constants.WorkerConstants.KubeConstants
import io.airbyte.protocol.models.v0.AirbyteMessage
import java.time.Duration

object LocalContainerConstants {
  private const val NORMAL_EXIT = 0
  private const val SIGTERM = 143

  val ACCEPTED_MESSAGE_TYPES =
    listOf(AirbyteMessage.Type.RECORD, AirbyteMessage.Type.STATE, AirbyteMessage.Type.TRACE, AirbyteMessage.Type.CONTROL)
  val IGNORED_EXIT_CODES = setOf(NORMAL_EXIT, SIGTERM)
  val LOCAL_CONTAINER_RETRY_POLICY: RetryPolicy<Any> =
    RetryPolicy
      .builder<Any>()
      .withBackoff(Duration.ofSeconds(10), KubeConstants.POD_READY_TIMEOUT)
      .build()
}
