/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.workers.temporal.activities.GetConnectionContextInput
import io.airbyte.workers.temporal.activities.GetConnectionContextOutput
import io.airbyte.workers.temporal.activities.GetLoadShedBackoffInput
import io.airbyte.workers.temporal.activities.GetLoadShedBackoffOutput
import io.airbyte.workers.temporal.activities.GetWebhookConfigInput
import io.airbyte.workers.temporal.activities.GetWebhookConfigOutput
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import java.time.Duration
import java.util.Objects
import java.util.Optional
import java.util.UUID

/**
 * ConfigFetchActivity.
 */
@ActivityInterface
interface ConfigFetchActivity {
  @ActivityMethod
  fun getSourceId(connectionId: UUID): Optional<UUID>

  @ActivityMethod
  fun getSourceConfig(sourceId: UUID): JsonNode

  @ActivityMethod
  fun getStatus(connectionId: UUID): Optional<ConnectionStatus>

  /**
   * ScheduleRetrieverInput.
   */
  class ScheduleRetrieverInput {
    @JvmField
    var connectionId: UUID? = null

    constructor()

    constructor(connectionId: UUID?) {
      this.connectionId = connectionId
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as ScheduleRetrieverInput
      return connectionId == that.connectionId
    }

    override fun hashCode(): Int = Objects.hashCode(connectionId)

    override fun toString(): String = "ScheduleRetrieverInput{connectionId=" + connectionId + '}'
  }

  /**
   * ScheduleRetrieverOutput.
   */
  class ScheduleRetrieverOutput {
    @JvmField
    var timeToWait: Duration? = null

    constructor()

    constructor(timeToWait: Duration?) {
      this.timeToWait = timeToWait
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as ScheduleRetrieverOutput
      return timeToWait == that.timeToWait
    }

    override fun hashCode(): Int = Objects.hashCode(timeToWait)

    override fun toString(): String = "ScheduleRetrieverOutput{timeToWait=" + timeToWait + '}'
  }

  /**
   * Return how much time to wait before running the next sync. It will query the DB to get the last
   * starting time of the latest terminal job (Failed, canceled or successful) and return the amount
   * of second the Workflow needs to await.
   */
  @ActivityMethod
  fun getTimeToWait(input: ScheduleRetrieverInput): ScheduleRetrieverOutput

  /**
   * Return a fully hydrated connection context (all the domain object ids relevant to the
   * connection).
   */
  @ActivityMethod
  fun getConnectionContext(input: GetConnectionContextInput): GetConnectionContextOutput

  /**
   * Return how much time to wait before checking load shed status. Consumer will wait in a loop until
   * this returns 0 or less.
   */
  @ActivityMethod
  fun getLoadShedBackoff(input: GetLoadShedBackoffInput): GetLoadShedBackoffOutput

  /**
   * GetMaxAttemptOutput.
   */
  @JvmRecord
  data class GetMaxAttemptOutput(
    val maxAttempt: Int,
  )

  fun getMaxAttempt(): GetMaxAttemptOutput

  @ActivityMethod
  fun isWorkspaceTombstone(connectionId: UUID): Boolean

  @ActivityMethod
  fun getWebhookConfig(input: GetWebhookConfigInput): GetWebhookConfigOutput
}
