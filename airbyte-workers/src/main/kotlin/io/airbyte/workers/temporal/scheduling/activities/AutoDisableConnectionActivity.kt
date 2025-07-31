/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import java.time.Instant
import java.util.Objects
import java.util.UUID

/**
 * AutoDisableConnectionActivity.
 */
@ActivityInterface
interface AutoDisableConnectionActivity {
  /**
   * AutoDisableConnectionActivityInput.
   */
  class AutoDisableConnectionActivityInput {
    @JvmField
    var connectionId: UUID? = null

    @Deprecated("")
    var currTimestamp: Instant? = null

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as AutoDisableConnectionActivityInput
      return connectionId == that.connectionId && currTimestamp == that.currTimestamp
    }

    override fun hashCode(): Int = Objects.hash(connectionId, currTimestamp)

    override fun toString(): String = "AutoDisableConnectionActivityInput{connectionId=" + connectionId + ", currTimestamp=" + currTimestamp + '}'
  }

  /**
   * AutoDisableConnectionOutput.
   */
  class AutoDisableConnectionOutput {
    var isDisabled: Boolean = false

    constructor(disabled: Boolean) {
      this.isDisabled = disabled
    }

    constructor()

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as AutoDisableConnectionOutput
      return this.isDisabled == that.isDisabled
    }

    override fun hashCode(): Int = Objects.hashCode(this.isDisabled)

    override fun toString(): String = "AutoDisableConnectionOutput{disabled=" + this.isDisabled + '}'
  }

  /**
   * Disable a connection if no successful sync jobs in the last MAX_FAILURE_JOBS_IN_A_ROW job
   * attempts or the last MAX_DAYS_OF_STRAIGHT_FAILURE days (minimum 1 job attempt): disable
   * connection to prevent wasting resources.
   */
  @ActivityMethod
  fun autoDisableFailingConnection(input: AutoDisableConnectionActivityInput): AutoDisableConnectionOutput
}
