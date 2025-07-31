/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.commons.temporal.scheduling.retries.RetryManager
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import java.util.Objects
import java.util.UUID

/**
 * Sends and retrieves retry state data from persistence.
 */
@ActivityInterface
interface RetryStatePersistenceActivity {
  /**
   * Input for hydrate activity method.
   */
  class HydrateInput {
    @JvmField
    var jobId: Long? = null

    @JvmField
    var connectionId: UUID? = null

    constructor()

    constructor(jobId: Long?, connectionId: UUID?) {
      this.jobId = jobId
      this.connectionId = connectionId
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as HydrateInput
      return jobId == that.jobId && connectionId == that.connectionId
    }

    override fun hashCode(): Int = Objects.hash(jobId, connectionId)

    override fun toString(): String = "HydrateInput{jobId=" + jobId + ", connectionId=" + connectionId + '}'
  }

  /**
   * Output for hydrate activity method.
   */
  class HydrateOutput {
    @JvmField
    var manager: RetryManager? = null

    constructor()

    constructor(manager: RetryManager?) {
      this.manager = manager
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as HydrateOutput
      return manager == that.manager
    }

    override fun hashCode(): Int = Objects.hashCode(manager)

    override fun toString(): String = "HydrateOutput{manager=" + manager + '}'
  }

  /**
   * Input for persist activity method.
   */
  class PersistInput {
    @JvmField
    var jobId: Long? = null

    @JvmField
    var connectionId: UUID? = null

    @JvmField
    var manager: RetryManager? = null

    constructor()

    constructor(jobId: Long?, connectionId: UUID?, manager: RetryManager?) {
      this.jobId = jobId
      this.connectionId = connectionId
      this.manager = manager
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as PersistInput
      return jobId == that.jobId && connectionId == that.connectionId && manager == that.manager
    }

    override fun hashCode(): Int = Objects.hash(jobId, connectionId, manager)

    override fun toString(): String = "PersistInput{jobId=" + jobId + ", connectionId=" + connectionId + ", manager=" + manager + '}'
  }

  /**
   * Output for persist activity method.
   */
  class PersistOutput {
    @JvmField
    var success: Boolean? = null

    constructor()

    constructor(success: Boolean?) {
      this.success = success
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as PersistOutput
      return success == that.success
    }

    override fun hashCode(): Int = Objects.hashCode(success)

    override fun toString(): String = "PersistOutput{success=" + success + '}'
  }

  /**
   * Hydrates a RetryStateManager with data from persistence.
   *
   * @param input jobId — the id of the current job.
   * @return HydrateOutput wih hydrated RetryStateManager or new RetryStateManager if no state exists.
   */
  @ActivityMethod
  fun hydrateRetryState(input: HydrateInput): HydrateOutput

  /**
   * Persist the state of a RetryStateManager.
   *
   * @param input jobId, connectionId and RetryManager to be persisted.
   * @return PersistOutput with boolean denoting success.
   */
  @ActivityMethod
  fun persistRetryState(input: PersistInput): PersistOutput
}
