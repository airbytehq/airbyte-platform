/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import java.util.Objects
import java.util.UUID
import kotlin.jvm.javaClass

/**
 * Activity that checks Data Worker capacity for an organization.
 *
 * Used to determine whether a job can run immediately or should wait
 * for capacity to become available.
 */
@ActivityInterface
interface CapacityCheckActivity {
  /**
   * Input for capacity check.
   */
  class CapacityCheckInput {
    @JvmField
    var jobId: Long? = null

    @JvmField
    var connectionId: UUID? = null

    @JvmField
    var organizationId: UUID? = null

    @JvmField
    var enforcementEnabled: Boolean = false

    constructor()

    constructor(jobId: Long?, connectionId: UUID?, organizationId: UUID?, enforcementEnabled: Boolean = false) {
      this.jobId = jobId
      this.connectionId = connectionId
      this.organizationId = organizationId
      this.enforcementEnabled = enforcementEnabled
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as CapacityCheckInput
      return jobId == that.jobId &&
        connectionId == that.connectionId &&
        organizationId == that.organizationId &&
        enforcementEnabled == that.enforcementEnabled
    }

    override fun hashCode(): Int = Objects.hash(jobId, connectionId, organizationId, enforcementEnabled)

    override fun toString(): String =
      "CapacityCheckInput{jobId=$jobId, connectionId=$connectionId, organizationId=$organizationId, enforcementEnabled=$enforcementEnabled}"
  }

  /**
   * Output for capacity check.
   */
  class CapacityCheckOutput {
    /**
     * Whether capacity is available to run the job immediately.
     */
    @JvmField
    var capacityAvailable: Boolean = true

    /**
     * Whether the job should use on-demand capacity (and be billed accordingly).
     */
    @JvmField
    var useOnDemandCapacity: Boolean = false

    /**
     * Whether capacity enforcement is enabled for this organization.
     */
    @JvmField
    var enforcementEnabled: Boolean = false

    constructor()

    constructor(capacityAvailable: Boolean, useOnDemandCapacity: Boolean, enforcementEnabled: Boolean) {
      this.capacityAvailable = capacityAvailable
      this.useOnDemandCapacity = useOnDemandCapacity
      this.enforcementEnabled = enforcementEnabled
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as CapacityCheckOutput
      return capacityAvailable == that.capacityAvailable &&
        useOnDemandCapacity == that.useOnDemandCapacity &&
        enforcementEnabled == that.enforcementEnabled
    }

    override fun hashCode(): Int = Objects.hash(capacityAvailable, useOnDemandCapacity, enforcementEnabled)

    override fun toString(): String =
      "CapacityCheckOutput{capacityAvailable=$capacityAvailable, useOnDemandCapacity=$useOnDemandCapacity, enforcementEnabled=$enforcementEnabled}"
  }

  /**
   * Check whether capacity is available for a connection's job.
   *
   * Returns information about:
   * - Whether capacity is available (within committed limits)
   * - Whether the job should use on-demand capacity (and be marked for billing)
   * - Whether capacity enforcement is enabled for this organization
   *
   * When committed capacity is exhausted but on-demand is enabled,
   * capacityAvailable=true and useOnDemandCapacity=true.
   *
   * When committed capacity is exhausted and on-demand is NOT enabled,
   * capacityAvailable=false and the job should wait.
   *
   * @param input Contains the job ID, connection ID, and organization ID
   * @return CapacityCheckOutput with availability information
   */
  @ActivityMethod
  fun checkCapacity(input: CapacityCheckInput): CapacityCheckOutput
}
