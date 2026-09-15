/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.dataworker

import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.CommittedDataWorkersEntitlement
import io.airbyte.config.Job
import io.airbyte.data.repositories.DataWorkerUsageReservationRepository
import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.entities.DataWorkerUsage
import io.airbyte.data.repositories.entities.DataWorkerUsageReservation
import io.airbyte.domain.models.DataplaneGroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.domain.models.dataworker.OrganizationDataWorkerAllocations
import io.airbyte.featureflag.EnableDataWorkerAllocation
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.transaction.TransactionOperations
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.sql.Connection
import kotlin.ranges.coerceAtLeast

private val logger = KotlinLogging.logger {}

/**
 * Result of a capacity check operation.
 */
data class CapacityCheckResult(
  /**
   * Whether the job can proceed immediately.
   */
  val hasAvailableCapacity: Boolean,
  /**
   * Current number of data workers in use by the organization.
   */
  val currentDataWorkers: Double,
  /**
   * Number of committed data workers available to the job, in the region it runs in.
   */
  val committedDataWorkers: Double,
  /**
   * Number of data workers required by the job being evaluated.
   */
  val requiredDataWorkers: Double,
  /**
   * Whether the job should use on-demand capacity if it proceeds.
   */
  val usedOnDemandCapacity: Boolean,
  /**
   * Whether the value was actually persisted in the reservation table.
   */
  val wasPersisted: Boolean,
)

/**
 * Snapshot of an organization's current Data Worker capacity.
 */
data class DataWorkerCapacityStatus(
  val currentDataWorkers: Double,
  val committedDataWorkers: Double,
) {
  val hasAvailableDataWorkers: Boolean
    get() = currentDataWorkers < committedDataWorkers
}

/**
 * Service for checking Data Worker capacity for an organization.
 *
 * This service determines whether an organization has available committed capacity
 * to run additional jobs. It compares the current data worker usage against the
 * organization's committed data worker entitlement.
 */
@Singleton
open class DataWorkerCapacityService(
  private val entitlementService: EntitlementService,
  private val organizationRepository: OrganizationRepository,
  private val dataWorkerUsageReservationRepository: DataWorkerUsageReservationRepository,
  private val dataWorkerUsageService: DataWorkerUsageService,
  private val dataWorkerAllocatedCapacityService: DataWorkerAllocatedCapacityService,
  private val featureFlagClient: FeatureFlagClient,
  @param:Named("config") private val configTransactionOperations: TransactionOperations<Connection>,
) {
  /**
   * Get the Data Worker capacity status for the region a workspace runs in.
   *
   * Reports organization-wide numbers only when the flag is off or the region cannot be resolved.
   * A known region reports zero when it has no allocation.
   */
  open fun getCapacityStatus(
    organizationId: OrganizationId,
    workspaceId: WorkspaceId,
  ): DataWorkerCapacityStatus {
    val allocations = allocationsOrNull(organizationId)
    val regionId =
      if (allocations == null) {
        null
      } else {
        dataWorkerUsageService.resolveDataplaneGroupIdForWorkspaceOrNull(workspaceId.value)?.let { DataplaneGroupId(it) }
      }

    return DataWorkerCapacityStatus(
      currentDataWorkers = getCurrentDataWorkersInUse(organizationId, regionId),
      committedDataWorkers = committedDataWorkers(organizationId, regionId, allocations),
    )
  }

  /**
   * Check if an organization has capacity for a job and reserve that capacity atomically.
   *
   * The organization row is locked so concurrent reservations for the same org serialize.
   * If the job was already reserved by a previous activity attempt, this method returns the
   * existing reservation result without reserving again.
   *
   * @param organizationId The organization to check capacity for
   * @param job The job that may reserve capacity
   * @param requiredDataWorkers The number of Data Workers required by the job
   * @param allowOnDemandCapacity Whether the job may use on-demand capacity
   * @return CapacityCheckResult containing availability, reservation, and usage details
   */
  open fun checkCapacityAndReserve(
    organizationId: OrganizationId,
    job: Job,
    requiredDataWorkers: Double,
    allowOnDemandCapacity: Boolean,
  ): CapacityCheckResult {
    // Decide the scope once, so usage and the cap it is compared against always cover the same thing.
    val allocations = allocationsOrNull(organizationId)
    val regionId =
      if (allocations == null) {
        null
      } else {
        dataWorkerUsageService.resolveDataplaneGroupIdForJobOrNull(job)?.let { DataplaneGroupId(it) }
      }

    val committedDataWorkers = committedDataWorkers(organizationId, regionId, allocations)
    var reservedUsage: DataWorkerUsage? = null

    val result =
      try {
        configTransactionOperations.executeWrite { _ ->
          // Serialize org-scoped admission so concurrent jobs cannot both read the same current usage,
          // independently pass the capacity check, and over-reserve committed data workers.
          lockOrganization(organizationId)

          val reservationAfterLock = findExistingReservation(job.id)
          if (reservationAfterLock != null) {
            return@executeWrite buildExistingReservationResult(
              organizationId,
              job.id,
              reservationAfterLock,
              committedDataWorkers,
              requiredDataWorkers,
              regionId,
            )
          }

          val currentDataWorkers = getCurrentDataWorkersInUse(organizationId, regionId)

          val hasCommittedCapacity = currentDataWorkers + requiredDataWorkers <= committedDataWorkers
          val usedOnDemandCapacity = !hasCommittedCapacity && allowOnDemandCapacity
          val hasAvailableCapacity = hasCommittedCapacity || usedOnDemandCapacity

          logger.debug {
            "Capacity check for org ${organizationId.value}: " +
              "current=$currentDataWorkers, required=$requiredDataWorkers, committed=$committedDataWorkers, " +
              "available=$hasAvailableCapacity, usedOnDemand=$usedOnDemandCapacity"
          }

          var persisted = false
          if (hasAvailableCapacity) {
            val preparedUsage = requirePreparedUsage(job, organizationId)
            persisted = dataWorkerUsageService.persistReservedUsageForJob(job.id, preparedUsage, usedOnDemandCapacity)
            if (persisted) {
              reservedUsage = preparedUsage
            }
          }

          CapacityCheckResult(
            hasAvailableCapacity = hasAvailableCapacity,
            currentDataWorkers = currentDataWorkers,
            committedDataWorkers = committedDataWorkers,
            requiredDataWorkers = requiredDataWorkers,
            usedOnDemandCapacity = usedOnDemandCapacity,
            wasPersisted = persisted,
          )
        }
      } catch (e: Exception) {
        reservedUsage?.let {
          dataWorkerUsageService.recordUsageMetric(job.id, false, DataWorkerUsageService.INCREMENT_OPERATION, it)
        }
        throw e
      }

    if (result.hasAvailableCapacity && result.wasPersisted) {
      reservedUsage?.let {
        dataWorkerUsageService.recordUsageMetric(job.id, true, DataWorkerUsageService.INCREMENT_OPERATION, it)
      }
    }

    return result
  }

  private fun findExistingReservation(jobId: Long): DataWorkerUsageReservation? = dataWorkerUsageReservationRepository.findById(jobId).orElse(null)

  private fun requirePreparedUsage(
    job: Job,
    organizationId: OrganizationId,
  ): DataWorkerUsage =
    dataWorkerUsageService.prepareUsageForJob(job, organizationId.value)
      ?: error("Unable to prepare data worker usage for job ${job.id} while reserving capacity")

  private fun buildExistingReservationResult(
    organizationId: OrganizationId,
    jobId: Long,
    reservation: DataWorkerUsageReservation,
    committedDataWorkers: Double,
    requiredDataWorkers: Double,
    regionId: DataplaneGroupId?,
  ): CapacityCheckResult {
    val currentDataWorkers = getCurrentDataWorkersInUse(organizationId, regionId)

    logger.debug {
      "Found existing capacity reservation for job $jobId in org ${organizationId.value}: " +
        "current=$currentDataWorkers, committed=$committedDataWorkers, required=$requiredDataWorkers, " +
        "usedOnDemand=${reservation.usedOnDemandCapacity}"
    }

    return CapacityCheckResult(
      hasAvailableCapacity = true,
      currentDataWorkers = currentDataWorkers,
      committedDataWorkers = committedDataWorkers,
      requiredDataWorkers = requiredDataWorkers,
      usedOnDemandCapacity = reservation.usedOnDemandCapacity,
      wasPersisted = true,
    )
  }

  private fun lockOrganization(organizationId: OrganizationId) {
    val lockedOrganization = organizationRepository.findByIdForUpdate(organizationId.value)
    check(lockedOrganization.isPresent) { "Organization ${organizationId.value} not found for capacity reservation" }
  }

  /**
   * Whether this organization reads its capacity from the allocation table.
   *
   * Returns false on any error, so capacity falls back to the Stigg entitlement.
   */
  private fun usesAllocatedCapacity(organizationId: OrganizationId): Boolean =
    try {
      featureFlagClient.boolVariation(EnableDataWorkerAllocation, Organization(organizationId.value))
    } catch (e: Exception) {
      logger.error(e) { "Error reading the Data Worker allocation flag for organization ${organizationId.value}, using entitlements" }
      false
    }

  /**
   * Get the number of data workers currently in use, in one region or across the organization.
   *
   * Live concurrency enforcement is based on active job reservations rather than hourly usage
   * buckets. Reservations remain present for the lifetime of a running job, so long-running jobs
   * continue to count against committed capacity even after an hour has elapsed.
   */
  private fun getCurrentDataWorkersInUse(
    organizationId: OrganizationId,
    regionId: DataplaneGroupId?,
  ): Double {
    try {
      val totalCpuUsage =
        if (regionId == null) {
          dataWorkerUsageReservationRepository.sumReservedCpuForActiveJobsByOrganizationId(organizationId.value)
        } else {
          dataWorkerUsageReservationRepository.sumReservedCpuForActiveJobsByOrganizationIdAndDataplaneGroupId(
            organizationId.value,
            regionId.value,
          )
        }
      return totalCpuUsage / DATA_WORKER_CPU_DIVISOR
    } catch (e: Exception) {
      logger.error(e) { "Error getting current data workers for organization ${organizationId.value}" }
      // Return 0 on error to avoid blocking jobs
      return 0.0
    }
  }

  /**
   * Get the organization's committed data workers across all regions, or null if it has no committed capacity.
   *
   * This is the organization-wide total.
   */
  fun getCommittedDataWorkersOrNull(organizationId: OrganizationId): Double? =
    allocationsOrNull(organizationId)?.totalAllocatedCapacity ?: entitledCapacityOrNull(organizationId)

  /**
   * The organization's allocated capacity per region.
   *
   * Null means capacity comes from the Stigg entitlement: the flag is off, the read failed, or the
   * organization has no rows because the Stigg backfill has not reached it.
   */
  private fun allocationsOrNull(organizationId: OrganizationId): OrganizationDataWorkerAllocations? {
    if (!usesAllocatedCapacity(organizationId)) {
      return null
    }

    return try {
      val allocations = dataWorkerAllocatedCapacityService.getAllocations(organizationId)
      if (allocations.allocations.isEmpty()) {
        logger.warn { "Organization ${organizationId.value} has no allocated Data Worker capacity, using entitlements" }
        null
      } else {
        allocations
      }
    } catch (e: Exception) {
      logger.error(e) { "Error reading allocated capacity for organization ${organizationId.value}, using entitlements" }
      null
    }
  }

  /**
   * The capacity to admit a job against.
   *
   * A known region is capped at what it holds, which is zero when the organization has no
   * allocation there. Only a region we cannot identify falls back to the organization-wide total.
   */
  private fun committedDataWorkers(
    organizationId: OrganizationId,
    regionId: DataplaneGroupId?,
    allocations: OrganizationDataWorkerAllocations?,
  ): Double {
    if (allocations == null) {
      return entitledCapacityOrNull(organizationId) ?: DEFAULT_COMMITTED_DATA_WORKERS
    }
    if (regionId == null) {
      return allocations.totalAllocatedCapacity
    }

    val allocatedCapacity = allocations.allocations.firstOrNull { it.dataplaneGroupId == regionId }?.allocatedCapacity ?: 0.0
    if (allocatedCapacity <= 0.0) {
      logger.warn {
        "Organization ${organizationId.value} has no Data Worker capacity allocated in region ${regionId.value}. "
      }
    }
    return allocatedCapacity
  }

  /**
   * The organization's committed data workers from Stigg, or null if unlimited, missing, or on error.
   */
  private fun entitledCapacityOrNull(organizationId: OrganizationId): Double? {
    try {
      val result = entitlementService.getNumericEntitlement(organizationId, CommittedDataWorkersEntitlement)
      val value = result.value

      return if (result.hasAccess && value != null && !result.isUnlimited) {
        value.toDouble().coerceAtLeast(MIN_COMMITTED_DATA_WORKERS)
      } else {
        null
      }
    } catch (e: Exception) {
      logger.error(e) { "Error getting committed data workers for organization ${organizationId.value}" }
      return null
    }
  }

  companion object {
    private const val DATA_WORKER_CPU_DIVISOR = 8.0

    // Default committed data workers when entitlement is present but value not specified
    private const val DEFAULT_COMMITTED_DATA_WORKERS = 1.0

    // Minimum data workers for orgs without the entitlement
    private const val MIN_COMMITTED_DATA_WORKERS = 1.0
  }
}
