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
import io.airbyte.domain.models.OrganizationId
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
   * Number of committed data workers for the organization.
   */
  val committedDataWorkers: Int,
  /**
   * Number of data workers required by the job being evaluated.
   */
  val requiredDataWorkers: Double,
  /**
   * Whether the job should use on-demand capacity if it proceeds.
   */
  val usedOnDemandCapacity: Boolean,
)

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
  @param:Named("config") private val configTransactionOperations: TransactionOperations<Connection>,
) {
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
    val committedDataWorkers = getCommittedDataWorkers(organizationId)
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
            )
          }

          val currentDataWorkers = getCurrentDataWorkersInUse(organizationId)

          val hasCommittedCapacity = currentDataWorkers + requiredDataWorkers <= committedDataWorkers
          val usedOnDemandCapacity = !hasCommittedCapacity && allowOnDemandCapacity
          val hasAvailableCapacity = hasCommittedCapacity || usedOnDemandCapacity

          logger.debug {
            "Capacity check for org ${organizationId.value}: " +
              "current=$currentDataWorkers, required=$requiredDataWorkers, committed=$committedDataWorkers, " +
              "available=$hasAvailableCapacity, usedOnDemand=$usedOnDemandCapacity"
          }

          if (hasAvailableCapacity) {
            val preparedUsage = requirePreparedUsage(job, organizationId)
            reservedUsage = preparedUsage
            dataWorkerUsageService.persistReservedUsageForJob(job.id, preparedUsage, usedOnDemandCapacity)
          }

          CapacityCheckResult(
            hasAvailableCapacity = hasAvailableCapacity,
            currentDataWorkers = currentDataWorkers,
            committedDataWorkers = committedDataWorkers,
            requiredDataWorkers = requiredDataWorkers,
            usedOnDemandCapacity = usedOnDemandCapacity,
          )
        }
      } catch (e: Exception) {
        reservedUsage?.let {
          dataWorkerUsageService.recordUsageMetric(job.id, false, DataWorkerUsageService.INCREMENT_OPERATION, it)
        }
        throw e
      }

    if (result.hasAvailableCapacity) {
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
    committedDataWorkers: Int,
    requiredDataWorkers: Double,
  ): CapacityCheckResult {
    val currentDataWorkers = getCurrentDataWorkersInUse(organizationId)

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
    )
  }

  private fun lockOrganization(organizationId: OrganizationId) {
    val lockedOrganization = organizationRepository.findByIdForUpdate(organizationId.value)
    check(lockedOrganization.isPresent) { "Organization ${organizationId.value} not found for capacity reservation" }
  }

  /**
   * Get the current number of data workers in use by an organization.
   *
   * Live concurrency enforcement is based on active job reservations rather than hourly usage
   * buckets. Reservations remain present for the lifetime of a running job, so long-running jobs
   * continue to count against committed capacity even after an hour has elapsed.
   */
  private fun getCurrentDataWorkersInUse(organizationId: OrganizationId): Double {
    try {
      val totalCpuUsage = dataWorkerUsageReservationRepository.sumReservedCpuForActiveJobsByOrganizationId(organizationId.value)
      return totalCpuUsage / DATA_WORKER_CPU_DIVISOR
    } catch (e: Exception) {
      logger.error(e) { "Error getting current data workers for organization ${organizationId.value}" }
      // Return 0 on error to avoid blocking jobs
      return 0.0
    }
  }

  /**
   * Get the number of committed data workers for an organization from entitlements.
   *
   * Returns the numeric value from the CommittedDataWorkersEntitlement.
   * Returns DEFAULT_COMMITTED_DATA_WORKERS if the entitlement is not found or on error.
   */
  private fun getCommittedDataWorkers(organizationId: OrganizationId): Int {
    try {
      val result = entitlementService.getNumericEntitlement(organizationId, CommittedDataWorkersEntitlement)
      val value = result.value

      return if (result.hasAccess && value != null) {
        // Use the numeric value from Stigg entitlement
        value.toInt().coerceAtLeast(MIN_COMMITTED_DATA_WORKERS)
      } else {
        // If not entitled to committed data workers or no value set, they get the minimum
        logger.debug { "No committed data workers value found for org ${organizationId.value}, using minimum" }
        MIN_COMMITTED_DATA_WORKERS
      }
    } catch (e: Exception) {
      logger.error(e) { "Error getting committed data workers for organization ${organizationId.value}" }
      // Return default on error to avoid blocking jobs
      return DEFAULT_COMMITTED_DATA_WORKERS
    }
  }

  companion object {
    private const val DATA_WORKER_CPU_DIVISOR = 8.0

    // Default committed data workers when entitlement is present but value not specified
    private const val DEFAULT_COMMITTED_DATA_WORKERS = 1

    // Minimum data workers for orgs without the entitlement
    private const val MIN_COMMITTED_DATA_WORKERS = 1
  }
}
