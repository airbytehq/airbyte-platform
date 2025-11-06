/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.dataworker

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.repositories.entities.DataWorkerUsage
import io.airbyte.data.services.DataWorkerUsageDataService
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.DataplaneGroupId
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.domain.models.dataworker.DataWorkerUsageWithTime
import io.airbyte.domain.models.dataworker.DataplaneGroupDataWorkerUsage
import io.airbyte.domain.models.dataworker.OrganizationDataWorkerUsage
import io.airbyte.domain.models.dataworker.WorkspaceDataWorkerUsage
import io.airbyte.featureflag.EnableDataWorkerUsage
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.UUID

private val logger = KotlinLogging.logger {}

@Singleton
open class DataWorkerUsageService(
  private val organizationService: OrganizationService,
  private val dataplaneGroupService: DataplaneGroupService,
  private val dataWorkerUsageDataService: DataWorkerUsageDataService,
  private val workspaceService: WorkspaceService,
  private val featureFlagClient: FeatureFlagClient,
  private val entitlementService: EntitlementService,
  private val metricClient: MetricClient,
) {
  /**
   * Records data worker usage when a job is created by tracking CPU resource requirements.
   *
   * This function implements an hourly bucketing system for tracking cumulative resource usage:
   * - If no recent bucket exists or the most recent bucket is more than 1 hour old, a new hourly
   *   bucket is created with the current job's CPU values added to the previous bucket's values
   * - If a bucket exists within the current hour, the existing bucket is updated by adding the
   *   new job's CPU requirements and recalculating the maximum values
   *
   * The function validates that the job meets the following criteria before recording usage:
   * 1. The job must be a sync job
   * 2. The data worker usage feature flag must be enabled for the organization
   * 3. The organization must be on a Pro, Flex, or SME plan
   *
   * CPU resources tracked include:
   * - Source connector CPU request
   * - Destination connector CPU request
   * - Orchestrator CPU request
   *
   * @param job The job for which to record data worker usage. Must be a sync job with valid
   *            resource requirements and an associated workspace/organization.
   */
  fun insertUsageForCreatedJob(job: Job) {
    // We return early here if we cannot build a dataWorkerUsage object.
    // Errors, logging, and metrics are handled inside the buildDataWorkerUsageOrNull method.
    val dataWorkerUsage = buildDataWorkerUsageOrNull(job, INCREMENT_OPERATION) ?: return

    // In this function, we check the following:
    // 1. is the job a sync
    // 2. is the organization entitled (ie. is this an enterprise organization)
    // 3. is the feature flag enabled for this organization
    // if any of these are not true, we do not record usage.
    if (!isJobValidForInsertion(job, dataWorkerUsage.organizationId)) {
      logger.debug { "Job ${job.id} for org ${dataWorkerUsage.organizationId} is not valid for data worker usage insertion." }
      return
    }

    try {
      performUsageInsertion(dataWorkerUsage)
    } catch (e: Exception) {
      logger.error(e) { "${e.message}: failed to insert data worker usage: $dataWorkerUsage" }
      sendRecordMetric(
        job.id,
        false,
        INCREMENT_OPERATION,
        dataWorkerUsage.organizationId,
        dataWorkerUsage.workspaceId,
        dataWorkerUsage.dataplaneGroupId,
      )
      return
    }

    sendRecordMetric(
      job.id,
      true,
      INCREMENT_OPERATION,
      dataWorkerUsage.organizationId,
      dataWorkerUsage.workspaceId,
      dataWorkerUsage.dataplaneGroupId,
    )
  }

  @Transactional("config")
  open fun performUsageInsertion(dataWorkerUsage: DataWorkerUsage) {
    val mostRecentUsageBucket =
      dataWorkerUsageDataService.findMostRecentUsageBucket(
        dataWorkerUsage.organizationId,
        dataWorkerUsage.workspaceId,
        dataWorkerUsage.dataplaneGroupId,
        dataWorkerUsage.bucketStart,
      )

    if (mostRecentUsageBucket == null || dataWorkerUsage.bucketStart.isAfter(mostRecentUsageBucket.bucketStart.plusHours(1))) {
      // If there is no most recent usage, or recent usage is older than one hour,
      // we must create a new bucket, carrying over previous usage values.
      // We calculate a new max, which is the previous bucket's cpu values + the new job's cpu values
      val newSourceCpuRequest = (mostRecentUsageBucket?.sourceCpuRequest ?: 0.0) + dataWorkerUsage.sourceCpuRequest
      val newDestCpuRequest = (mostRecentUsageBucket?.destinationCpuRequest ?: 0.0) + dataWorkerUsage.destinationCpuRequest
      val newOrchCpuRequest = (mostRecentUsageBucket?.orchestratorCpuRequest ?: 0.0) + dataWorkerUsage.orchestratorCpuRequest

      val usageWithNewCpuValues =
        dataWorkerUsage.copy(
          sourceCpuRequest = newSourceCpuRequest,
          destinationCpuRequest = newDestCpuRequest,
          orchestratorCpuRequest = newOrchCpuRequest,
          maxSourceCpuRequest = newSourceCpuRequest,
          maxDestinationCpuRequest = newDestCpuRequest,
          maxOrchestratorCpuRequest = newOrchCpuRequest,
        )
      logger.info { "Data worker usage addition detected a new bucket is required. Creating a new bucket with values: $usageWithNewCpuValues" }

      dataWorkerUsageDataService.insertNewDataWorkerUsageBucket(usageWithNewCpuValues)
    } else {
      // Otherwise we can update the existing bucket. Incrementing the current usage values
      // and calculating a new maximum will be handled by the SQL query in the repository,
      // so we just pass in the dataWorkerUsage object as is.
      dataWorkerUsageDataService.incrementExistingDataWorkerUsageBucket(dataWorkerUsage)
    }
  }

  /**
   * Decrements current data worker usage when a job completes by subtracting CPU resource requirements.
   *
   * This function decrements the tracked resource usage in the hourly bucketing system:
   * - If no recent bucket exists, the function logs an error and returns without modification
   *   to prevent negative values (indicates missing data or a deleted bucket)
   * - If the most recent bucket is more than 1 hour old, a new hourly bucket is created with
   *   the completed job's CPU values subtracted from the previous bucket's values
   * - If a bucket exists within the current hour, the existing bucket is updated by subtracting
   *   the completed job's CPU requirements
   *
   * This function works in tandem with [insertUsageForCreatedJob] to maintain accurate tracking
   * of cumulative resource usage over time. When a job completes, its resources are released and
   * should no longer count toward the organization's usage totals.
   *
   * CPU resources subtracted include:
   * - Source connector CPU request
   * - Destination connector CPU request
   * - Orchestrator CPU request
   *
   * @param job The completed job for which to remove data worker usage. The job's resource
   *            requirements will be subtracted from the current usage bucket.
   */
  fun subtractUsageForCompletedJob(job: Job) {
    val dataWorkerUsage = buildDataWorkerUsageOrNull(job, DECREMENT_OPERATION) ?: return

    try {
      performUsageSubtraction(dataWorkerUsage, job.id)
    } catch (e: Exception) {
      logger.error(e) { "${e.message}: failed to subtract $dataWorkerUsage" }
      sendRecordMetric(
        job.id,
        false,
        DECREMENT_OPERATION,
        dataWorkerUsage.organizationId,
        dataWorkerUsage.workspaceId,
        dataWorkerUsage.dataplaneGroupId,
      )
      return
    }

    sendRecordMetric(
      job.id,
      true,
      DECREMENT_OPERATION,
      dataWorkerUsage.organizationId,
      dataWorkerUsage.workspaceId,
      dataWorkerUsage.dataplaneGroupId,
    )
  }

  @Transactional("config")
  open fun performUsageSubtraction(
    dataWorkerUsage: DataWorkerUsage,
    jobId: Long,
  ) {
    val mostRecentUsageBucket =
      dataWorkerUsageDataService.findMostRecentUsageBucket(
        dataWorkerUsage.organizationId,
        dataWorkerUsage.workspaceId,
        dataWorkerUsage.dataplaneGroupId,
        dataWorkerUsage.bucketStart,
      )

    if (mostRecentUsageBucket == null) {
      // Edge case: if no recent bucket exists AND we have a job end event, it implies
      // that data is either missing or we've deleted a previous bucket.
      // In this case, we should not subtract the usage values, because we could
      // end up with negative values, which could be carried over to subsequent hours
      logger.error { "Found a completed job $jobId with no prior usage recorded, skipping data worker usage for $dataWorkerUsage" }
      sendRecordMetric(
        jobId,
        false,
        DECREMENT_OPERATION,
        dataWorkerUsage.organizationId,
        dataWorkerUsage.workspaceId,
        dataWorkerUsage.dataplaneGroupId,
      )
      return
    } else if (dataWorkerUsage.bucketStart.isAfter(mostRecentUsageBucket.bucketStart.plusHours(1))) {
      // If the most recent usage is older than one hour,
      // we must create a new bucket, carrying over previous usage values.
      // We calculate a new max, which is the previous buckets cpu values - the new jobs cpu values
      // We coerce the values to be at least 0.0 to prevent negative values
      val newSourceCpuRequest = (mostRecentUsageBucket.sourceCpuRequest - dataWorkerUsage.sourceCpuRequest).coerceAtLeast(0.0)
      val newDestCpuRequest = (mostRecentUsageBucket.destinationCpuRequest - dataWorkerUsage.destinationCpuRequest).coerceAtLeast(0.0)
      val newOrchCpuRequest = (mostRecentUsageBucket.orchestratorCpuRequest - dataWorkerUsage.orchestratorCpuRequest).coerceAtLeast(0.0)

      val usageWithNewCpuValues =
        dataWorkerUsage.copy(
          sourceCpuRequest = newSourceCpuRequest,
          destinationCpuRequest = newDestCpuRequest,
          orchestratorCpuRequest = newOrchCpuRequest,
          maxSourceCpuRequest = newSourceCpuRequest,
          maxDestinationCpuRequest = newDestCpuRequest,
          maxOrchestratorCpuRequest = newOrchCpuRequest,
        )
      logger.info { "Data worker usage subtraction detected a new bucket is required. Creating a new bucket with values: $usageWithNewCpuValues" }

      dataWorkerUsageDataService.insertNewDataWorkerUsageBucket(usageWithNewCpuValues)
    } else {
      // In this case, we only need to decrement existing values, which is handled
      // by the SQL query in the repository. We can pass in the dataWorkerUsage object as is.
      dataWorkerUsageDataService.decrementExistingDataWorkerUsageBucket(dataWorkerUsage)
    }
  }

  /**
   * Retrieves aggregated data worker usage for an organization over a specified date range.
   *
   * This function queries and aggregates usage data with the following structure:
   * - Organization level: Contains all dataplane groups
   * - Dataplane group level: Groups usage by dataplane group, containing all workspaces
   * - Workspace level: Groups usage by workspace, containing hourly usage data
   * - Hourly level: Individual hourly buckets with CPU usage and calculated data worker counts
   *
   * The function performs several transformations on the raw usage data:
   * 1. Retrieves all usage records for the organization within the date range
   * 2. Groups records by dataplane group ID and workspace ID
   * 3. Aggregates hourly records by summing CPU requests across source, destination, and orchestrator
   * 4. Calculates data worker counts based on total CPU usage (divided by 8)
   * 5. Fills gaps in hourly data to maintain continuity when usage > 0
   * 6. Filters out any dataplane groups or workspaces that no longer exist
   *
   * Time range handling:
   * - Start date is converted to 00:00:00 UTC
   * - End date is converted to 23:59:59 UTC
   *
   * @param organizationId The UUID of the organization to retrieve usage for
   * @param startDate The start date (inclusive) of the usage period
   * @param endDate The end date (inclusive) of the usage period
   * @return An [OrganizationDataWorkerUsage] object containing hierarchical usage data grouped
   *         by dataplane groups and workspaces, with hourly usage details
   */
  fun getDataWorkerUsage(
    organizationId: UUID,
    startDate: LocalDate,
    endDate: LocalDate,
  ): OrganizationDataWorkerUsage {
    val startDateTime = startDate.atStartOfDay(ZoneOffset.UTC).toOffsetDateTime()
    val endDateTime = endDate.atTime(23, 59, 59).atZone(ZoneOffset.UTC).toOffsetDateTime()

    val usageRecords =
      dataWorkerUsageDataService.getDataWorkerUsageByOrganizationAndTimeRange(
        organizationId,
        startDateTime,
        endDateTime,
      )

    val dataplaneGroups =
      usageRecords
        .groupBy { it.dataplaneGroupId }
        .mapNotNull { (dataplaneGroupId, entries) ->
          val dataplaneGroup = retrieveDataplaneGroupOrNull(dataplaneGroupId) ?: return@mapNotNull null

          DataplaneGroupDataWorkerUsage(
            dataplaneGroupId = DataplaneGroupId(dataplaneGroupId),
            dataplaneGroupName = dataplaneGroup.name,
            workspaces =
              entries
                .groupBy { it.workspaceId }
                .mapNotNull { (workspaceId, records) ->
                  val workspace = retrieveWorkspaceOrNull(workspaceId) ?: return@mapNotNull null
                  val hourlyUsage =
                    records
                      .groupBy { it.bucketStart }
                      .map { (hour, hourRecords) ->
                        DataWorkerUsageWithTime(
                          usageStartTime = hour,
                          currentUsage = hourRecords.sumOf { it.sourceCpuRequest + it.destinationCpuRequest + it.orchestratorCpuRequest },
                          dataWorkers = hourRecords.sumOf { it.calculateDataWorkers() },
                        )
                      }.sortedBy { it.usageStartTime }

                  // Fill gaps in hourly usage. Gaps can exist when a single job runs over multiple hours
                  // in which no other jobs are started or finished. Because we create hourly buckets
                  // based on when jobs start and end, in this case the buckets will not be present in
                  // the db. We fill them in here at query time, so the data returned is consistent.
                  val filledHourlyUsage = fillGapsInHourlyUsage(hourlyUsage)

                  WorkspaceDataWorkerUsage(
                    workspaceId = WorkspaceId(workspaceId),
                    workspaceName = workspace.name,
                    dataWorkers = filledHourlyUsage,
                  )
                },
          )
        }

    return OrganizationDataWorkerUsage(
      organizationId = OrganizationId(organizationId),
      dataplaneGroups = dataplaneGroups,
    )
  }

  private fun buildDataWorkerUsageOrNull(
    job: Job,
    operation: String,
  ): DataWorkerUsage? {
    val workspaceId = job.config.sync?.workspaceId
    if (workspaceId == null) {
      logger.error { "Workspace ID is null for job ${job.id}, skipping data worker usage insertion." }
      sendRecordMetric(job.id, false, operation)
      return null
    }

    val organization = organizationService.getOrganizationForWorkspaceId(workspaceId).orElse(null)
    if (organization == null) {
      logger.error { "Organization not found for workspace $workspaceId (job ${job.id}), skipping data worker usage insertion." }
      sendRecordMetric(job.id, false, operation, workspaceId = workspaceId)
      return null
    }

    val organizationId = organization.organizationId
    val workspace = retrieveWorkspaceOrNull(workspaceId) ?: return null
    val dataplaneGroupId =
      workspace.dataplaneGroupId ?: run {
        logger.error {
          "Dataplane group is null for workspace $workspaceId and org id $organizationId (job ${job.id}), skipping data worker usage insertion."
        }
        sendRecordMetric(job.id, false, operation, organizationId, workspaceId)
        return null
      }

    val sourceCpu = job.config.sync.syncResourceRequirements.source.cpuRequest
    val destCpu = job.config.sync.syncResourceRequirements.destination.cpuRequest
    val orchCpu = job.config.sync.syncResourceRequirements.orchestrator.cpuRequest
    if (!areJobResourceRequirementsValid(sourceCpu, destCpu, orchCpu)) {
      logger.error {
        "Found invalid resource requirements in job configuration: ${job.config.sync.syncResourceRequirements}, skipping data worker usage insertion."
      }
      sendRecordMetric(job.id, false, operation, organizationId, workspaceId, dataplaneGroupId)
      return null
    }

    // We use the current timestamp of insertion as the job start and end time. This avoids any edge
    // cases around delayed job ends or mismatched job start/end timestamps that could pollute stored data.
    return DataWorkerUsage(
      organizationId = organizationId,
      workspaceId = workspaceId,
      dataplaneGroupId = dataplaneGroupId,
      sourceCpuRequest = sourceCpu.toDouble(),
      destinationCpuRequest = destCpu.toDouble(),
      orchestratorCpuRequest = orchCpu.toDouble(),
      bucketStart = OffsetDateTime.now(ZoneOffset.UTC),
      createdAt = OffsetDateTime.now(ZoneOffset.UTC),
      maxSourceCpuRequest = sourceCpu.toDouble(),
      maxDestinationCpuRequest = destCpu.toDouble(),
      maxOrchestratorCpuRequest = orchCpu.toDouble(),
    )
  }

  private fun isJobValidForInsertion(
    job: Job,
    organizationId: UUID,
  ): Boolean {
    val isSync = job.configType == JobConfig.ConfigType.SYNC
    val flagEnabled = featureFlagClient.boolVariation(EnableDataWorkerUsage, Organization(organizationId))

    val planId = entitlementService.getCurrentPlanId(OrganizationId(organizationId))
    val isEntitled = VALID_PLANS.contains(planId)

    return isSync && flagEnabled && isEntitled
  }

  private fun areJobResourceRequirementsValid(
    sourceCpu: String,
    destCpu: String,
    orchCpu: String,
  ): Boolean =
    try {
      sourceCpu.toDouble()
      destCpu.toDouble()
      orchCpu.toDouble()
      true
    } catch (_: Exception) {
      false
    }

  private fun retrieveDataplaneGroupOrNull(dataplaneGroupId: UUID): DataplaneGroup? {
    try {
      return dataplaneGroupService.getDataplaneGroup(dataplaneGroupId)
    } catch (e: Exception) {
      logger.error(e) { "Failed to fetch dataplane group $dataplaneGroupId, skipping usage retrieval" }
      return null
    }
  }

  private fun retrieveWorkspaceOrNull(workspaceId: UUID): StandardWorkspace? {
    try {
      return workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false)
    } catch (e: Exception) {
      logger.error(e) { "Failed to fetch workspace $workspaceId, skipping usage retrieval" }
      return null
    }
  }

  /**
   * Fills gaps in hourly usage data to ensure continuity when jobs run across multiple hours.
   *
   * This function addresses a gap in the bucketing system: when a single job runs for multiple
   * hours without any other jobs starting or finishing, no usage buckets are created for the
   * intermediate hours. This is because buckets are only created when jobs start or complete.
   *
   * For example, if a job starts at 10:00 and completes at 13:00 with no other job events:
   * - A bucket exists for 10:00 (job start)
   * - A bucket exists for 13:00 (job complete)
   * - Buckets for 11:00 and 12:00 are missing from the database
   *
   * This function fills these missing hours at query time by:
   * 1. Identifying gaps larger than 1 hour between consecutive usage records
   * 2. Only filling gaps when the current usage is greater than 0 (indicating active jobs)
   * 3. Creating synthetic hourly records with the same usage values as the preceding hour
   * 4. Maintaining chronological order of all records
   *
   * This ensures that usage data remains consistent and complete for reporting purposes, showing
   * continuous resource usage for long-running jobs.
   *
   * @param hourlyUsage A list of hourly usage records sorted by date, potentially with gaps
   * @return A new list with all gaps filled, or the original list if empty or no gaps exist
   */
  @VisibleForTesting
  fun fillGapsInHourlyUsage(hourlyUsage: List<DataWorkerUsageWithTime>): List<DataWorkerUsageWithTime> {
    if (hourlyUsage.isEmpty()) {
      return hourlyUsage
    }

    val result = mutableListOf<DataWorkerUsageWithTime>()

    for (i in hourlyUsage.indices) {
      val current = hourlyUsage[i]
      result.add(current)

      if (i < hourlyUsage.size - 1) {
        val next = hourlyUsage[i + 1]
        val currentHour = current.usageStartTime.truncatedTo(ChronoUnit.HOURS)
        val nextHour = next.usageStartTime.truncatedTo(ChronoUnit.HOURS)

        val hoursDifference = ChronoUnit.HOURS.between(currentHour, nextHour)
        if (hoursDifference > 1 && current.currentUsage > 0.0) {
          var gapHour = currentHour.plusHours(1)
          while (gapHour.isBefore(nextHour)) {
            result.add(
              DataWorkerUsageWithTime(
                usageStartTime = gapHour,
                currentUsage = current.currentUsage,
                dataWorkers = current.dataWorkers,
              ),
            )
            gapHour = gapHour.plusHours(1)
          }
        }
      }
    }

    return result
  }

  /**
   * We sometimes call this function before org/workspace/dataplaneGroup ID's
   * are available, for example, when retrieving any of those values fails.
   * Therefore, those parameters are optional.
   */
  private fun sendRecordMetric(
    jobId: Long,
    wasSuccess: Boolean,
    operation: String,
    organizationId: UUID? = null,
    workspaceId: UUID? = null,
    dataplaneGroupId: UUID? = null,
  ) {
    val attributes =
      listOfNotNull(
        MetricAttribute(MetricTags.JOB_ID, jobId.toString()),
        MetricAttribute(MetricTags.SUCCESS, wasSuccess.toString()),
        MetricAttribute(MetricTags.DATA_WORKER_USAGE_OPERATION, operation),
        organizationId?.let { MetricAttribute(MetricTags.ORGANIZATION_ID, it.toString()) },
        workspaceId?.let { MetricAttribute(MetricTags.WORKSPACE_ID, it.toString()) },
        dataplaneGroupId?.let { MetricAttribute(MetricTags.DATA_PLANE_GROUP_TAG, it.toString()) },
      )

    metricClient.count(
      OssMetricsRegistry.DATA_WORKER_USAGE_RECORDED,
      attributes = attributes.toTypedArray(),
    )
  }

  companion object {
    val VALID_PLANS = setOf(EntitlementPlan.PRO.id, EntitlementPlan.FLEX.id, EntitlementPlan.SME.id)
    const val INCREMENT_OPERATION = "INCREMENT"
    const val DECREMENT_OPERATION = "DECREMENT"
  }
}
