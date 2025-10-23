/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.dataworker

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
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.time.Instant
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

private val logger = KotlinLogging.logger {}

@Singleton
class DataWorkerUsageService(
  private val organizationService: OrganizationService,
  private val dataplaneGroupService: DataplaneGroupService,
  private val dataWorkerUsageDataService: DataWorkerUsageDataService,
  private val workspaceService: WorkspaceService,
  private val featureFlagClient: FeatureFlagClient,
  private val entitlementService: EntitlementService,
) {
  fun insertUsageForCompletedJob(job: Job) {
    val workspaceId = job.config.sync?.workspaceId
    if (workspaceId == null) {
      // TODO: metrics and alerting
      logger.error { "Workspace ID is null for job ${job.id}, skipping data worker usage insertion." }
      return
    }

    val organization = organizationService.getOrganizationForWorkspaceId(workspaceId).orElse(null)
    if (organization == null) {
      // TODO: metrics and alerting
      logger.error { "Organization not found for workspace $workspaceId (job ${job.id}), skipping data worker usage insertion." }
      return
    }

    // before proceeding with writing the usage, we must check the following conditions:
    // 1. is the feature flag enabled for data worker usage
    // 2. is the org a pro/flex/sme customer
    // 3. is the job a sync
    if (!isJobValidForInsertion(job, organization.organizationId)) {
      logger.debug { "Job ${job.id} for org ${organization.organizationId} is not valid for data worker usage insertion." }
      return
    }

    // job start time may not be available here, depending on where in the job lifecycle
    // this function is called from. if we don't have job start, we can fall back to the
    // job creation time (which always exists here) and safely use that for our usage timestamp.
    val jobStartForUsage = job.startedAtInSecond ?: job.createdAtInSecond
    val jobStart = OffsetDateTime.ofInstant(Instant.ofEpochSecond(jobStartForUsage), ZoneOffset.UTC)

    val organizationId = organization.organizationId
    val workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false)
    val dataplaneGroupId =
      workspace.dataplaneGroupId ?: run {
        // TODO: metrics and alerting
        logger.error { "Dataplane group is null for workspace $workspaceId (job ${job.id}), skipping data worker usage insertion." }
        return
      }

    val sourceCpu = job.config.sync.syncResourceRequirements.source.cpuRequest
    val destCpu = job.config.sync.syncResourceRequirements.destination.cpuRequest
    val orchCpu = job.config.sync.syncResourceRequirements.orchestrator.cpuRequest
    if (!areJobResourceRequirementsValid(sourceCpu, destCpu, orchCpu)) {
      // TODO: metrics and alerting
      logger.error {
        "Found invalid resource requirements in job configuration: ${job.config.sync.syncResourceRequirements}, skipping data worker usage insertion."
      }
      return
    }

    // the toDouble() calls below are safe, given that areJobResourceRequirementsValid has been called above.
    dataWorkerUsageDataService.insertDataWorkerUsage(
      DataWorkerUsage(
        organizationId = organizationId,
        workspaceId = workspaceId,
        dataplaneGroupId = dataplaneGroupId,
        sourceCpuRequest = sourceCpu.toDouble(),
        destinationCpuRequest = destCpu.toDouble(),
        orchestratorCpuRequest = orchCpu.toDouble(),
        bucketStart = jobStart,
        createdAt = OffsetDateTime.now(),
      ),
    )
  }

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
                          date = hour,
                          usage = hourRecords.sumOf { it.calculateDataWorkers() },
                        )
                      }.sortedBy { it.date }

                  WorkspaceDataWorkerUsage(
                    workspaceId = WorkspaceId(workspaceId),
                    workspaceName = workspace.name,
                    dataWorkers = hourlyUsage,
                  )
                },
          )
        }

    return OrganizationDataWorkerUsage(
      organizationId = OrganizationId(organizationId),
      dataplaneGroups = dataplaneGroups,
    )
  }

  private fun isJobValidForInsertion(
    job: Job,
    organizationId: UUID,
  ): Boolean {
    val isSync = job.configType == JobConfig.ConfigType.SYNC
    val flagEnabled = featureFlagClient.boolVariation(EnableDataWorkerUsage, io.airbyte.featureflag.Organization(organizationId))

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

  companion object {
    val VALID_PLANS = setOf(EntitlementPlan.PRO.id, EntitlementPlan.FLEX.id, EntitlementPlan.SME.id)
  }
}
