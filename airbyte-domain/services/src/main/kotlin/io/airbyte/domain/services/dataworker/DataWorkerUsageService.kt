/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.dataworker

import io.airbyte.config.Job
import io.airbyte.data.repositories.entities.DataWorkerUsage
import io.airbyte.data.services.DataWorkerUsageDataService
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.dataworker.DataWorkerUsageWithTime
import io.airbyte.domain.models.dataworker.DataplaneGroupDataWorkerUsage
import io.airbyte.domain.models.dataworker.OrganizationDataWorkerUsage
import io.airbyte.domain.models.dataworker.WorkspaceDataWorkerUsage
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
) {
  fun insertUsageWhenJobStarts(job: Job) {
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
        jobId = job.id,
        organizationId = organizationId,
        workspaceId = workspaceId,
        dataplaneGroupId = dataplaneGroupId,
        sourceCpuRequest = sourceCpu.toDouble(),
        destinationCpuRequest = destCpu.toDouble(),
        orchestratorCpuRequest = orchCpu.toDouble(),
        jobStart = jobStart,
        jobEnd = null,
        createdAt = OffsetDateTime.now(),
      ),
    )
  }

  fun updateUsageWhenJobFinishes(job: Job) {
    val jobEnd = OffsetDateTime.ofInstant(Instant.ofEpochSecond(job.updatedAtInSecond), ZoneOffset.UTC)

    dataWorkerUsageDataService.updateDataWorkerUsage(
      jobId = job.id,
      jobEnd = jobEnd,
    )
  }

  fun getDataWorkerUsage(
    organizationId: UUID,
    startDate: LocalDate,
    endDate: LocalDate,
  ): OrganizationDataWorkerUsage {
    TODO("not implemented")
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
}
