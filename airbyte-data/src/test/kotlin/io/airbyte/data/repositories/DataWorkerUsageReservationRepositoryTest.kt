/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.commons.json.Jsons
import io.airbyte.data.repositories.entities.DataWorkerUsageReservation
import io.airbyte.data.repositories.entities.Job
import io.airbyte.data.repositories.entities.NON_TERMINAL_STATUSES
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import io.kotest.matchers.doubles.plusOrMinus
import io.kotest.matchers.shouldBe
import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

@MicronautTest(environments = [Environment.TEST])
internal class DataWorkerUsageReservationRepositoryTest : AbstractConfigRepositoryTest() {
  private val config = Jsons.jsonNode(mapOf<String, String>())
  private var nextCreatedAt = OffsetDateTime.of(2021, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)

  @AfterEach
  fun cleanup() {
    dataWorkerUsageReservationRepository.deleteAll()
    jobsRepository.deleteAll()
  }

  @Test
  fun `sumReservedCpuForActiveJobsByOrganizationId counts only active job reservations`() {
    val targetOrganizationId = UUID.randomUUID()
    val otherOrganizationId = UUID.randomUUID()

    jobsRepository.saveAll(
      listOf(
        createJob(1L, JobStatus.pending),
        createJob(2L, JobStatus.queued),
        createJob(3L, JobStatus.running),
        createJob(4L, JobStatus.incomplete),
        createJob(5L, JobStatus.succeeded),
        createJob(6L, JobStatus.cancelled),
        createJob(7L, JobStatus.running),
        createJob(8L, JobStatus.failed),
      ),
    )

    dataWorkerUsageReservationRepository.saveAll(
      listOf(
        createReservation(1L, targetOrganizationId, sourceCpu = 2.0, destinationCpu = 1.0, orchestratorCpu = 0.5),
        createReservation(2L, targetOrganizationId, sourceCpu = 1.0, destinationCpu = 0.5, orchestratorCpu = 0.0),
        createReservation(3L, targetOrganizationId, sourceCpu = 3.0, destinationCpu = 1.0, orchestratorCpu = 0.5),
        createReservation(4L, targetOrganizationId, sourceCpu = 4.0, destinationCpu = 0.0, orchestratorCpu = 0.5),
        createReservation(5L, targetOrganizationId, sourceCpu = 20.0, destinationCpu = 20.0, orchestratorCpu = 20.0),
        createReservation(6L, targetOrganizationId, sourceCpu = 30.0, destinationCpu = 30.0, orchestratorCpu = 30.0),
        createReservation(7L, otherOrganizationId, sourceCpu = 40.0, destinationCpu = 40.0, orchestratorCpu = 40.0),
        createReservation(8L, targetOrganizationId, sourceCpu = 50.0, destinationCpu = 50.0, orchestratorCpu = 50.0),
      ),
    )

    val result = dataWorkerUsageReservationRepository.sumReservedCpuForActiveJobsByOrganizationId(targetOrganizationId)

    result.shouldBe((14.0) plusOrMinus 0.0001)
  }

  @Test
  fun `sumReservedCpuForActiveJobsByOrganizationId returns zero when org has no active reservations`() {
    val organizationId = UUID.randomUUID()

    val result = dataWorkerUsageReservationRepository.sumReservedCpuForActiveJobsByOrganizationId(organizationId)

    result.shouldBe(0.0)
  }

  @ParameterizedTest
  @EnumSource(JobStatus::class)
  fun `sumReservedCpuForActiveJobsByOrganizationId covers all job statuses`(status: JobStatus) {
    val organizationId = UUID.randomUUID()
    val jobId = 1L
    val expected =
      if (status in NON_TERMINAL_STATUSES) {
        3.5
      } else {
        0.0
      }

    jobsRepository.save(createJob(jobId, status))
    dataWorkerUsageReservationRepository.save(
      createReservation(jobId, organizationId, sourceCpu = 2.0, destinationCpu = 1.0, orchestratorCpu = 0.5),
    )

    val result = dataWorkerUsageReservationRepository.sumReservedCpuForActiveJobsByOrganizationId(organizationId)

    result.shouldBe(expected plusOrMinus 0.0001)
  }

  private fun createJob(
    id: Long,
    status: JobStatus,
  ) = Job(
    id = id,
    scope = "scope-$id",
    status = status,
    configType = JobConfigType.sync,
    config = config,
    createdAt = nextCreatedAt,
    updatedAt = nextCreatedAt,
    isScheduled = false,
  ).also { nextCreatedAt = nextCreatedAt.plusDays(1) }

  private fun createReservation(
    jobId: Long,
    organizationId: UUID,
    sourceCpu: Double,
    destinationCpu: Double,
    orchestratorCpu: Double,
  ) = DataWorkerUsageReservation(
    jobId = jobId,
    organizationId = organizationId,
    workspaceId = UUID.randomUUID(),
    dataplaneGroupId = UUID.randomUUID(),
    sourceCpuRequest = sourceCpu,
    destinationCpuRequest = destinationCpu,
    orchestratorCpuRequest = orchestratorCpu,
    usedOnDemandCapacity = false,
    createdAt = OffsetDateTime.now(),
  )
}
