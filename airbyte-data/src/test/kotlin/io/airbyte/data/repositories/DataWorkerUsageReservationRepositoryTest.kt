/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.commons.json.Jsons
import io.airbyte.data.repositories.entities.DataWorkerUsageReservation
import io.airbyte.data.repositories.entities.Job
import io.airbyte.data.repositories.entities.NON_TERMINAL_STATUSES
import io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import io.kotest.matchers.doubles.plusOrMinus
import io.kotest.matchers.shouldBe
import io.micronaut.context.env.Environment
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.transaction.TransactionOperations
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.sql.Connection
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

@MicronautTest(environments = [Environment.TEST])
internal class DataWorkerUsageReservationRepositoryTest : AbstractConfigRepositoryTest() {
  private val config = Jsons.jsonNode(mapOf<String, String>())
  private var nextCreatedAt = OffsetDateTime.of(2021, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)

  @Suppress("UNCHECKED_CAST")
  private val configTransactionOperations =
    context.getBean(TransactionOperations::class.java, Qualifiers.byName("config")) as TransactionOperations<Connection>

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

  @ParameterizedTest
  @EnumSource(value = JobStatus::class, names = ["failed", "succeeded", "cancelled"])
  fun `findTerminalReservationCandidates includes every terminal status`(status: JobStatus) {
    val organizationId = UUID.randomUUID()
    val terminalAt = OffsetDateTime.of(2026, 8, 26, 10, 0, 0, 0, ZoneOffset.UTC)
    jobsRepository.save(createJob(1L, status))
    setJobUpdatedAt(1L, terminalAt)
    dataWorkerUsageReservationRepository.save(createReservation(1L, organizationId))

    val result =
      dataWorkerUsageReservationRepository.findTerminalReservationCandidates(
        organizationIds = listOf(organizationId),
        terminalBefore = terminalAt,
        limit = 100,
      )

    result.map { it.jobId } shouldBe listOf(1L)
    result.single().organizationId shouldBe organizationId
    result.single().terminalAt.toInstant() shouldBe terminalAt.toInstant()
  }

  @ParameterizedTest
  @EnumSource(value = JobStatus::class, mode = EnumSource.Mode.EXCLUDE, names = ["failed", "succeeded", "cancelled"])
  fun `findTerminalReservationCandidates excludes every non-terminal status`(status: JobStatus) {
    val organizationId = UUID.randomUUID()
    val terminalAt = OffsetDateTime.of(2026, 8, 26, 10, 0, 0, 0, ZoneOffset.UTC)
    jobsRepository.save(createJob(1L, status))
    setJobUpdatedAt(1L, terminalAt)
    dataWorkerUsageReservationRepository.save(createReservation(1L, organizationId))

    val result =
      dataWorkerUsageReservationRepository.findTerminalReservationCandidates(
        organizationIds = listOf(organizationId),
        terminalBefore = terminalAt,
        limit = 100,
      )

    result shouldBe emptyList()
  }

  @Test
  fun `findTerminalReservationCandidates includes the cutoff and excludes newer jobs`() {
    val organizationId = UUID.randomUUID()
    val cutoff = OffsetDateTime.of(2026, 8, 26, 10, 0, 0, 0, ZoneOffset.UTC)
    jobsRepository.saveAll(
      listOf(
        createJob(1L, JobStatus.failed),
        createJob(2L, JobStatus.failed),
      ),
    )
    setJobUpdatedAt(1L, cutoff)
    setJobUpdatedAt(2L, cutoff.plusSeconds(1))
    dataWorkerUsageReservationRepository.saveAll(
      listOf(
        createReservation(1L, organizationId),
        createReservation(2L, organizationId),
      ),
    )

    val result =
      dataWorkerUsageReservationRepository.findTerminalReservationCandidates(
        organizationIds = listOf(organizationId),
        terminalBefore = cutoff,
        limit = 100,
      )

    result.map { it.jobId } shouldBe listOf(1L)
  }

  @Test
  fun `findTerminalReservationCandidates is organization scoped`() {
    val targetOrganizationId = UUID.randomUUID()
    val otherOrganizationId = UUID.randomUUID()
    val terminalAt = OffsetDateTime.of(2026, 8, 26, 10, 0, 0, 0, ZoneOffset.UTC)
    jobsRepository.saveAll(
      listOf(
        createJob(1L, JobStatus.succeeded),
        createJob(2L, JobStatus.succeeded),
      ),
    )
    setJobUpdatedAt(1L, terminalAt)
    setJobUpdatedAt(2L, terminalAt)
    dataWorkerUsageReservationRepository.saveAll(
      listOf(
        createReservation(1L, targetOrganizationId),
        createReservation(2L, otherOrganizationId),
      ),
    )

    val result =
      dataWorkerUsageReservationRepository.findTerminalReservationCandidates(
        organizationIds = listOf(targetOrganizationId),
        terminalBefore = terminalAt,
        limit = 100,
      )

    result.map { it.jobId } shouldBe listOf(1L)
  }

  @Test
  fun `findTerminalReservationCandidates orders deterministically and applies the limit`() {
    val organizationId = UUID.randomUUID()
    val oldest = OffsetDateTime.of(2026, 8, 26, 8, 0, 0, 0, ZoneOffset.UTC)
    jobsRepository.saveAll((1L..5L).map { createJob(it, JobStatus.cancelled) })
    setJobUpdatedAt(1L, oldest.plusHours(2))
    setJobUpdatedAt(2L, oldest)
    setJobUpdatedAt(3L, oldest.plusHours(1))
    setJobUpdatedAt(4L, oldest)
    setJobUpdatedAt(5L, oldest.plusHours(3))
    dataWorkerUsageReservationRepository.saveAll((1L..5L).map { createReservation(it, organizationId) })

    val result =
      dataWorkerUsageReservationRepository.findTerminalReservationCandidates(
        organizationIds = listOf(organizationId),
        terminalBefore = oldest.plusHours(3),
        limit = 3,
      )

    result.map { it.jobId } shouldBe listOf(2L, 4L, 3L)
  }

  @Test
  fun `findTerminalReservationCandidatesAfter starts strictly after timestamp and job ID cursor`() {
    val organizationId = UUID.randomUUID()
    val otherOrganizationId = UUID.randomUUID()
    val cursorTime = OffsetDateTime.of(2026, 8, 26, 8, 0, 0, 0, ZoneOffset.UTC)
    jobsRepository.saveAll((1L..5L).map { createJob(it, JobStatus.cancelled) })
    setJobUpdatedAt(1L, cursorTime.minusSeconds(1))
    setJobUpdatedAt(2L, cursorTime)
    setJobUpdatedAt(3L, cursorTime)
    setJobUpdatedAt(4L, cursorTime.plusSeconds(1))
    setJobUpdatedAt(5L, cursorTime.plusSeconds(1))
    dataWorkerUsageReservationRepository.saveAll(
      (1L..4L).map { createReservation(it, organizationId) } + createReservation(5L, otherOrganizationId),
    )

    val result =
      dataWorkerUsageReservationRepository.findTerminalReservationCandidatesAfter(
        organizationIds = listOf(organizationId),
        terminalBefore = cursorTime.plusSeconds(1),
        terminalAfter = cursorTime,
        jobIdAfter = 2L,
        limit = 100,
      )

    result.map { it.jobId } shouldBe listOf(3L, 4L)
  }

  @Test
  fun `findTerminalReservationCandidatesAfter returns empty at end of candidates`() {
    val organizationId = UUID.randomUUID()
    val terminalAt = OffsetDateTime.of(2026, 8, 26, 8, 0, 0, 0, ZoneOffset.UTC)
    jobsRepository.save(createJob(1L, JobStatus.succeeded))
    setJobUpdatedAt(1L, terminalAt)
    dataWorkerUsageReservationRepository.save(createReservation(1L, organizationId))

    val result =
      dataWorkerUsageReservationRepository.findTerminalReservationCandidatesAfter(
        organizationIds = listOf(organizationId),
        terminalBefore = terminalAt,
        terminalAfter = terminalAt,
        jobIdAfter = 1L,
        limit = 100,
      )

    result shouldBe emptyList()
  }

  @Test
  fun `scoped reservation lock serializes concurrent deletion`() {
    val organizationId = UUID.randomUUID()
    val jobId = 1L
    jobsRepository.save(createJob(jobId, JobStatus.succeeded))
    dataWorkerUsageReservationRepository.save(createReservation(jobId, organizationId))
    val firstHasDeleted = CountDownLatch(1)
    val allowFirstCommit = CountDownLatch(1)
    val secondStarted = CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(2)

    try {
      val first =
        executor.submit(
          Callable {
            configTransactionOperations.executeWrite { _ ->
              dataWorkerUsageReservationRepository
                .findByJobIdAndOrganizationIdForUpdate(jobId, organizationId)
                .isPresent shouldBe true
              dataWorkerUsageReservationRepository.deleteByJobIdAndOrganizationId(jobId, organizationId) shouldBe 1L
              firstHasDeleted.countDown()
              allowFirstCommit.await(10, TimeUnit.SECONDS) shouldBe true
              true
            }
          },
        )
      firstHasDeleted.await(10, TimeUnit.SECONDS) shouldBe true

      val second =
        executor.submit(
          Callable {
            configTransactionOperations.executeWrite { _ ->
              secondStarted.countDown()
              dataWorkerUsageReservationRepository
                .findByJobIdAndOrganizationIdForUpdate(jobId, organizationId)
                .isPresent
            }
          },
        )
      secondStarted.await(10, TimeUnit.SECONDS) shouldBe true
      assertThrows(TimeoutException::class.java) { second.get(300, TimeUnit.MILLISECONDS) }

      allowFirstCommit.countDown()
      first.get(10, TimeUnit.SECONDS) shouldBe true
      second.get(10, TimeUnit.SECONDS) shouldBe false
      assertFalse(dataWorkerUsageReservationRepository.existsById(jobId))
    } finally {
      allowFirstCommit.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `insertReservationIfJobActive inserts a reservation when the job is active`() {
    val organizationId = UUID.randomUUID()
    val jobId = 1L
    jobsRepository.save(createJob(jobId, JobStatus.running))

    val inserted = insertReservationIfActive(jobId, organizationId)

    inserted shouldBe 1
    dataWorkerUsageReservationRepository.existsById(jobId) shouldBe true
  }

  @ParameterizedTest
  @EnumSource(value = JobStatus::class, names = ["failed", "succeeded", "cancelled"])
  fun `insertReservationIfJobActive does not insert when the job is terminal`(status: JobStatus) {
    val organizationId = UUID.randomUUID()
    val jobId = 1L
    jobsRepository.save(createJob(jobId, status))

    val inserted = insertReservationIfActive(jobId, organizationId)

    inserted shouldBe 0
    dataWorkerUsageReservationRepository.existsById(jobId) shouldBe false
  }

  @Test
  fun `insertReservationIfJobActive does not insert when the job does not exist`() {
    val inserted = insertReservationIfActive(jobId = 42L, organizationId = UUID.randomUUID())

    inserted shouldBe 0
    dataWorkerUsageReservationRepository.existsById(42L) shouldBe false
  }

  @Test
  fun `insertReservationIfJobActive is idempotent on a duplicate reservation`() {
    val organizationId = UUID.randomUUID()
    val jobId = 1L
    jobsRepository.save(createJob(jobId, JobStatus.running))

    insertReservationIfActive(jobId, organizationId) shouldBe 1
    insertReservationIfActive(jobId, organizationId) shouldBe 0
    dataWorkerUsageReservationRepository.existsById(jobId) shouldBe true
  }

  private fun insertReservationIfActive(
    jobId: Long,
    organizationId: UUID,
  ): Int =
    dataWorkerUsageReservationRepository.insertReservationIfJobActive(
      jobId = jobId,
      organizationId = organizationId,
      workspaceId = UUID.randomUUID(),
      dataplaneGroupId = UUID.randomUUID(),
      sourceCpuRequest = 1.0,
      destinationCpuRequest = 1.0,
      orchestratorCpuRequest = 2.0,
      usedOnDemandCapacity = false,
      createdAt = OffsetDateTime.now(),
    )

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
    sourceCpu: Double = 1.0,
    destinationCpu: Double = 1.0,
    orchestratorCpu: Double = 1.0,
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

  private fun setJobUpdatedAt(
    jobId: Long,
    updatedAt: OffsetDateTime,
  ) {
    jooqDslContext
      .update(JOBS)
      .set(JOBS.UPDATED_AT, updatedAt)
      .where(JOBS.ID.eq(jobId))
      .execute() shouldBe 1
  }
}
