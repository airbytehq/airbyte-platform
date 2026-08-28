/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.commons.json.Jsons
import io.airbyte.data.repositories.entities.Job
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import io.kotest.matchers.shouldBe
import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

@MicronautTest(environments = [Environment.TEST])
internal class JobsRepositoryTest : AbstractConfigRepositoryTest() {
  private val scope1 = "scope1"
  private val scope2 = "scope2"
  private val scope3 = "scope3"
  private val scope4 = "scope4"
  private val sourceDefinitionVersionId = UUID.fromString("0d1777a3-0000-0000-0000-000000000001")
  private val destinationDefinitionVersionId = UUID.fromString("0d1777a3-0000-0000-0000-000000000002")

  /**
   * Job configs nest the replication config under a per-config-type key, so the health summary
   * query has to read from a different path for each type.
   */
  private fun configFor(configType: JobConfigType) =
    Jsons.jsonNode(
      mapOf(
        when (configType) {
          JobConfigType.refresh -> "refresh"
          JobConfigType.reset_connection -> "resetConnection"
          else -> "sync"
        } to
          mapOf(
            "sourceDefinitionVersionId" to sourceDefinitionVersionId,
            "destinationDefinitionVersionId" to destinationDefinitionVersionId,
            "sourceDockerImageIsDefault" to true,
            "destinationDockerImageIsDefault" to false,
            "configuredAirbyteCatalog" to mapOf("streams" to listOf("stream")),
          ),
      ),
    )

  private var nextCreatedAt = OffsetDateTime.of(2021, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)

  @AfterEach
  fun cleanup() {
    jobsRepository.deleteAll()
  }

  /**
   * Create a job with the given scope and status.
   * Increments the job ID and created at time on each call so that
   * created jobs are ordered and unique.
   */
  private fun createJob(
    id: Int,
    scope: String,
    status: JobStatus,
    configType: JobConfigType = JobConfigType.sync,
  ) = Job(
    id = id.toLong(),
    scope = scope,
    status = status,
    configType = configType,
    config = configFor(configType),
    createdAt = nextCreatedAt,
    updatedAt = nextCreatedAt,
    isScheduled = false,
  ).also { nextCreatedAt = nextCreatedAt.plusDays(1) }

  private fun assertHealthSummary(summary: LatestJobHealthSummaryRow) {
    summary.sourceDefinitionVersionId.shouldBe(sourceDefinitionVersionId)
    summary.destinationDefinitionVersionId.shouldBe(destinationDefinitionVersionId)
    summary.sourceDockerImageIsDefault.shouldBe(true)
    summary.destinationDockerImageIsDefault.shouldBe(false)
  }

  @Nested
  inner class CountFailedJobsSinceLastSuccessForScope {
    @Test
    fun `test count failed jobs since last success for scope`() {
      val jobs =
        listOf(
          createJob(1, scope1, JobStatus.failed),
          createJob(2, scope1, JobStatus.succeeded),
          createJob(3, scope1, JobStatus.failed),
          // wrong scope
          createJob(4, scope2, JobStatus.failed),
          createJob(5, scope1, JobStatus.cancelled),
          createJob(6, scope1, JobStatus.failed),
        )

      jobsRepository.saveAll(jobs)

      val result = jobsRepository.countFailedJobsSinceLastSuccessForScope(scope1)

      result.shouldBe(2)
    }

    @Test
    fun `test count all failed jobs if no success`() {
      val jobs =
        listOf(
          createJob(1, scope1, JobStatus.failed),
          createJob(2, scope1, JobStatus.failed),
          createJob(3, scope1, JobStatus.failed),
          // wrong scope
          createJob(4, scope2, JobStatus.failed),
          createJob(5, scope1, JobStatus.cancelled),
          createJob(6, scope1, JobStatus.failed),
          createJob(7, scope1, JobStatus.running),
        )

      jobsRepository.saveAll(jobs)

      val result = jobsRepository.countFailedJobsSinceLastSuccessForScope(scope1)

      result.shouldBe(4)
    }
  }

  @Nested
  inner class LastSuccessfulJobForScope {
    @Test
    fun `returns most recent succeeded job for scope`() {
      val jobs =
        listOf(
          createJob(1, scope1, JobStatus.failed),
          createJob(2, scope1, JobStatus.succeeded),
          createJob(3, scope1, JobStatus.failed),
          // most recent success for scope1
          createJob(4, scope1, JobStatus.succeeded),
          // most recent success for scope2
          createJob(5, scope2, JobStatus.succeeded),
          createJob(6, scope1, JobStatus.cancelled),
          createJob(7, scope1, JobStatus.failed),
          // scope3 never succeeded
          createJob(8, scope3, JobStatus.running),
        )

      jobsRepository.saveAll(jobs)

      val resultScope1 = jobsRepository.lastSuccessfulJobForScope(scope1)
      val resultScope2 = jobsRepository.lastSuccessfulJobForScope(scope2)
      val resultScope3 = jobsRepository.lastSuccessfulJobForScope(scope3)

      resultScope1?.id.shouldBe(4)
      resultScope2?.id.shouldBe(5)
      resultScope3.shouldBe(null)
    }
  }

  @Nested
  inner class GetPriorJobWithStatusForScopeAndJobId {
    @Test
    fun `returns most recent job with given status before given job ID`() {
      val jobs =
        listOf(
          createJob(1, scope1, JobStatus.failed),
          createJob(2, scope1, JobStatus.succeeded),
          createJob(3, scope1, JobStatus.failed),
          createJob(4, scope2, JobStatus.succeeded),
          createJob(5, scope2, JobStatus.failed),
          createJob(6, scope2, JobStatus.running),
          createJob(7, scope3, JobStatus.failed),
          createJob(8, scope3, JobStatus.failed),
        )

      jobsRepository.saveAll(jobs)

      val result1 = jobsRepository.getPriorJobWithStatusForScopeAndJobId(scope1, 3, JobStatus.failed)
      val result2 = jobsRepository.getPriorJobWithStatusForScopeAndJobId(scope2, 6, JobStatus.succeeded)
      val result3 = jobsRepository.getPriorJobWithStatusForScopeAndJobId(scope3, 8, JobStatus.succeeded)

      result1?.id.shouldBe(1)
      result2?.id.shouldBe(4)
      result3.shouldBe(null)
    }
  }

  @Nested
  inner class FindLatestJobPerScope {
    @Test
    fun `returns latest job per scope matching config type and createdAt`() {
      val jobs =
        listOf(
          // scope1
          createJob(1, scope1, JobStatus.failed), // older
          createJob(2, scope1, JobStatus.succeeded), // newer
          // scope2
          createJob(3, scope2, JobStatus.running), // only one
          // scope3
          createJob(4, scope3, JobStatus.failed), // before threshold
          createJob(5, scope3, JobStatus.succeeded), // after threshold
        )

      jobsRepository.saveAll(jobs)

      val createdAtThreshold = OffsetDateTime.of(2021, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC)
      val result =
        jobsRepository.findLatestJobPerScope(
          configTypes = listOf(JobConfigType.sync.literal),
          scopes = setOf(scope1, scope2, scope3),
          createdAtStart = createdAtThreshold,
        )

      // should return:
      // - scope1: job 2 (created Jan 2)
      // - scope2: job 3 (created Jan 3)
      // - scope3: job 5 (created Jan 5)
      result.size.shouldBe(3)

      result.map { it.scope }.toSet().shouldBe(setOf(scope1, scope2, scope3))
      result.forEach(::assertHealthSummary)
      result.find { it.scope == scope1 }!!.status.shouldBe(JobStatus.succeeded)
      result.find { it.scope == scope2 }!!.status.shouldBe(JobStatus.running)
      result.find { it.scope == scope3 }!!.status.shouldBe(JobStatus.succeeded)
    }

    @Test
    fun `picks the newest job across every requested config type`() {
      // scope1's refresh (Jan 2) is newer than its sync (Jan 1), so refresh must win.
      jobsRepository.saveAll(
        listOf(
          createJob(1, scope1, JobStatus.succeeded, JobConfigType.sync),
          createJob(2, scope1, JobStatus.failed, JobConfigType.refresh),
          createJob(3, scope2, JobStatus.succeeded, JobConfigType.reset_connection),
        ),
      )

      val result =
        jobsRepository.findLatestJobPerScope(
          configTypes = listOf(JobConfigType.sync.literal, JobConfigType.refresh.literal),
          scopes = setOf(scope1, scope2),
          createdAtStart = OffsetDateTime.of(2021, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC),
        )

      // scope2 only has reset_connection, which was not requested
      result.map { it.scope }.shouldBe(listOf(scope1))
      result[0].status.shouldBe(JobStatus.failed)
      assertHealthSummary(result[0])
    }

    @Test
    fun `reads the replication config from the key matching the requested config type`() {
      val jobs =
        listOf(
          createJob(1, scope1, JobStatus.succeeded, JobConfigType.sync),
          createJob(2, scope2, JobStatus.failed, JobConfigType.refresh),
        )

      jobsRepository.saveAll(jobs)

      val result =
        jobsRepository.findLatestJobPerScope(
          configTypes = listOf(JobConfigType.refresh.literal),
          scopes = setOf(scope1, scope2, scope3),
          createdAtStart = OffsetDateTime.of(2021, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC),
        )

      // sync was not requested, so scope1 drops out; scope2's fields come from config->'refresh'
      result.map { it.scope }.toSet().shouldBe(setOf(scope2))
      result.forEach(::assertHealthSummary)
      result[0].status.shouldBe(JobStatus.failed)
    }

    @Test
    fun `returns null health fields when the replication config is absent`() {
      jobsRepository.save(
        Job(
          id = 1,
          scope = scope1,
          status = JobStatus.succeeded,
          configType = JobConfigType.sync,
          config = Jsons.jsonNode(mapOf<String, String>()),
          createdAt = nextCreatedAt,
          updatedAt = nextCreatedAt,
          isScheduled = false,
        ),
      )

      val result =
        jobsRepository.findLatestJobPerScope(
          configTypes = listOf(JobConfigType.sync.literal),
          scopes = setOf(scope1),
          createdAtStart = OffsetDateTime.of(2021, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC),
        )

      result.size.shouldBe(1)
      result[0].status.shouldBe(JobStatus.succeeded)
      result[0].sourceDefinitionVersionId.shouldBe(null)
      result[0].destinationDefinitionVersionId.shouldBe(null)
      result[0].sourceDockerImageIsDefault.shouldBe(null)
      result[0].destinationDockerImageIsDefault.shouldBe(null)
    }
  }

  @Test
  fun `returns latest job per scope matching config type and createdAt`() {
    val jobs =
      listOf(
        // job 1 is before threshold
        createJob(1, scope1, JobStatus.failed),
        createJob(2, scope1, JobStatus.succeeded),
        createJob(3, scope2, JobStatus.running),
        // job 4 is older than job 5 so shouldn't be included
        createJob(4, scope3, JobStatus.succeeded),
        createJob(5, scope3, JobStatus.failed),
        // job 6's config doesn't match
        createJob(6, scope3, JobStatus.succeeded, JobConfigType.refresh),
        // job 7's scope doesn't match
        createJob(7, scope4, JobStatus.succeeded),
      )

    jobsRepository.saveAll(jobs)

    val createdAtThreshold = OffsetDateTime.of(2021, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC)
    val result =
      jobsRepository.findLatestJobPerScope(
        configTypes = listOf(JobConfigType.sync.literal),
        scopes = setOf(scope1, scope2, scope3),
        createdAtStart = createdAtThreshold,
      )

    // should return:
    // - scope1: job 2 (created Jan 2)
    // - scope2: job 3 (created Jan 3)
    // - scope3: job 5 (created Jan 5)
    result.size.shouldBe(3)

    result.map { it.scope }.toSet().shouldBe(setOf(scope1, scope2, scope3))
    result.forEach(::assertHealthSummary)
    result.find { it.scope == scope1 }!!.status.shouldBe(JobStatus.succeeded)
    result.find { it.scope == scope2 }!!.status.shouldBe(JobStatus.running)
    result.find { it.scope == scope3 }!!.status.shouldBe(JobStatus.failed)
  }
}
