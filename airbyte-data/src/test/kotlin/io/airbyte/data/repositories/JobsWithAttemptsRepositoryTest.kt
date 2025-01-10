/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.commons.json.Jsons
import io.airbyte.data.repositories.entities.Attempt
import io.airbyte.data.repositories.entities.Job
import io.airbyte.db.instance.jobs.jooq.generated.enums.AttemptStatus
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import io.micronaut.context.env.Environment
import io.micronaut.data.model.Pageable
import io.micronaut.data.model.Sort
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

@MicronautTest(environments = [Environment.TEST])
internal class JobsWithAttemptsRepositoryTest : AbstractConfigRepositoryTest() {
  @AfterEach
  fun tearDown() {
    attemptsRepository.deleteAll()
    jobsRepository.deleteAll()
    jobsWithAttemptsRepository.deleteAll()
  }

  @Test
  internal fun testFetchJobWithAssociatedAttempts() {
    val scope = UUID.randomUUID().toString()
    val job =
      Job(
        id = 1L,
        status = JobStatus.succeeded,
        scope = scope,
        configType = JobConfigType.sync,
        config = Jsons.jsonNode(mapOf<String, String>()),
      )

    val savedJob = jobsRepository.save(job)

    val attempt1 =
      Attempt(
        id = 1L,
        output = Jsons.jsonNode(mapOf<String, String>()),
        attemptNumber = 1,
        jobId = savedJob.id,
        status = AttemptStatus.failed,
        attemptSyncConfig = Jsons.jsonNode(mapOf<String, String>()),
      )
    val attempt2 =
      Attempt(
        id = 2L,
        output = Jsons.jsonNode(mapOf<String, String>()),
        attemptNumber = 2,
        jobId = savedJob.id,
        status = AttemptStatus.succeeded,
        attemptSyncConfig = Jsons.jsonNode(mapOf<String, String>()),
      )

    val savedAttempt1 = attemptsRepository.save(attempt1)
    val savedAttempt2 = attemptsRepository.save(attempt2)

    val results =
      jobsWithAttemptsRepository.findAll(
        Specifications.jobWithAssociatedAttempts(
          statuses = setOf(),
          updatedAtStart = null,
          createdAtStart = null,
          updatedAtEnd = null,
          createdAtEnd = null,
          configTypes = setOf(),
          scope = scope,
        ),
      )

    assertEquals(1, results.size)
    assertEquals(2, results.first().attempts?.size)
    assertEquals(savedJob.id, results.first().id)
    val attempts = results.first().attempts?.sortedBy { it.id }
    assertEquals(savedAttempt1.id, attempts?.first()?.id)
    assertEquals(savedAttempt2.id, attempts?.last()?.id)
    assertEquals(savedJob.id, attempts?.first()?.jobId)
    assertEquals(savedJob.id, attempts?.last()?.jobId)
  }

  @Test
  internal fun testFetchJobWithAssociatedAttemptsWithoutPredicate() {
    val scope = UUID.randomUUID().toString()
    val job =
      Job(
        id = 1L,
        status = JobStatus.succeeded,
        scope = scope,
        configType = JobConfigType.sync,
        config = Jsons.jsonNode(mapOf<String, String>()),
      )

    val savedJob = jobsRepository.save(job)

    val attempt1 =
      Attempt(
        id = 1L,
        output = Jsons.jsonNode(mapOf<String, String>()),
        attemptNumber = 1,
        jobId = savedJob.id,
        status = AttemptStatus.failed,
        attemptSyncConfig = Jsons.jsonNode(mapOf<String, String>()),
      )
    val attempt2 =
      Attempt(
        id = 2L,
        output = Jsons.jsonNode(mapOf<String, String>()),
        attemptNumber = 2,
        jobId = savedJob.id,
        status = AttemptStatus.succeeded,
        attemptSyncConfig = Jsons.jsonNode(mapOf<String, String>()),
      )

    val savedAttempt1 = attemptsRepository.save(attempt1)
    val savedAttempt2 = attemptsRepository.save(attempt2)

    val results =
      jobsWithAttemptsRepository.findAll(
        Specifications.jobWithAssociatedAttempts(
          statuses = setOf(),
          updatedAtStart = null,
          createdAtStart = null,
          updatedAtEnd = null,
          createdAtEnd = null,
          configTypes = setOf(),
          scope = "",
        ),
      )

    assertEquals(1, results.size)
    assertEquals(2, results.first().attempts?.size)
    assertEquals(savedJob.id, results.first().id)
    val attempts = results.first().attempts?.sortedBy { it.id }
    assertEquals(savedAttempt1.id, attempts?.first()?.id)
    assertEquals(savedAttempt2.id, attempts?.last()?.id)
    assertEquals(savedJob.id, attempts?.first()?.jobId)
    assertEquals(savedJob.id, attempts?.last()?.jobId)
  }

  @Test
  fun testFetchWithPageable() {
    val scope = UUID.randomUUID().toString()
    val job =
      Job(
        id = 1L,
        status = JobStatus.succeeded,
        scope = scope,
        configType = JobConfigType.sync,
        config = Jsons.jsonNode(mapOf<String, String>()),
        createdAt = OffsetDateTime.MIN,
      )

    val job2 =
      Job(
        id = 2L,
        status = JobStatus.succeeded,
        scope = scope,
        configType = JobConfigType.sync,
        config = Jsons.jsonNode(mapOf<String, String>()),
        createdAt = OffsetDateTime.MAX,
      )

    val savedJob = jobsRepository.save(job)
    val savedJob2 = jobsRepository.save(job2)

    val pageable = Pageable.from(0, 1).order("createdAt", Sort.Order.Direction.DESC)

    val results =
      jobsWithAttemptsRepository.findAll(
        Specifications.jobWithAssociatedAttempts(
          statuses = setOf(),
          updatedAtStart = null,
          createdAtStart = null,
          updatedAtEnd = null,
          createdAtEnd = null,
          configTypes = setOf(),
          scope = "",
        ),
        pageable,
      )

    val resultList = results.toList()
    assertEquals(resultList.size, 1)
    assertEquals(resultList[0].id, savedJob2.id)

    val pageable2 = Pageable.from(1, 1).order("createdAt", Sort.Order.Direction.DESC)

    val results2 =
      jobsWithAttemptsRepository.findAll(
        Specifications.jobWithAssociatedAttempts(
          statuses = setOf(),
          updatedAtStart = null,
          createdAtStart = null,
          updatedAtEnd = null,
          createdAtEnd = null,
          configTypes = setOf(),
          scope = "",
        ),
        pageable2,
      )

    val resultList2 = results2.toList()
    assertEquals(resultList2.size, 1)
    assertEquals(resultList2[0].id, savedJob.id)
  }
}
