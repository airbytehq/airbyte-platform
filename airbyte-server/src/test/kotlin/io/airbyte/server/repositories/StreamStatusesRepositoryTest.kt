/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.repositories

import io.airbyte.commons.json.Jsons
import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.jobs.jooq.generated.Keys
import io.airbyte.db.instance.jobs.jooq.generated.Tables
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusIncompleteRunCause
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusJobType
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusRunState
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.airbyte.server.repositories.StreamStatusesRepository.FilterParams
import io.airbyte.server.repositories.domain.StreamStatus
import io.airbyte.server.repositories.domain.StreamStatus.StreamStatusBuilder
import io.airbyte.server.repositories.domain.StreamStatusRateLimitedMetadataRepositoryStructure
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.PropertySource
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import javax.sql.DataSource

@MicronautTest(rebuildContext = true)
internal class StreamStatusesRepositoryTest {
  @BeforeEach
  fun truncate() {
    jooqDslContext!!.truncateTable(Tables.STREAM_STATUSES).cascade().execute()
  }

  @Test
  fun testInsert() {
    val s = Fixtures.status().build()

    val inserted = repo!!.save(s)

    val found = repo!!.findById(inserted.id)

    Assertions.assertTrue(found.isPresent)
    Assertions.assertEquals(inserted, found.get())
  }

  @Test
  fun testUpdateCompleteFlow() {
    val pendingAt = Fixtures.now()
    val s = Fixtures.status().transitionedAt(pendingAt).build()

    val inserted = repo!!.save(s)
    val id = inserted.id
    val found1 = repo!!.findById(id)

    val runningAt = Fixtures.now()
    val running =
      Fixtures
        .statusFrom(inserted)
        .runState(JobStreamStatusRunState.running)
        .transitionedAt(runningAt)
        .build()
    repo!!.update(running)
    val found2 = repo!!.findById(id)

    val rateLimitedAt = Fixtures.now()
    val rateLimited =
      Fixtures
        .statusFrom(running)
        .runState(JobStreamStatusRunState.rate_limited)
        .transitionedAt(rateLimitedAt)
        .metadata(Jsons.jsonNode(StreamStatusRateLimitedMetadataRepositoryStructure(Fixtures.now().toInstant().toEpochMilli())))
        .build()
    repo!!.update(rateLimited)
    val found3 = repo!!.findById(id)

    val completedAt = Fixtures.now()
    val completed =
      Fixtures
        .statusFrom(rateLimited)
        .runState(JobStreamStatusRunState.complete)
        .metadata(null)
        .transitionedAt(completedAt)
        .build()
    repo!!.update(completed)
    val found4 = repo!!.findById(id)

    Assertions.assertTrue(found1.isPresent)
    Assertions.assertEquals(pendingAt, found1.get().transitionedAt)
    Assertions.assertEquals(JobStreamStatusRunState.pending, found1.get().runState)

    Assertions.assertTrue(found2.isPresent)
    Assertions.assertEquals(runningAt, found2.get().transitionedAt)
    Assertions.assertEquals(JobStreamStatusRunState.running, found2.get().runState)

    Assertions.assertTrue(found3.isPresent)
    Assertions.assertEquals(rateLimitedAt, found3.get().transitionedAt)
    Assertions.assertEquals(JobStreamStatusRunState.rate_limited, found3.get().runState)

    Assertions.assertTrue(found4.isPresent)
    Assertions.assertEquals(completedAt, found4.get().transitionedAt)
    Assertions.assertEquals(JobStreamStatusRunState.complete, found4.get().runState)
  }

  @Test
  fun testUpdateIncompleteFlowFailed() {
    val pendingAt = Fixtures.now()
    val s = Fixtures.status().transitionedAt(pendingAt).build()

    val inserted = repo!!.save(s)
    val id = inserted.id
    val found1 = repo!!.findById(id)

    val runningAt = Fixtures.now()
    val running =
      Fixtures
        .statusFrom(inserted)
        .runState(JobStreamStatusRunState.running)
        .transitionedAt(runningAt)
        .build()
    repo!!.update(running)
    val found2 = repo!!.findById(id)

    val incompleteAt = Fixtures.now()
    val incomplete =
      Fixtures
        .statusFrom(running)
        .runState(JobStreamStatusRunState.incomplete)
        .incompleteRunCause(JobStreamStatusIncompleteRunCause.failed)
        .transitionedAt(incompleteAt)
        .build()
    repo!!.update(incomplete)
    val found3 = repo!!.findById(id)

    Assertions.assertTrue(found1.isPresent)
    Assertions.assertEquals(pendingAt, found1.get().transitionedAt)
    Assertions.assertEquals(JobStreamStatusRunState.pending, found1.get().runState)
    Assertions.assertNull(found1.get().incompleteRunCause)

    Assertions.assertTrue(found2.isPresent)
    Assertions.assertEquals(runningAt, found2.get().transitionedAt)
    Assertions.assertEquals(JobStreamStatusRunState.running, found2.get().runState)
    Assertions.assertNull(found2.get().incompleteRunCause)

    Assertions.assertTrue(found3.isPresent)
    Assertions.assertEquals(incompleteAt, found3.get().transitionedAt)
    Assertions.assertEquals(JobStreamStatusRunState.incomplete, found3.get().runState)
    Assertions.assertEquals(JobStreamStatusIncompleteRunCause.failed, found3.get().incompleteRunCause)
  }

  @Test
  fun testUpdateIncompleteFlowCanceled() {
    val pendingAt = Fixtures.now()
    val s = Fixtures.status().transitionedAt(pendingAt).build()

    val inserted = repo!!.save(s)
    val id = inserted.id
    val found1 = repo!!.findById(id)

    val runningAt = Fixtures.now()
    val running =
      Fixtures
        .statusFrom(inserted)
        .runState(JobStreamStatusRunState.running)
        .transitionedAt(runningAt)
        .build()
    repo!!.update(running)
    val found2 = repo!!.findById(id)

    val incompleteAt = Fixtures.now()
    val incomplete =
      Fixtures
        .statusFrom(running)
        .runState(JobStreamStatusRunState.incomplete)
        .incompleteRunCause(JobStreamStatusIncompleteRunCause.canceled)
        .transitionedAt(incompleteAt)
        .build()
    repo!!.update(incomplete)
    val found3 = repo!!.findById(id)

    Assertions.assertTrue(found1.isPresent)
    Assertions.assertEquals(pendingAt, found1.get().transitionedAt)
    Assertions.assertEquals(JobStreamStatusRunState.pending, found1.get().runState)
    Assertions.assertNull(found1.get().incompleteRunCause)

    Assertions.assertTrue(found2.isPresent)
    Assertions.assertEquals(runningAt, found2.get().transitionedAt)
    Assertions.assertEquals(JobStreamStatusRunState.running, found2.get().runState)
    Assertions.assertNull(found2.get().incompleteRunCause)

    Assertions.assertTrue(found3.isPresent)
    Assertions.assertEquals(incompleteAt, found3.get().transitionedAt)
    Assertions.assertEquals(JobStreamStatusRunState.incomplete, found3.get().runState)
    Assertions.assertEquals(JobStreamStatusIncompleteRunCause.canceled, found3.get().incompleteRunCause)
  }

  @Test
  fun testFindAllFilteredSimple() {
    val s1 = Fixtures.status().workspaceId(Fixtures.workspaceId1).build()
    val s2 = Fixtures.status().workspaceId(Fixtures.workspaceId2).build()
    val inserted1 = repo!!.save(s1)
    val inserted2 = repo!!.save(s2)

    val result = repo!!.findAllFiltered(FilterParams(Fixtures.workspaceId1, null, null, null, null, null, null, null))

    Assertions.assertEquals(1, result.content.size)
    Assertions.assertEquals(inserted1.id, result.content.first().id)
    Assertions.assertNotEquals(inserted2.id, result.content.first().id)
  }

  @Test
  fun testFindAllFilteredMatrix() {
    // create and save a variety of stream statuses
    val s1 = Fixtures.status().build()
    val s2 = Fixtures.statusFrom(s1).attemptNumber(1).build()
    val s3 = Fixtures.statusFrom(s2).attemptNumber(2).build()
    val s4 = Fixtures.status().streamName(Fixtures.name2).build()
    val s5 = Fixtures.statusFrom(s4).attemptNumber(1).build()
    val s6 = Fixtures.status().workspaceId(Fixtures.workspaceId2).build()
    val s7 = Fixtures.status().workspaceId(Fixtures.workspaceId3).build()
    val s8 = Fixtures.status().jobId(Fixtures.jobId2).build()
    val s9 = Fixtures.statusFrom(s8).attemptNumber(1).build()
    val s10 = Fixtures.statusFrom(s8).streamName(Fixtures.name3).build()
    val s11 = Fixtures.status().connectionId(Fixtures.connectionId2).build()
    val s12 = Fixtures.status().connectionId(Fixtures.connectionId3).build()
    val s13 = Fixtures.status().streamNamespace("").build()
    val s14 = Fixtures.status().jobType(JobStreamStatusJobType.reset).build()
    val s15 = Fixtures.statusFrom(s8).jobType(JobStreamStatusJobType.reset).build()

    repo!!.saveAll(java.util.List.of(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15))

    // create some filter params on various properties
    val f1 = Fixtures.filters(Fixtures.workspaceId1, null, null, null, null, null, null, null)
    val f2 = Fixtures.filters(Fixtures.workspaceId2, null, null, null, null, null, null, null)
    val f3 = Fixtures.filters(Fixtures.workspaceId3, null, null, null, null, null, null, null)
    val f4 = Fixtures.filters(Fixtures.workspaceId1, Fixtures.connectionId1, null, null, null, null, null, null)
    val f5 = Fixtures.filters(Fixtures.workspaceId1, Fixtures.connectionId2, null, null, null, null, null, null)
    val f6 = Fixtures.filters(Fixtures.workspaceId1, Fixtures.connectionId1, null, Fixtures.namespace, null, null, null, null)
    val f7 =
      Fixtures.filters(Fixtures.workspaceId1, Fixtures.connectionId1, null, Fixtures.namespace, Fixtures.name1, null, null, null)
    val f8 = Fixtures.filters(Fixtures.workspaceId1, null, Fixtures.jobId1, null, null, null, null, null)
    val f9 = Fixtures.filters(Fixtures.workspaceId1, null, Fixtures.jobId2, null, null, null, null, null)
    val f10 = Fixtures.filters(Fixtures.workspaceId1, null, Fixtures.jobId3, null, null, null, null, null)
    val f11 = Fixtures.filters(Fixtures.workspaceId1, null, Fixtures.jobId1, Fixtures.namespace, Fixtures.name1, null, null, null)
    val f12 = Fixtures.filters(Fixtures.workspaceId1, null, Fixtures.jobId1, Fixtures.namespace, Fixtures.name2, null, null, null)
    val f13 =
      Fixtures.filters(Fixtures.workspaceId1, null, Fixtures.jobId1, Fixtures.namespace, Fixtures.name1, 2, null, null)
    val f14 = Fixtures.filters(Fixtures.workspaceId1, null, null, null, null, null, JobStreamStatusJobType.sync, null)
    val f15 = Fixtures.filters(Fixtures.workspaceId1, null, null, null, null, null, JobStreamStatusJobType.reset, null)

    assertContainsSameElements(java.util.List.of(s1, s2, s3, s4, s5, s8, s9, s10, s11, s12, s13, s14, s15), repo!!.findAllFiltered(f1).content)
    assertContainsSameElements(java.util.List.of(s6), repo!!.findAllFiltered(f2).content)
    assertContainsSameElements(java.util.List.of(s7), repo!!.findAllFiltered(f3).content)
    assertContainsSameElements(java.util.List.of(s1, s2, s3, s4, s5, s8, s9, s10, s13, s14, s15), repo!!.findAllFiltered(f4).content)
    assertContainsSameElements(java.util.List.of(s11), repo!!.findAllFiltered(f5).content)
    assertContainsSameElements(java.util.List.of(s1, s2, s3, s4, s5, s8, s9, s10, s14, s15), repo!!.findAllFiltered(f6).content)
    assertContainsSameElements(java.util.List.of(s1, s2, s3, s8, s9, s14, s15), repo!!.findAllFiltered(f7).content)
    assertContainsSameElements(java.util.List.of(s1, s2, s3, s4, s5, s11, s12, s13, s14), repo!!.findAllFiltered(f8).content)
    assertContainsSameElements(java.util.List.of(s8, s9, s10, s15), repo!!.findAllFiltered(f9).content)
    assertContainsSameElements(listOf(), repo!!.findAllFiltered(f10).content)
    assertContainsSameElements(java.util.List.of(s1, s2, s3, s11, s12, s14), repo!!.findAllFiltered(f11).content)
    assertContainsSameElements(java.util.List.of(s4, s5), repo!!.findAllFiltered(f12).content)
    assertContainsSameElements(java.util.List.of(s3), repo!!.findAllFiltered(f13).content)
    assertContainsSameElements(java.util.List.of(s1, s2, s3, s4, s5, s8, s9, s10, s11, s12, s13), repo!!.findAllFiltered(f14).content)
    assertContainsSameElements(java.util.List.of(s14, s15), repo!!.findAllFiltered(f15).content)
  }

  @Test
  fun testPagination() {
    // create 10 statuses
    val s1 = Fixtures.status().build()
    val s2 = Fixtures.status().build()
    val s3 = Fixtures.status().build()
    val s4 = Fixtures.status().build()
    val s5 = Fixtures.status().build()
    val s6 = Fixtures.status().build()
    val s7 = Fixtures.status().build()
    val s8 = Fixtures.status().build()
    val s9 = Fixtures.status().build()
    val s10 = Fixtures.status().build()

    repo!!.saveAll(java.util.List.of(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10))

    // paginate by 10 at a time
    val f1 = Fixtures.filters(Fixtures.workspaceId1, null, null, null, null, null, null, StreamStatusesRepository.Pagination(0, 10))
    val f2 = Fixtures.filters(Fixtures.workspaceId1, null, null, null, null, null, null, StreamStatusesRepository.Pagination(1, 10))

    // paginate by 5
    val f3 = Fixtures.filters(Fixtures.workspaceId1, null, null, null, null, null, null, StreamStatusesRepository.Pagination(0, 5))
    val f4 = Fixtures.filters(Fixtures.workspaceId1, null, null, null, null, null, null, StreamStatusesRepository.Pagination(1, 5))
    val f5 = Fixtures.filters(Fixtures.workspaceId1, null, null, null, null, null, null, StreamStatusesRepository.Pagination(2, 5))

    // paginate by 3
    val f6 = Fixtures.filters(Fixtures.workspaceId1, null, null, null, null, null, null, StreamStatusesRepository.Pagination(0, 3))
    val f7 = Fixtures.filters(Fixtures.workspaceId1, null, null, null, null, null, null, StreamStatusesRepository.Pagination(1, 3))
    val f8 = Fixtures.filters(Fixtures.workspaceId1, null, null, null, null, null, null, StreamStatusesRepository.Pagination(2, 3))
    val f9 = Fixtures.filters(Fixtures.workspaceId1, null, null, null, null, null, null, StreamStatusesRepository.Pagination(3, 3))
    val f10 = Fixtures.filters(Fixtures.workspaceId1, null, null, null, null, null, null, StreamStatusesRepository.Pagination(4, 3))

    assertContainsSameElements(java.util.List.of(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10), repo!!.findAllFiltered(f1).content)
    assertContainsSameElements(listOf(), repo!!.findAllFiltered(f2).content)

    assertContainsSameElements(java.util.List.of(s1, s2, s3, s4, s5), repo!!.findAllFiltered(f3).content)
    assertContainsSameElements(java.util.List.of(s6, s7, s8, s9, s10), repo!!.findAllFiltered(f4).content)
    assertContainsSameElements(listOf(), repo!!.findAllFiltered(f5).content)

    assertContainsSameElements(java.util.List.of(s1, s2, s3), repo!!.findAllFiltered(f6).content)
    assertContainsSameElements(java.util.List.of(s4, s5, s6), repo!!.findAllFiltered(f7).content)
    assertContainsSameElements(java.util.List.of(s7, s8, s9), repo!!.findAllFiltered(f8).content)
    assertContainsSameElements(java.util.List.of(s10), repo!!.findAllFiltered(f9).content)
    assertContainsSameElements(listOf(), repo!!.findAllFiltered(f10).content)
  }

  @Test
  fun testFindAllPerRunStateByConnectionId() {
    val p1 =
      Fixtures
        .pending()
        .transitionedAt(Fixtures.timestamp(1))
        .connectionId(Fixtures.connectionId1)
        .jobId(Fixtures.jobId6)
        .build()
    val p2 =
      Fixtures
        .pending()
        .transitionedAt(Fixtures.timestamp(2))
        .connectionId(Fixtures.connectionId1)
        .jobId(Fixtures.jobId6)
        .streamName(Fixtures.name2)
        .build()
    val p3 =
      Fixtures
        .pending()
        .transitionedAt(Fixtures.timestamp(3))
        .connectionId(Fixtures.connectionId1)
        .jobId(Fixtures.jobId6)
        .build()
    val p4 =
      Fixtures
        .pending()
        .transitionedAt(Fixtures.timestamp(4))
        .connectionId(Fixtures.connectionId2)
        .jobId(Fixtures.jobId6)
        .build()

    val r1 =
      Fixtures
        .running()
        .transitionedAt(Fixtures.timestamp(1))
        .connectionId(Fixtures.connectionId1)
        .jobId(Fixtures.jobId6)
        .build()
    val r2 =
      Fixtures
        .running()
        .transitionedAt(Fixtures.timestamp(2))
        .connectionId(Fixtures.connectionId1)
        .jobId(Fixtures.jobId6)
        .build()
    val r3 =
      Fixtures
        .running()
        .transitionedAt(Fixtures.timestamp(3))
        .connectionId(Fixtures.connectionId2)
        .jobId(Fixtures.jobId6)
        .streamName(Fixtures.name2)
        .build()
    val r4 =
      Fixtures
        .running()
        .transitionedAt(Fixtures.timestamp(4))
        .connectionId(Fixtures.connectionId2)
        .jobId(Fixtures.jobId6)
        .build()

    val rate1 =
      Fixtures
        .rateLimited()
        .transitionedAt(Fixtures.timestamp(1))
        .connectionId(Fixtures.connectionId1)
        .jobId(Fixtures.jobId6)
        .build()
    val rate2 =
      Fixtures
        .rateLimited()
        .transitionedAt(Fixtures.timestamp(2))
        .connectionId(Fixtures.connectionId1)
        .jobId(Fixtures.jobId6)
        .build()
    val rate3 =
      Fixtures
        .rateLimited()
        .transitionedAt(Fixtures.timestamp(3))
        .connectionId(Fixtures.connectionId2)
        .streamName(Fixtures.name2)
        .jobId(Fixtures.jobId6)
        .build()
    val rate4 =
      Fixtures
        .rateLimited()
        .transitionedAt(Fixtures.timestamp(4))
        .connectionId(Fixtures.connectionId2)
        .jobId(Fixtures.jobId6)
        .build()

    val c1 =
      Fixtures
        .complete()
        .transitionedAt(Fixtures.timestamp(1))
        .connectionId(Fixtures.connectionId1)
        .jobId(Fixtures.jobId6)
        .build()
    val c2 =
      Fixtures
        .complete()
        .transitionedAt(Fixtures.timestamp(2))
        .connectionId(Fixtures.connectionId2)
        .jobId(Fixtures.jobId6)
        .build()
    val c3 =
      Fixtures
        .complete()
        .transitionedAt(Fixtures.timestamp(3))
        .connectionId(Fixtures.connectionId2)
        .jobId(Fixtures.jobId6)
        .streamName(Fixtures.name2)
        .build()
    val c4 =
      Fixtures
        .complete()
        .transitionedAt(Fixtures.timestamp(4))
        .connectionId(Fixtures.connectionId2)
        .jobId(Fixtures.jobId6)
        .build()

    val if1 =
      Fixtures
        .failed()
        .transitionedAt(Fixtures.timestamp(1))
        .connectionId(Fixtures.connectionId2)
        .jobId(Fixtures.jobId6)
        .build()
    val if2 =
      Fixtures
        .failed()
        .transitionedAt(Fixtures.timestamp(2))
        .connectionId(Fixtures.connectionId2)
        .jobId(Fixtures.jobId6)
        .build()
    val if3 =
      Fixtures
        .failed()
        .transitionedAt(Fixtures.timestamp(3))
        .connectionId(Fixtures.connectionId1)
        .jobId(Fixtures.jobId6)
        .streamNamespace("test2_")
        .build()
    val if4 =
      Fixtures
        .failed()
        .transitionedAt(Fixtures.timestamp(4))
        .connectionId(Fixtures.connectionId1)
        .jobId(Fixtures.jobId6)
        .build()

    val ic1 =
      Fixtures
        .canceled()
        .transitionedAt(Fixtures.timestamp(1))
        .connectionId(Fixtures.connectionId1)
        .jobId(Fixtures.jobId6)
        .build()
    val ic2 =
      Fixtures
        .canceled()
        .transitionedAt(Fixtures.timestamp(2))
        .connectionId(Fixtures.connectionId1)
        .jobId(Fixtures.jobId6)
        .streamName(Fixtures.name2)
        .build()
    val ic3 =
      Fixtures
        .canceled()
        .transitionedAt(Fixtures.timestamp(3))
        .connectionId(Fixtures.connectionId1)
        .jobId(Fixtures.jobId6)
        .streamName(Fixtures.name2)
        .build()
    val ic4 =
      Fixtures
        .canceled()
        .transitionedAt(Fixtures.timestamp(4))
        .connectionId(Fixtures.connectionId1)
        .jobId(Fixtures.jobId6)
        .build()

    val reset1 =
      Fixtures
        .reset()
        .transitionedAt(Fixtures.timestamp(1))
        .connectionId(Fixtures.connectionId1)
        .jobId(Fixtures.jobId6)
        .build()
    val reset2 =
      Fixtures
        .reset()
        .transitionedAt(Fixtures.timestamp(2))
        .connectionId(Fixtures.connectionId1)
        .jobId(Fixtures.jobId6)
        .build()

    jooqDslContext!!.execute(
      "insert into jobs (id, scope, status, config_type) values (" + Fixtures.jobId6 + ", '" + Fixtures.connectionId1 + "', 'running', 'sync')",
    )

    repo!!.saveAll(
      java.util.List.of(
        p1,
        p2,
        p3,
        p4,
        r1,
        r2,
        r3,
        r4,
        rate1,
        rate2,
        rate3,
        rate4,
        c1,
        c2,
        c3,
        c4,
        if1,
        if2,
        if3,
        if4,
        ic1,
        ic2,
        ic3,
        ic4,
        reset1,
        reset2,
      ),
    )

    val results1 = repo!!.findAllPerRunStateByConnectionId(Fixtures.connectionId1)
    val results2 = repo!!.findAllPerRunStateByConnectionId(Fixtures.connectionId2)

    assertContainsSameElements(java.util.List.of(p2, p3, r2, rate2, c1, if3, if4, ic3, ic4, reset2), results1)
    assertContainsSameElements(java.util.List.of(p4, r3, r4, rate3, rate4, c3, c4, if2), results2)
  }

  @Test
  fun testFindLatestTerminalStatusPerStreamByConnectionId() {
    // connection 1
    val p1 =
      Fixtures
        .pending()
        .connectionId(Fixtures.connectionId1)
        .jobId(Fixtures.jobId1)
        .build()
    val c1 =
      Fixtures
        .complete()
        .connectionId(Fixtures.connectionId1)
        .attemptNumber(5)
        .jobId(Fixtures.jobId1)
        .build()

    val c2 =
      Fixtures
        .complete()
        .connectionId(Fixtures.connectionId1)
        .streamName(Fixtures.name2)
        .jobId(Fixtures.jobId2)
        .attemptNumber(3)
        .build()
    val r1 =
      Fixtures
        .reset()
        .connectionId(Fixtures.connectionId1)
        .streamName(Fixtures.name2)
        .jobId(Fixtures.jobId2)
        .attemptNumber(5)
        .build()

    val p2 =
      Fixtures
        .pending()
        .connectionId(Fixtures.connectionId1)
        .streamName(Fixtures.name3)
        .build()
    val f1 =
      Fixtures
        .failed()
        .connectionId(Fixtures.connectionId1)
        .streamName(Fixtures.name3)
        .jobId(Fixtures.jobId3)
        .attemptNumber(3)
        .build()
    val r2 =
      Fixtures
        .reset()
        .connectionId(Fixtures.connectionId1)
        .streamName(Fixtures.name3)
        .jobId(Fixtures.jobId3)
        .attemptNumber(5)
        .build()
    val p3 =
      Fixtures
        .pending()
        .connectionId(Fixtures.connectionId1)
        .streamName(Fixtures.name3)
        .build()

    // connection 2
    val p4 = Fixtures.pending().connectionId(Fixtures.connectionId2).build()

    val r3 =
      Fixtures
        .reset()
        .connectionId(Fixtures.connectionId2)
        .streamName(Fixtures.name2)
        .attemptNumber(1)
        .jobId(Fixtures.jobId4)
        .build()
    val f2 =
      Fixtures
        .failed()
        .connectionId(Fixtures.connectionId2)
        .streamName(Fixtures.name2)
        .attemptNumber(2)
        .jobId(Fixtures.jobId4)
        .build()

    val c3 =
      Fixtures
        .complete()
        .connectionId(Fixtures.connectionId2)
        .streamName(Fixtures.name3)
        .attemptNumber(1)
        .jobId(Fixtures.jobId5)
        .build()
    val f3 =
      Fixtures
        .failed()
        .connectionId(Fixtures.connectionId2)
        .streamName(Fixtures.name3)
        .attemptNumber(3)
        .jobId(Fixtures.jobId5)
        .build()

    jooqDslContext!!.execute(
      "insert into jobs (id, scope, status, config_type) values (" + Fixtures.jobId1 + ", '" + Fixtures.connectionId1 +
        "', 'succeeded', 'sync')",
    )
    jooqDslContext!!.execute(
      (
        "insert into jobs (id, scope, status, config_type) values (" + Fixtures.jobId2 + ", '" + Fixtures.connectionId1 +
          "', 'succeeded', 'refresh')"
      ),
    )
    jooqDslContext!!.execute(
      "insert into jobs (id, scope, status, config_type) values (" + Fixtures.jobId3 + ", '" + Fixtures.connectionId1 +
        "', 'succeeded', 'sync')",
    )
    jooqDslContext!!.execute(
      "insert into jobs (id, scope, status, config_type) values (" + Fixtures.jobId4 + ", '" + Fixtures.connectionId2 +
        "', 'succeeded', 'sync')",
    )
    jooqDslContext!!.execute(
      "insert into jobs (id, scope, status, config_type) values (" + Fixtures.jobId5 + ", '" + Fixtures.connectionId2 +
        "', 'succeeded', 'sync')",
    )

    repo!!.saveAll(java.util.List.of(p1, p2, p3, p4, r1, r2, r3, c1, c2, c3, f1, f2, f3))

    val results1 = repo!!.findLastAttemptsOfLastXJobsForConnection(Fixtures.connectionId1, 3)
    val results2 = repo!!.findLastAttemptsOfLastXJobsForConnection(Fixtures.connectionId2, 2)

    assertContainsSameElements(java.util.List.of(c1, r1, r2), results1)
    assertContainsSameElements(java.util.List.of(f2, f3), results2)
  }

  private object Fixtures {
    var namespace: String = "test_"

    var name1: String = "table_1"
    var name2: String = "table_2"
    var name3: String = "table_3"

    var workspaceId1: UUID = UUID.randomUUID()
    var workspaceId2: UUID = UUID.randomUUID()
    var workspaceId3: UUID = UUID.randomUUID()

    var connectionId1: UUID = UUID.randomUUID()
    var connectionId2: UUID = UUID.randomUUID()
    var connectionId3: UUID = UUID.randomUUID()

    var jobId1: Long = ThreadLocalRandom.current().nextLong()
    var jobId2: Long = ThreadLocalRandom.current().nextLong()
    var jobId3: Long = ThreadLocalRandom.current().nextLong()
    var jobId4: Long = ThreadLocalRandom.current().nextLong()
    var jobId5: Long = ThreadLocalRandom.current().nextLong()
    var jobId6: Long = ThreadLocalRandom.current().nextLong()

    // java defaults to 9 precision while postgres defaults to 6
    // this provides us with 6 decimal precision for comparison purposes
    fun now(): OffsetDateTime =
      OffsetDateTime.ofInstant(
        Instant.now().truncatedTo(ChronoUnit.MICROS),
        ZoneId.systemDefault(),
      )

    fun timestamp(ms: Long): OffsetDateTime =
      OffsetDateTime.ofInstant(
        Instant.ofEpochMilli(ms).truncatedTo(ChronoUnit.MICROS),
        ZoneId.systemDefault(),
      )

    fun status(): StreamStatusBuilder =
      StreamStatusBuilder()
        .workspaceId(workspaceId1)
        .connectionId(connectionId1)
        .jobId(jobId1)
        .attemptNumber(0)
        .streamNamespace(namespace)
        .streamName(name1)
        .jobType(JobStreamStatusJobType.sync)
        .runState(JobStreamStatusRunState.pending)
        .transitionedAt(now())

    fun statusFrom(s: StreamStatus): StreamStatusBuilder =
      StreamStatusBuilder()
        .id(s.id)
        .workspaceId(s.workspaceId)
        .connectionId(s.connectionId)
        .jobId(s.jobId)
        .attemptNumber(s.attemptNumber)
        .streamNamespace(s.streamNamespace)
        .streamName(s.streamName)
        .jobType(s.jobType)
        .runState(s.runState)
        .incompleteRunCause(s.incompleteRunCause)
        .createdAt(s.createdAt)
        .updatedAt(s.updatedAt)
        .transitionedAt(s.transitionedAt)
        .metadata(s.metadata)

    fun pending(): StreamStatusBuilder =
      status()
        .runState(JobStreamStatusRunState.pending)

    fun running(): StreamStatusBuilder =
      status()
        .runState(JobStreamStatusRunState.running)

    fun rateLimited(): StreamStatusBuilder =
      status()
        .runState(JobStreamStatusRunState.rate_limited)
        .metadata(Jsons.jsonNode(StreamStatusRateLimitedMetadataRepositoryStructure(now().toInstant().toEpochMilli())))

    fun complete(): StreamStatusBuilder =
      status()
        .runState(JobStreamStatusRunState.complete)

    fun failed(): StreamStatusBuilder =
      status()
        .runState(JobStreamStatusRunState.incomplete)
        .incompleteRunCause(JobStreamStatusIncompleteRunCause.failed)

    fun canceled(): StreamStatusBuilder =
      status()
        .runState(JobStreamStatusRunState.incomplete)
        .incompleteRunCause(JobStreamStatusIncompleteRunCause.canceled)

    fun reset(): StreamStatusBuilder =
      status()
        .jobType(JobStreamStatusJobType.reset)
        .runState(JobStreamStatusRunState.complete)

    fun filters(
      workspaceId: UUID?,
      connectionId: UUID?,
      jobId: Long?,
      streamNamespace: String?,
      streamName: String?,
      attemptNumber: Int?,
      jobType: JobStreamStatusJobType?,
      pagination: StreamStatusesRepository.Pagination?,
    ): FilterParams =
      FilterParams(
        workspaceId,
        connectionId,
        jobId,
        streamNamespace,
        streamName,
        attemptNumber,
        jobType,
        pagination,
      )
  }

  companion object {
    private const val DATA_SOURCE_NAME = "config"
    private const val DATA_SOURCES = "datasources."

    var context: ApplicationContext? = null

    var repo: StreamStatusesRepository? = null

    var jooqDslContext: DSLContext? = null

    // we run against an actual database to ensure micronaut data and jooq properly integrate
    var container: PostgreSQLContainer<*> =
      PostgreSQLContainer(DatabaseConstants.DEFAULT_DATABASE_VERSION)
        .withDatabaseName("airbyte")
        .withUsername("docker")
        .withPassword("docker")

    @BeforeAll
    @JvmStatic
    fun setup() {
      container.start()
      // set the micronaut datasource properties to match our container we started up
      context =
        ApplicationContext.run(
          PropertySource.of(
            "test",
            mapOf<String, Any>(
              DATA_SOURCES + DATA_SOURCE_NAME + ".driverClassName" to "org.postgresql.Driver",
              DATA_SOURCES + DATA_SOURCE_NAME + ".db-type" to "postgres",
              DATA_SOURCES + DATA_SOURCE_NAME + ".dialect" to "POSTGRES",
              DATA_SOURCES + DATA_SOURCE_NAME + ".url" to container.jdbcUrl,
              DATA_SOURCES + DATA_SOURCE_NAME + ".username" to container.username,
              DATA_SOURCES + DATA_SOURCE_NAME + ".password" to container.password,
            ),
          ),
        )

      // removes micronaut transactional wrapper that doesn't play nice with our non-micronaut factories
      val dataSource = (context!!.getBean(DataSource::class.java, Qualifiers.byName(DATA_SOURCE_NAME)) as DelegatingDataSource).targetDataSource
      jooqDslContext = DSLContextFactory.create(dataSource, SQLDialect.POSTGRES)
      val databaseProviders = TestDatabaseProviders(dataSource, jooqDslContext!!)

      // this line is what runs the migrations
      databaseProviders.createNewJobsDatabase()

      // so we don't have to deal with making jobs as well
      jooqDslContext!!
        .alterTable(
          Tables.STREAM_STATUSES,
        ).dropForeignKey(Keys.STREAM_STATUSES__STREAM_STATUSES_JOB_ID_FKEY.constraint())
        .execute()

      repo =
        context!!.getBean(
          StreamStatusesRepository::class.java,
        )
    }

    @AfterAll
    @JvmStatic
    fun dbDown() {
      container.close()
    }

    // Aliasing to cut down on the verbosity significantly
    private fun <T> assertContainsSameElements(
      expected: List<T>,
      actual: List<T>?,
    ) {
      org.assertj.core.api.Assertions
        .assertThat(actual)
        .containsExactlyInAnyOrderElementsOf(expected)
    }
  }
}
