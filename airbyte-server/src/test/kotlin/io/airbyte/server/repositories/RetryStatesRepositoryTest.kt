/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.repositories

import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.jobs.jooq.generated.Keys
import io.airbyte.db.instance.jobs.jooq.generated.Tables
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.airbyte.server.repositories.domain.RetryState
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
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import javax.sql.DataSource

@MicronautTest
internal class RetryStatesRepositoryTest {
  @BeforeEach
  fun truncate() {
    jooqDslContext!!.truncateTable(Tables.RETRY_STATES).cascade().execute()
  }

  @Test
  fun testInsert() {
    val s = Fixtures.state().build()

    val inserted = repo!!.save(s)

    val found = repo!!.findById(inserted.id)

    Assertions.assertTrue(found.isPresent)
    Assertions.assertEquals(inserted, found.get())
  }

  @Test
  fun testUpdateByJobId() {
    val s =
      Fixtures
        .state()
        .jobId(Fixtures.jobId2)
        .build()

    val inserted = repo!!.save(s)
    val id = inserted.id
    val found1 = repo!!.findById(id)

    val updated =
      Fixtures
        .stateFrom(inserted)
        .successiveCompleteFailures(s.successiveCompleteFailures!! + 1)
        .totalCompleteFailures(s.totalCompleteFailures!! + 1)
        .successivePartialFailures(0)
        .build()

    repo!!.updateByJobId(updated)

    val found2 = repo!!.findById(id)

    Assertions.assertTrue(found1.isPresent)
    Assertions.assertEquals(s, found1.get())

    Assertions.assertTrue(found2.isPresent)
    Assertions.assertEquals(updated, found2.get())
  }

  @Test
  fun findByJobId() {
    val s1 =
      Fixtures
        .state()
        .jobId(Fixtures.jobId2)
        .connectionId(Fixtures.connectionId2)
        .totalCompleteFailures(0)
        .build()

    val s2 =
      Fixtures
        .stateFrom(s1)
        .jobId(Fixtures.jobId3)
        .build()

    val s3 =
      Fixtures
        .stateFrom(s2)
        .jobId(Fixtures.jobId1)
        .build()

    repo!!.save(s1)
    repo!!.save(s2)
    repo!!.save(s3)

    val found1 = repo!!.findByJobId(Fixtures.jobId2)
    val found2 = repo!!.findByJobId(Fixtures.jobId3)
    val found3 = repo!!.findByJobId(Fixtures.jobId1)

    Assertions.assertNotNull(found1)
    Assertions.assertEquals(s1, found1)

    Assertions.assertNotNull(found2)
    Assertions.assertEquals(s2, found2)

    Assertions.assertNotNull(found3)
    Assertions.assertEquals(s3, found3)
  }

  @Test
  fun testExistsByJobId() {
    val s =
      Fixtures
        .state()
        .jobId(Fixtures.jobId3)
        .build()

    repo!!.save(s)

    val exists1 = repo!!.existsByJobId(Fixtures.jobId3)
    val exists2 = repo!!.existsByJobId(Fixtures.jobId2)

    Assertions.assertTrue(exists1)
    Assertions.assertFalse(exists2)
  }

  @Test
  fun testCreateOrUpdateByJobIdUpdate() {
    val s =
      Fixtures
        .state()
        .jobId(Fixtures.jobId2)
        .build()

    val inserted = repo!!.save(s)
    val id = inserted.id
    val found1 = repo!!.findById(id)

    val updated =
      Fixtures
        .stateFrom(inserted)
        .successiveCompleteFailures(s.successiveCompleteFailures!! + 1)
        .totalCompleteFailures(s.totalCompleteFailures!! + 1)
        .successivePartialFailures(0)
        .build()

    repo!!.createOrUpdateByJobId(Fixtures.jobId2, updated)

    val found2 = repo!!.findById(id)

    Assertions.assertTrue(found1.isPresent)
    Assertions.assertEquals(s, found1.get())

    Assertions.assertTrue(found2.isPresent)
    Assertions.assertEquals(updated, found2.get())
  }

  @Test
  fun testCreateOrUpdateByJobIdCreate() {
    val s =
      Fixtures
        .state()
        .jobId(Fixtures.jobId4)
        .build()

    repo!!.createOrUpdateByJobId(Fixtures.jobId4, s)

    val found1 = repo!!.findByJobId(Fixtures.jobId4)

    Assertions.assertNotNull(found1)
    Assertions.assertEquals(s, found1)
  }

  private object Fixtures {
    var connectionId1: UUID = UUID.randomUUID()
    var connectionId2: UUID = UUID.randomUUID()

    var jobId1: Long = ThreadLocalRandom.current().nextLong()
    var jobId2: Long = ThreadLocalRandom.current().nextLong()
    var jobId3: Long = ThreadLocalRandom.current().nextLong()
    var jobId4: Long = ThreadLocalRandom.current().nextLong()

    fun state(): RetryState.RetryStateBuilder =
      RetryState
        .RetryStateBuilder()
        .connectionId(connectionId1)
        .jobId(jobId1)
        .successiveCompleteFailures(0)
        .totalCompleteFailures(1)
        .successivePartialFailures(2)
        .totalPartialFailures(2)

    fun stateFrom(s: RetryState): RetryState.RetryStateBuilder =
      RetryState
        .RetryStateBuilder()
        .connectionId(s.connectionId)
        .jobId(s.jobId)
        .successiveCompleteFailures(s.successiveCompleteFailures)
        .totalCompleteFailures(s.totalCompleteFailures)
        .successivePartialFailures(s.successivePartialFailures)
        .totalPartialFailures(s.totalPartialFailures)
        .updatedAt(s.updatedAt)
        .createdAt(s.createdAt)
  }

  companion object {
    private const val DATA_SOURCE_NAME = "config"
    private const val DATA_SOURCES = "datasources."

    var context: ApplicationContext? = null

    var repo: RetryStatesRepository? = null

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
              "${DATA_SOURCES}${DATA_SOURCE_NAME}.driverClassName" to "org.postgresql.Driver",
              "${DATA_SOURCES}${DATA_SOURCE_NAME}.db-type" to "postgres",
              "${DATA_SOURCES}${DATA_SOURCE_NAME}.dialect" to "POSTGRES",
              "${DATA_SOURCES}${DATA_SOURCE_NAME}.url" to container.jdbcUrl,
              "${DATA_SOURCES}${DATA_SOURCE_NAME}.username" to container.username,
              "${DATA_SOURCES}${DATA_SOURCE_NAME}.password" to container.password,
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
      jooqDslContext!!.alterTable(Tables.RETRY_STATES).dropForeignKey(Keys.RETRY_STATES__RETRY_STATES_JOB_ID_FKEY.constraint()).execute()

      repo =
        context!!.getBean(
          RetryStatesRepository::class.java,
        )
    }

    @AfterAll
    @JvmStatic
    fun dbDown() {
      container.close()
    }
  }
}
