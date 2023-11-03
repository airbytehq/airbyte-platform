package io.airbyte.workload.repository

import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.instance.configs.jooq.generated.enums.WorkloadStatus
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadLabel
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.PropertySource
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.transaction.jdbc.DelegatingDataSource
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import javax.sql.DataSource

@MicronautTest
internal class WorkloadRepositoryTest {
  private val workloadId = "test"

  companion object {
    private lateinit var context: ApplicationContext
    lateinit var workloadRepo: WorkloadRepository
    lateinit var workloadLabelRepo: WorkloadLabelRepository
    private lateinit var jooqDslContext: DSLContext

    // we run against an actual database to ensure micronaut data and jooq properly integrate
    private val container: PostgreSQLContainer<*> =
      PostgreSQLContainer("postgres:13-alpine")
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
            mapOf(
              "datasources.default.driverClassName" to "org.postgresql.Driver",
              "datasources.default.db-type" to "postgres",
              "datasources.default.dialect" to "POSTGRES",
              "datasources.default.url" to container.jdbcUrl,
              "datasources.default.username" to container.username,
              "datasources.default.password" to container.password,
            ),
          ),
        )

      // removes micronaut transactional wrapper that doesn't play nice with our non-micronaut factories
      val dataSource = (context.getBean(DataSource::class.java) as DelegatingDataSource).targetDataSource
      jooqDslContext = DSLContextFactory.create(dataSource, SQLDialect.POSTGRES)
      val databaseProviders = TestDatabaseProviders(dataSource, jooqDslContext)

      // this line is what runs the migrations
      databaseProviders.createNewConfigsDatabase()
      workloadRepo = context.getBean(WorkloadRepository::class.java)
      workloadLabelRepo = context.getBean(WorkloadLabelRepository::class.java)
    }

    @AfterAll
    @JvmStatic
    fun dbDown() {
      container.close()
    }

    private fun sortedSearch(
      dataplaneIds: List<String>?,
      statuses: List<WorkloadStatus>?,
      updatedBefore: OffsetDateTime?,
    ): MutableList<Workload> {
      val workloads = workloadRepo.search(dataplaneIds, statuses, updatedBefore).toMutableList()
      workloads.sortWith(Comparator.comparing(Workload::id))
      return workloads
    }
  }

  @AfterEach
  fun cleanDb() {
    workloadLabelRepo.deleteAll()
    workloadRepo.deleteAll()
  }

  @Test
  fun testInsert() {
    val label1 =
      WorkloadLabel(
        null,
        "key1",
        "value1",
        null,
      )
    val label2 =
      WorkloadLabel(
        null,
        "key2",
        "value2",
        null,
      )
    val objects = ArrayList<WorkloadLabel>()
    objects.add(label1)
    objects.add(label2)
    val workload =
      Workload(
        workloadId,
        null,
        WorkloadStatus.pending,
        null,
        null,
        null,
        objects,
      )
    workloadRepo.save(workload)
    val persistedWorkload = workloadRepo.findById(workloadId)
    Assertions.assertTrue(persistedWorkload.isPresent)
    Assertions.assertNull(persistedWorkload.get().dataplaneId)
    Assertions.assertEquals(WorkloadStatus.pending, persistedWorkload.get().status)
    Assertions.assertNotNull(persistedWorkload.get().createdAt)
    Assertions.assertNotNull(persistedWorkload.get().updatedAt)
    Assertions.assertNull(persistedWorkload.get().lastHeartbeatAt)
    Assertions.assertEquals(2, persistedWorkload.get().workloadLabels!!.size)
    val workloadLabels = persistedWorkload.get().workloadLabels!!.toMutableList()
    workloadLabels.sortWith(Comparator.comparing(WorkloadLabel::key))
    Assertions.assertEquals("key1", workloadLabels[0].key)
    Assertions.assertEquals("value1", workloadLabels[0].value)
    Assertions.assertNotNull(workloadLabels[0].id)
    Assertions.assertEquals("key2", workloadLabels[1].key)
    Assertions.assertEquals("value2", workloadLabels[1].value)
    Assertions.assertNotNull(workloadLabels[1].id)
  }

  @Test
  fun testHeartbeatUpdate() {
    val workload =
      Workload(
        workloadId,
        null,
        WorkloadStatus.pending,
        null,
        null,
        null,
        null,
      )
    workloadRepo.save(workload)
    val now = OffsetDateTime.now()
    workloadRepo.update(workloadId, WorkloadStatus.running, now)
    var persistedWorkload = workloadRepo.findById(workloadId)
    Assertions.assertTrue(persistedWorkload.isPresent)
    // Using .toEpochSecond() here because of dagger, it is passing locally but there is nano second errors on dagger
    Assertions.assertEquals(now.toEpochSecond(), persistedWorkload.get().lastHeartbeatAt?.toEpochSecond())
    Assertions.assertEquals(WorkloadStatus.running, persistedWorkload.get().status)
    val nowPlusOneMinute = now.plus(1, ChronoUnit.MINUTES)
    workloadRepo.update(workloadId, WorkloadStatus.running, nowPlusOneMinute)
    persistedWorkload = workloadRepo.findById(workloadId)
    Assertions.assertTrue(persistedWorkload.isPresent)
    Assertions.assertEquals(nowPlusOneMinute.toEpochSecond(), persistedWorkload.get().lastHeartbeatAt?.toEpochSecond())
  }

  @Test
  fun testSearch() {
    val workload1 =
      Workload(
        "workload1",
        "dataplane1",
        WorkloadStatus.failure,
        null,
        null,
        null,
        null,
      )
    val workload2 =
      Workload(
        "workload2",
        "dataplane2",
        WorkloadStatus.success,
        null,
        null,
        null,
        null,
      )
    workloadRepo.save(workload1)
    workloadRepo.save(workload2)
    val now = OffsetDateTime.now()
    val resulSearch1 = sortedSearch(null, null, now.plusDays(1))
    Assertions.assertEquals(2, resulSearch1.size)
    Assertions.assertEquals("workload1", resulSearch1[0].id)
    Assertions.assertEquals("workload2", resulSearch1[1].id)
    val resultSearch2 = sortedSearch(null, java.util.List.of(WorkloadStatus.failure, WorkloadStatus.success), null)
    Assertions.assertEquals(2, resultSearch2.size)
    Assertions.assertEquals("workload1", resultSearch2[0].id)
    Assertions.assertEquals("workload2", resultSearch2[1].id)
    val resultSearch3 = sortedSearch(listOf("dataplane1", "dataplane2"), null, null)
    Assertions.assertEquals(2, resultSearch3.size)
    Assertions.assertEquals("workload1", resultSearch3[0].id)
    Assertions.assertEquals("workload2", resultSearch3[1].id)
    val resultSearch4 =
      sortedSearch(
        listOf("dataplane1", "dataplane2"),
        java.util.List.of(WorkloadStatus.failure, WorkloadStatus.success),
        now.plusDays(1),
      )
    Assertions.assertEquals(2, resultSearch4.size)
    Assertions.assertEquals("workload1", resultSearch4[0].id)
    Assertions.assertEquals("workload2", resultSearch4[1].id)
    val resultSearch5 = sortedSearch(null, java.util.List.of(WorkloadStatus.failure), now.plusDays(1))
    Assertions.assertEquals(1, resultSearch5.size)
    Assertions.assertEquals("workload1", resultSearch5[0].id)
    val resultSearch6 = sortedSearch(listOf("dataplane1"), null, now.plusDays(1))
    Assertions.assertEquals(1, resultSearch6.size)
    Assertions.assertEquals("workload1", resultSearch6[0].id)
    val resultSearch7 = sortedSearch(listOf("dataplane1", "dataplane2"), java.util.List.of(WorkloadStatus.failure), null)
    Assertions.assertEquals(1, resultSearch7.size)
    Assertions.assertEquals("workload1", resultSearch7[0].id)
    val resultSearch8 = sortedSearch(listOf("dataplane1"), java.util.List.of(WorkloadStatus.failure, WorkloadStatus.success), now.plusDays(1))
    Assertions.assertEquals(1, resultSearch8.size)
    Assertions.assertEquals("workload1", resultSearch8[0].id)
    val resultSearch9 =
      sortedSearch(
        listOf("dataplane1", "dataplane2"),
        java.util.List.of(WorkloadStatus.failure, WorkloadStatus.success),
        now.minusDays(1),
      )
    Assertions.assertEquals(0, resultSearch9.size)
    val resultSearch10 = sortedSearch(listOf("dataplane1", "dataplane2"), java.util.List.of(WorkloadStatus.claimed), now.plusDays(1))
    Assertions.assertEquals(0, resultSearch10.size)
    val resultSearch11 = sortedSearch(listOf("fakeDataplane"), java.util.List.of(WorkloadStatus.failure, WorkloadStatus.success), now.plusDays(1))
    Assertions.assertEquals(0, resultSearch11.size)
  }
}
