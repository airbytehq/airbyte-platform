package io.airbyte.workload.repository

import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.airbyte.workload.repository.WorkloadRepositoryTest.Fixtures.WORKLOAD_ID
import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadLabel
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.repository.domain.WorkloadType
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.PropertySource
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.transaction.jdbc.DelegatingDataSource
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import javax.sql.DataSource

@MicronautTest
internal class WorkloadRepositoryTest {
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

    private fun sortedSearchByTypeStatusCreatedDate(
      dataplaneIds: List<String>?,
      statuses: List<WorkloadStatus>?,
      types: List<WorkloadType>?,
      createdBefore: OffsetDateTime?,
    ): MutableList<Workload> {
      val workloads = workloadRepo.searchByTypeStatusAndCreationDate(dataplaneIds, statuses, types, createdBefore).toMutableList()
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
  fun `test db insertion`() {
    val label1 =
      WorkloadLabel(
        id = null,
        key = "key1",
        value = "value1",
        workload = null,
      )
    val label2 =
      WorkloadLabel(
        id = null,
        key = "key2",
        value = "value2",
        workload = null,
      )
    val labels = ArrayList<WorkloadLabel>()
    labels.add(label1)
    labels.add(label2)
    val workload =
      Fixtures.workload(
        id = WORKLOAD_ID,
        dataplaneId = null,
        status = WorkloadStatus.PENDING,
        workloadLabels = labels,
      )
    workloadRepo.save(workload)
    val persistedWorkload = workloadRepo.findById(WORKLOAD_ID)
    assertTrue(persistedWorkload.isPresent)
    assertNull(persistedWorkload.get().dataplaneId)
    assertEquals(WorkloadStatus.PENDING, persistedWorkload.get().status)
    assertNotNull(persistedWorkload.get().createdAt)
    assertNotNull(persistedWorkload.get().updatedAt)
    assertNull(persistedWorkload.get().lastHeartbeatAt)
    assertEquals(2, persistedWorkload.get().workloadLabels!!.size)

    val workloadLabels = persistedWorkload.get().workloadLabels!!.toMutableList()
    workloadLabels.sortWith(Comparator.comparing(WorkloadLabel::key))
    assertEquals("key1", workloadLabels[0].key)
    assertEquals("value1", workloadLabels[0].value)
    assertNotNull(workloadLabels[0].id)
    assertEquals("key2", workloadLabels[1].key)
    assertEquals("value2", workloadLabels[1].value)
    assertNotNull(workloadLabels[1].id)
  }

  @Test
  fun `test status update`() {
    val workload =
      Fixtures.workload(
        id = WORKLOAD_ID,
        dataplaneId = null,
        status = WorkloadStatus.PENDING,
        geography = "US",
      )
    workloadRepo.save(workload)
    workloadRepo.update(WORKLOAD_ID, WorkloadStatus.RUNNING)
    var persistedWorkload = workloadRepo.findById(WORKLOAD_ID)
    assertTrue(persistedWorkload.isPresent)
    assertEquals(WorkloadStatus.RUNNING, persistedWorkload.get().status)

    workloadRepo.update(WORKLOAD_ID, WorkloadStatus.FAILURE)
    persistedWorkload = workloadRepo.findById(WORKLOAD_ID)
    assertTrue(persistedWorkload.isPresent)
    assertEquals(WorkloadStatus.FAILURE, persistedWorkload.get().status)
  }

  @Test
  fun `test heartbeat update`() {
    val workload =
      Fixtures.workload(
        id = WORKLOAD_ID,
        dataplaneId = null,
        status = WorkloadStatus.PENDING,
        geography = "US",
      )
    workloadRepo.save(workload)
    val now = OffsetDateTime.now()
    workloadRepo.update(WORKLOAD_ID, WorkloadStatus.RUNNING, now)
    var persistedWorkload = workloadRepo.findById(WORKLOAD_ID)
    assertTrue(persistedWorkload.isPresent)
    // Using .toEpochSecond() here because of dagger, it is passing locally but there is nano second errors on dagger
    assertEquals(now.toEpochSecond(), persistedWorkload.get().lastHeartbeatAt?.toEpochSecond())
    assertEquals(WorkloadStatus.RUNNING, persistedWorkload.get().status)

    val nowPlusOneMinute = now.plus(1, ChronoUnit.MINUTES)
    workloadRepo.update(WORKLOAD_ID, WorkloadStatus.RUNNING, nowPlusOneMinute)
    persistedWorkload = workloadRepo.findById(WORKLOAD_ID)
    assertTrue(persistedWorkload.isPresent)
    assertEquals(nowPlusOneMinute.toEpochSecond(), persistedWorkload.get().lastHeartbeatAt?.toEpochSecond())
  }

  @Test
  fun `test dataplane update`() {
    val workload =
      Fixtures.workload(
        id = WORKLOAD_ID,
        dataplaneId = null,
        status = WorkloadStatus.PENDING,
        geography = "AUTO",
      )
    workloadRepo.save(workload)
    workloadRepo.update(WORKLOAD_ID, "dataplaneId1", WorkloadStatus.RUNNING)
    var persistedWorkload = workloadRepo.findById(WORKLOAD_ID)
    assertTrue(persistedWorkload.isPresent)
    assertEquals("dataplaneId1", persistedWorkload.get().dataplaneId)

    workloadRepo.update(WORKLOAD_ID, "dataplaneId2", WorkloadStatus.RUNNING)
    persistedWorkload = workloadRepo.findById(WORKLOAD_ID)
    assertTrue(persistedWorkload.isPresent)
    assertEquals("dataplaneId2", persistedWorkload.get().dataplaneId)
  }

  @Test
  fun `test search`() {
    val workload1 =
      Fixtures.workload(
        id = "workload1",
        dataplaneId = "dataplane1",
        status = WorkloadStatus.FAILURE,
        geography = "AUTO",
      )
    val workload2 =
      Fixtures.workload(
        id = "workload2",
        dataplaneId = "dataplane2",
        status = WorkloadStatus.SUCCESS,
        geography = "US",
      )
    workloadRepo.save(workload1)
    workloadRepo.save(workload2)
    val now = OffsetDateTime.now()
    val resulSearch1 = sortedSearch(null, null, now.plusDays(1))
    assertEquals(2, resulSearch1.size)
    assertEquals("workload1", resulSearch1[0].id)
    assertEquals("workload2", resulSearch1[1].id)

    val resultSearch2 = sortedSearch(null, listOf(WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS), null)
    assertEquals(2, resultSearch2.size)
    assertEquals("workload1", resultSearch2[0].id)
    assertEquals("workload2", resultSearch2[1].id)

    val resultSearch3 = sortedSearch(listOf("dataplane1", "dataplane2"), null, null)
    assertEquals(2, resultSearch3.size)
    assertEquals("workload1", resultSearch3[0].id)
    assertEquals("workload2", resultSearch3[1].id)

    val resultSearch4 =
      sortedSearch(
        listOf("dataplane1", "dataplane2"),
        listOf(WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS),
        now.plusDays(1),
      )
    assertEquals(2, resultSearch4.size)
    assertEquals("workload1", resultSearch4[0].id)
    assertEquals("workload2", resultSearch4[1].id)

    val resultSearch5 = sortedSearch(null, listOf(WorkloadStatus.FAILURE), now.plusDays(1))
    assertEquals(1, resultSearch5.size)
    assertEquals("workload1", resultSearch5[0].id)

    val resultSearch6 = sortedSearch(listOf("dataplane1"), null, now.plusDays(1))
    assertEquals(1, resultSearch6.size)
    assertEquals("workload1", resultSearch6[0].id)

    val resultSearch7 = sortedSearch(listOf("dataplane1", "dataplane2"), listOf(WorkloadStatus.FAILURE), null)
    assertEquals(1, resultSearch7.size)
    assertEquals("workload1", resultSearch7[0].id)

    val resultSearch8 = sortedSearch(listOf("dataplane1"), listOf(WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS), now.plusDays(1))
    assertEquals(1, resultSearch8.size)
    assertEquals("workload1", resultSearch8[0].id)

    val resultSearch9 =
      sortedSearch(
        listOf("dataplane1", "dataplane2"),
        listOf(WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS),
        now.minusDays(1),
      )
    assertEquals(0, resultSearch9.size)

    val resultSearch10 = sortedSearch(listOf("dataplane1", "dataplane2"), listOf(WorkloadStatus.CLAIMED), now.plusDays(1))
    assertEquals(0, resultSearch10.size)

    val resultSearch11 = sortedSearch(listOf("fakeDataplane"), listOf(WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS), now.plusDays(1))
    assertEquals(0, resultSearch11.size)
  }

  @Test
  fun `test search by type status and creation date`() {
    val workload1 =
      Fixtures.workload(
        id = "workload1",
        dataplaneId = "dataplane1",
        status = WorkloadStatus.RUNNING,
        geography = "AUTO",
        type = WorkloadType.CHECK,
      )
    val workload2 =
      Fixtures.workload(
        id = "workload2",
        dataplaneId = "dataplane2",
        status = WorkloadStatus.SUCCESS,
        geography = "US",
        type = WorkloadType.SYNC,
      )
    workloadRepo.save(workload1)
    workloadRepo.save(workload2)
    val now = OffsetDateTime.now()
    var resultSearch = sortedSearchByTypeStatusCreatedDate(null, null, null, now.plusDays(1))
    assertEquals(2, resultSearch.size)
    assertEquals("workload1", resultSearch[0].id)
    assertEquals("workload2", resultSearch[1].id)

    resultSearch = sortedSearchByTypeStatusCreatedDate(null, null, listOf(WorkloadType.CHECK), now.plusDays(1))
    assertEquals(1, resultSearch.size)
    assertEquals("workload1", resultSearch[0].id)

    resultSearch = sortedSearchByTypeStatusCreatedDate(null, null, listOf(WorkloadType.SPEC), now.plusDays(1))
    assertEquals(0, resultSearch.size)

    resultSearch = sortedSearchByTypeStatusCreatedDate(null, listOf(WorkloadStatus.RUNNING), null, now.plusDays(1))
    assertEquals(1, resultSearch.size)
    assertEquals("workload1", resultSearch[0].id)

    resultSearch = sortedSearchByTypeStatusCreatedDate(null, listOf(WorkloadStatus.CANCELLED), null, now.plusDays(1))
    assertEquals(0, resultSearch.size)

    resultSearch = sortedSearchByTypeStatusCreatedDate(null, null, null, now.minusDays(1))
    assertEquals(0, resultSearch.size)

    resultSearch = sortedSearchByTypeStatusCreatedDate(null, listOf(WorkloadStatus.RUNNING), listOf(WorkloadType.SYNC), now.plusDays(1))
    assertEquals(0, resultSearch.size)
  }

  object Fixtures {
    const val WORKLOAD_ID = "test"

    fun workload(
      id: String = WORKLOAD_ID,
      dataplaneId: String? = null,
      status: WorkloadStatus = WorkloadStatus.PENDING,
      workloadLabels: List<WorkloadLabel>? = listOf(),
      inputPayload: String = "",
      logPath: String = "/",
      geography: String = "US",
      mutexKey: String = "",
      type: WorkloadType = WorkloadType.SYNC,
    ): Workload =
      Workload(
        id = id,
        dataplaneId = dataplaneId,
        status = status,
        workloadLabels = workloadLabels,
        inputPayload = inputPayload,
        logPath = logPath,
        geography = geography,
        mutexKey = mutexKey,
        type = type,
      )
  }
}
