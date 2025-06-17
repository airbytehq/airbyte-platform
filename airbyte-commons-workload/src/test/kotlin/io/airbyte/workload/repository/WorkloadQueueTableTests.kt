/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.repository

import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadLabel
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.repository.domain.WorkloadType
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.PropertySource
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.testcontainers.containers.PostgreSQLContainer
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.util.UUID
import javax.sql.DataSource

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WorkloadQueueTableTests {
  @AfterEach
  fun cleanDb() {
    workloadLabelRepo.deleteAll()
    workloadQueueRepo.deleteAll()
    workloadRepo.deleteAll()
  }

  @ParameterizedTest
  @MethodSource("pendingWorkloadMatrix")
  fun `poll workload gets enqueued workloads for provided params`(
    group: String,
    priority: Int,
    workloads: List<Workload>,
  ) {
    val seeds =
      listOf(
        Fixtures.workload(),
        Fixtures.workload(),
        Fixtures.workload(),
      )
    seeds.forEach {
      workloadRepo.save(it)
      workloadQueueRepo.enqueueWorkload(it.dataplaneGroup!!, it.priority!!, it.id)
    }
    workloads.forEach {
      workloadRepo.save(it)
      workloadQueueRepo.enqueueWorkload(group, priority, it.id)
    }

    val result = workloadQueueRepo.pollWorkloadQueue(group, priority, quantity = 10)

    assertWorkloadsEqual(workloads, result)
  }

  @ParameterizedTest
  @MethodSource("pendingWorkloadMatrix")
  fun `subsequent calls to poll workload do not return duplicate workloads within poll deadline`(
    group: String,
    priority: Int,
    workloads: List<Workload>,
  ) {
    workloads.forEach {
      workloadRepo.save(it)
      workloadQueueRepo.enqueueWorkload(group, priority, it.id)
    }

    val result1 = workloadQueueRepo.pollWorkloadQueue(group, priority, quantity = 10)
    assertWorkloadsEqual(workloads, result1, "workloads are delivered as expected")

    val result2 = workloadQueueRepo.pollWorkloadQueue(group, priority, quantity = 10)
    assertWorkloadsEqual(listOf(), result2, "workloads are not re-delivered")

    val freshlyEnqueued =
      listOf(
        Fixtures.workload(dataplaneGroup = group, priority = priority),
        Fixtures.workload(dataplaneGroup = group, priority = priority),
      )

    freshlyEnqueued.forEach {
      workloadRepo.save(it)
      workloadQueueRepo.enqueueWorkload(group, priority, it.id)
    }

    val result3 = workloadQueueRepo.pollWorkloadQueue(group, priority, quantity = 10)
    assertWorkloadsEqual(freshlyEnqueued, result3, "new workloads get delivered as expected")
  }

  @ParameterizedTest
  @MethodSource("pendingWorkloadMatrix")
  fun `poll workload will re-deliver (return previously seen) workloads once poll deadline expires`(
    group: String,
    priority: Int,
    workloads: List<Workload>,
  ) {
    workloads.forEach {
      workloadRepo.save(it)
      workloadQueueRepo.enqueueWorkload(group, priority, it.id)
    }
    val result1 = workloadQueueRepo.pollWorkloadQueue(group, priority, quantity = 10, redeliveryWindowSecs = 0)
    assertWorkloadsEqual(workloads, result1, "workloads are delivered as expected")

    val result2 = workloadQueueRepo.pollWorkloadQueue(group, priority, quantity = 10, redeliveryWindowSecs = 60)
    assertWorkloadsEqual(workloads, result2, "workloads are re-delivered since we passed an empty window in the first poll")

    val result3 = workloadQueueRepo.pollWorkloadQueue(group, priority, quantity = 10)
    assertWorkloadsEqual(listOf(), result3, "workloads are not re-delivered since we passed a non 0 window")
  }

  @ParameterizedTest
  @MethodSource("pendingWorkloadMatrix")
  fun `an acked workload is no longer considered enqueued`(
    group: String,
    priority: Int,
    workloads: List<Workload>,
  ) {
    workloads.forEach {
      workloadRepo.save(it)
      workloadQueueRepo.enqueueWorkload(group, priority, it.id)
    }
    // split the workloads in 2
    var index = 0
    val (toAck, unAcked) = workloads.partition { index++ % 2 == 0 }
    // ack half of them
    toAck.forEach {
      workloadQueueRepo.ackWorkloadQueueItem(it.id)
    }

    val result = workloadQueueRepo.pollWorkloadQueue(group, priority, quantity = 10)
    assertWorkloadsEqual(unAcked, result, "only un-acked workloads are delivered")
  }

  private fun pendingWorkloadMatrix(): List<Arguments> =
    listOf(
      Arguments.of(
        "group-1",
        0,
        listOf(
          Fixtures.workload(
            dataplaneGroup = "group-1",
            priority = 0,
            workloadLabels = Fixtures.labelList1(),
          ),
          Fixtures.workload(
            dataplaneGroup = "group-1",
            priority = 0,
            workloadLabels = Fixtures.labelList2(),
          ),
          Fixtures.workload(
            dataplaneGroup = "group-1",
            priority = 0,
            workloadLabels = Fixtures.labelList3(),
          ),
        ),
      ),
      Arguments.of(
        "group-2",
        1,
        listOf(
          Fixtures.workload(
            dataplaneGroup = "group-2",
            priority = 1,
            workloadLabels = Fixtures.labelList4(),
          ),
          Fixtures.workload(
            dataplaneGroup = "group-2",
            priority = 1,
            workloadLabels = Fixtures.labelList2(),
          ),
        ),
      ),
      Arguments.of(
        "group-3",
        0,
        listOf(
          Fixtures.workload(
            dataplaneGroup = "group-3",
            priority = 0,
            workloadLabels = Fixtures.labelList3(),
          ),
        ),
      ),
      Arguments.of(
        "group-1",
        1,
        listOf(
          Fixtures.workload(
            dataplaneGroup = "group-1",
            priority = 1,
            workloadLabels = Fixtures.labelList1(),
          ),
          Fixtures.workload(
            dataplaneGroup = "group-1",
            priority = 1,
            workloadLabels = Fixtures.labelList4(),
          ),
        ),
      ),
    )

  @ParameterizedTest
  @MethodSource("deletionLimitScenarios")
  fun `cleanUpAckedEntries removes entries up to the specified limit`(
    deletionLimit: Int,
    initialEntries: List<Workload>,
  ) {
    val dataplaneGroup = "${UUID.randomUUID()}_group-1"
    val priority = 0

    initialEntries.forEach {
      workloadRepo.save(it)
      workloadQueueRepo.enqueueWorkload(dataplaneGroup, priority, it.id)
      workloadQueueRepo.ackWorkloadQueueItem(it.id)
    }

    val entriesToOverrideAckedDate = workloadQueueRepo.findByDataplaneGroup(dataplaneGroup)

    entriesToOverrideAckedDate.forEach {
      it.ackedAt = OffsetDateTime.now().minusWeeks(1).minusMinutes(5)
      workloadQueueRepo.update(it)
    }
    workloadQueueRepo.cleanUpAckedEntries(deletionLimit)

    val remainingEntries = workloadQueueRepo.findByDataplaneGroup(dataplaneGroup)
    Assertions.assertEquals(0.coerceAtLeast(initialEntries.size - deletionLimit), remainingEntries.size)
  }

  @Test
  fun `cleanUpAckedEntries doesn't remove non ack entries`() {
    val dataplaneGroup = "group-1"
    val priority = 0

    val initialEntries =
      listOf(
        Fixtures.workload(
          dataplaneGroup = dataplaneGroup,
          priority = priority,
          workloadLabels = Fixtures.labelList1(),
        ),
        Fixtures.workload(
          dataplaneGroup = dataplaneGroup,
          priority = priority,
          workloadLabels = Fixtures.labelList2(),
        ),
        Fixtures.workload(
          dataplaneGroup = dataplaneGroup,
          priority = priority,
          workloadLabels = Fixtures.labelList3(),
        ),
      )
    initialEntries.forEach {
      workloadRepo.save(it)
      workloadQueueRepo.enqueueWorkload(it.dataplaneGroup!!, it.priority!!, it.id)
    }

    workloadQueueRepo.cleanUpAckedEntries(1000)

    val remainingEntries = workloadQueueRepo.pollWorkloadQueue(dataplaneGroup, priority, quantity = 10)
    Assertions.assertEquals(initialEntries.size, remainingEntries.size)
  }

  private fun deletionLimitScenarios(): List<Arguments> =
    listOf(
      Arguments.of(
        4,
        listOf(
          Fixtures.workload(
            dataplaneGroup = "group-1",
            priority = 0,
            workloadLabels = Fixtures.labelList1(),
          ),
          Fixtures.workload(
            dataplaneGroup = "group-1",
            priority = 0,
            workloadLabels = Fixtures.labelList2(),
          ),
          Fixtures.workload(
            dataplaneGroup = "group-1",
            priority = 0,
            workloadLabels = Fixtures.labelList3(),
          ),
        ),
      ),
      Arguments.of(
        1,
        listOf(
          Fixtures.workload(
            dataplaneGroup = "group-1",
            priority = 0,
            workloadLabels = Fixtures.labelList4(),
          ),
          Fixtures.workload(
            dataplaneGroup = "group-1",
            priority = 0,
            workloadLabels = Fixtures.labelList2(),
          ),
        ),
      ),
      Arguments.of(
        0,
        listOf(
          Fixtures.workload(
            dataplaneGroup = "group-1",
            priority = 0,
            workloadLabels = Fixtures.labelList3(),
          ),
        ),
      ),
    )

  companion object {
    private lateinit var context: ApplicationContext
    lateinit var workloadRepo: WorkloadRepository
    lateinit var workloadLabelRepo: WorkloadLabelRepository
    lateinit var workloadQueueRepo: WorkloadQueueRepository
    private lateinit var jooqDslContext: DSLContext

    // we run against an actual database to ensure micronaut data and jooq properly integrate
    private val container: PostgreSQLContainer<*> =
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
            mapOf(
              "datasources.config.driverClassName" to "org.postgresql.Driver",
              "datasources.config.db-type" to "postgres",
              "datasources.config.dialect" to "POSTGRES",
              "datasources.config.url" to container.jdbcUrl,
              "datasources.config.username" to container.username,
              "datasources.config.password" to container.password,
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
      workloadQueueRepo = context.getBean(WorkloadQueueRepository::class.java)
    }

    @AfterAll
    @JvmStatic
    fun dbDown() {
      container.close()
    }

    /*
     * Utility for keeping timestamps at a consistent precision for test comparisons. Our test container sql truncates to millis.
     */
    private fun OffsetDateTime.truncateToTestPrecision(): OffsetDateTime = this.truncatedTo(ChronoUnit.MILLIS)

    /**
     * Compares 2 lists of workloads ignoring order and normalizing timestamp precision.
     */
    fun assertWorkloadsEqual(
      a: List<Workload>,
      b: List<Workload>,
      message: String? = null,
    ) {
      val aGroomed =
        a
          .map {
            it.createdAt = it.createdAt?.truncateToTestPrecision()
            it.updatedAt = it.updatedAt?.truncateToTestPrecision()
            it.deadline = it.deadline?.truncateToTestPrecision()
            it.workloadLabels = it.workloadLabels ?: listOf()
            it
          }.sortedBy { it.id }
      val bGroomed =
        b
          .map {
            it.createdAt = it.createdAt?.truncateToTestPrecision()
            it.updatedAt = it.updatedAt?.truncateToTestPrecision()
            it.deadline = it.deadline?.truncateToTestPrecision()
            it.workloadLabels = it.workloadLabels ?: listOf()
            it
          }.sortedBy { it.id }

      Assertions.assertEquals(aGroomed, bGroomed, message)
    }
  }

  object Fixtures {
    fun newWorkloadId() = "${UUID.randomUUID()}_test"

    val label1 = { WorkloadLabel(key = "key-1", value = "value-1") }
    val label2 = { WorkloadLabel(key = "key-2", value = "value-2") }
    val label3 = { WorkloadLabel(key = "key-3", value = "value-3") }

    val labelList1 = { arrayListOf(label1(), label2()) }
    val labelList2 = { arrayListOf(label1(), label2(), label3()) }
    val labelList3 = { arrayListOf(label3()) }
    val labelList4 = { arrayListOf(label3(), label2()) }

    fun workload(
      id: String = newWorkloadId(),
      dataplaneId: String? = null,
      status: WorkloadStatus = WorkloadStatus.PENDING,
      workloadLabels: List<WorkloadLabel>? = null,
      inputPayload: String = "",
      workspaceId: UUID? = UUID.randomUUID(),
      organizationId: UUID? = UUID.randomUUID(),
      logPath: String = "/",
      mutexKey: String = "",
      type: WorkloadType = WorkloadType.SYNC,
      deadline: OffsetDateTime = newTimestamp(),
      signalInput: String = "",
      dataplaneGroup: String = "",
      priority: Int = 0,
    ): Workload =
      Workload(
        id = id,
        dataplaneId = dataplaneId,
        status = status,
        workloadLabels = workloadLabels,
        inputPayload = inputPayload,
        workspaceId = workspaceId,
        organizationId = organizationId,
        logPath = logPath,
        mutexKey = mutexKey,
        type = type,
        deadline = deadline.truncateToTestPrecision(),
        signalInput = signalInput,
        dataplaneGroup = dataplaneGroup,
        priority = priority,
      )

    /**
     * Generate a timestamp with truncated precision to ensure accurate test comparisons.
     */
    fun newTimestamp(): OffsetDateTime = OffsetDateTime.now().truncateToTestPrecision()
  }
}
