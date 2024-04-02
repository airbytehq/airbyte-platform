package io.airbyte.config.persistence

import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.DestinationConnection
import io.airbyte.config.Geography
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.SupportLevel
import io.airbyte.config.persistence.domain.StreamRefresh
import io.airbyte.config.persistence.domain.StreamRefreshPK
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl
import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.Environment
import io.micronaut.context.env.PropertySource
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer
import java.util.UUID
import javax.sql.DataSource

@MicronautTest(environments = [Environment.TEST])
class StreamRefreshesRepositoryTest {
  companion object {
    private val connectionId1 = UUID.randomUUID()
    private val connectionId2 = UUID.randomUUID()
    private lateinit var context: ApplicationContext
    lateinit var streamRefreshesRepository: StreamRefreshesRepository
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
      val database = databaseProviders.createNewConfigsDatabase()
      streamRefreshesRepository = context.getBean(StreamRefreshesRepository::class.java)

      val workspaceId = UUID.randomUUID()
      val workspaceService =
        WorkspaceServiceJooqImpl(
          database,
          mockk(),
          mockk(),
          mockk(),
          mockk(),
        )

      workspaceService.writeStandardWorkspaceNoSecrets(
        StandardWorkspace()
          .withWorkspaceId(workspaceId)
          .withDefaultGeography(Geography.US)
          .withName("")
          .withSlug("")
          .withInitialSetupComplete(true),
      )

      val actorDefinitionUpdate: ActorDefinitionVersionUpdater = mockk()

      every { actorDefinitionUpdate.updateSourceDefaultVersion(any(), any(), any()) } returns Unit
      every { actorDefinitionUpdate.updateDestinationDefaultVersion(any(), any(), any()) } returns Unit

      val sourceJooq =
        SourceServiceJooqImpl(
          database,
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          actorDefinitionUpdate,
        )

      val sourceDefinitionId = UUID.randomUUID()
      val sourceDefinitionVersionId = UUID.randomUUID()

      sourceJooq.writeConnectorMetadata(
        StandardSourceDefinition()
          .withSourceDefinitionId(sourceDefinitionId)
          .withName("sourceDef"),
        ActorDefinitionVersion()
          .withVersionId(sourceDefinitionVersionId)
          .withActorDefinitionId(sourceDefinitionId)
          .withDockerRepository("")
          .withDockerImageTag("")
          .withSupportState(ActorDefinitionVersion.SupportState.SUPPORTED)
          .withSupportLevel(SupportLevel.CERTIFIED),
        listOf(),
      )

      val actorDefinitionService =
        ActorDefinitionServiceJooqImpl(
          database,
        )
      actorDefinitionService.updateActorDefinitionDefaultVersionId(sourceDefinitionId, sourceDefinitionVersionId)

      val sourceId = UUID.randomUUID()
      sourceJooq.writeSourceConnectionNoSecrets(
        SourceConnection()
          .withSourceId(sourceId)
          .withName("source")
          .withSourceDefinitionId(sourceDefinitionId)
          .withDefaultVersionId(sourceDefinitionVersionId)
          .withWorkspaceId(workspaceId),
      )

      val destinationService =
        DestinationServiceJooqImpl(
          database,
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          actorDefinitionUpdate,
        )

      val destinationDefinitionId = UUID.randomUUID()
      val destinationDefinitionVersionId = UUID.randomUUID()
      destinationService.writeConnectorMetadata(
        StandardDestinationDefinition()
          .withDestinationDefinitionId(destinationDefinitionId)
          .withName("sourceDef"),
        ActorDefinitionVersion()
          .withVersionId(destinationDefinitionVersionId)
          .withActorDefinitionId(destinationDefinitionId)
          .withDockerRepository("")
          .withDockerImageTag("")
          .withSupportState(ActorDefinitionVersion.SupportState.SUPPORTED)
          .withSupportLevel(SupportLevel.CERTIFIED),
        listOf(),
      )

      actorDefinitionService.updateActorDefinitionDefaultVersionId(destinationDefinitionId, destinationDefinitionVersionId)

      val destinationId = UUID.randomUUID()
      destinationService.writeDestinationConnectionNoSecrets(
        DestinationConnection()
          .withDestinationId(destinationId)
          .withName("destination")
          .withDestinationDefinitionId(destinationDefinitionId)
          .withDefaultVersionId(destinationDefinitionVersionId)
          .withWorkspaceId(workspaceId),
      )

      val connectionRepo = StandardSyncPersistence(database)
      connectionRepo.writeStandardSync(
        StandardSync()
          .withConnectionId(connectionId1)
          .withGeography(Geography.US)
          .withSourceId(sourceId)
          .withDestinationId(destinationId)
          .withName("not null")
          .withBreakingChange(true),
      )

      connectionRepo.writeStandardSync(
        StandardSync()
          .withConnectionId(connectionId2)
          .withGeography(Geography.US)
          .withSourceId(sourceId)
          .withDestinationId(destinationId)
          .withName("not null")
          .withBreakingChange(true),
      )
    }

    @AfterAll
    @JvmStatic
    fun dbDown() {
      container.close()
    }
  }

  @AfterEach
  fun cleanDb() {
    streamRefreshesRepository.deleteAll()
  }

  @Test
  fun `test db insertion`() {
    val streamRefresh =
      StreamRefresh(
        StreamRefreshPK(
          connectionId = connectionId1,
          streamName = "sname",
          streamNamespace = "snamespace",
        ),
      )

    streamRefreshesRepository.save(streamRefresh)

    assertTrue(streamRefreshesRepository.existsById(streamRefresh.pk))
  }

  @Test
  fun `find by connection id`() {
    val streamRefresh1 =
      StreamRefresh(
        StreamRefreshPK(
          connectionId = connectionId1,
          streamName = "sname1",
          streamNamespace = "snamespace1",
        ),
      )

    streamRefreshesRepository.save(streamRefresh1)

    val streamRefresh2 =
      StreamRefresh(
        StreamRefreshPK(
          connectionId = connectionId1,
          streamName = "sname2",
          streamNamespace = "snamespace2",
        ),
      )

    streamRefreshesRepository.save(streamRefresh2)

    val streamRefresh3 =
      StreamRefresh(
        StreamRefreshPK(
          connectionId = connectionId2,
          streamName = "sname3",
          streamNamespace = "snamespace3",
        ),
      )

    streamRefreshesRepository.save(streamRefresh3)

    assertEquals(2, streamRefreshesRepository.findByPkConnectionId(connectionId1).size)
  }

  @Test
  fun `delete by connection id`() {
    val streamRefresh1 =
      StreamRefresh(
        StreamRefreshPK(
          connectionId = connectionId1,
          streamName = "sname1",
          streamNamespace = "snamespace1",
        ),
      )

    streamRefreshesRepository.save(streamRefresh1)

    val streamRefresh2 =
      StreamRefresh(
        StreamRefreshPK(
          connectionId = connectionId2,
          streamName = "sname2",
          streamNamespace = "snamespace2",
        ),
      )

    streamRefreshesRepository.save(streamRefresh2)

    streamRefreshesRepository.deleteByPkConnectionId(streamRefresh1.pk.connectionId)

    assertTrue(streamRefreshesRepository.findById(streamRefresh1.pk).isEmpty)
    assertTrue(streamRefreshesRepository.findById(streamRefresh2.pk).isPresent)
  }
}
