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
import io.airbyte.config.persistence.OrganizationPersistence.DEFAULT_ORGANIZATION_ID
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl
import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.PropertySource
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource
import io.mockk.every
import io.mockk.mockk
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.testcontainers.containers.PostgreSQLContainer
import java.util.UUID
import javax.sql.DataSource

open class RepositoryTestSetup {
  companion object {
    val connectionId1 = UUID.randomUUID()
    val connectionId2 = UUID.randomUUID()
    private lateinit var context: ApplicationContext
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
      val database = databaseProviders.createNewConfigsDatabase()

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
          .withInitialSetupComplete(true)
          .withOrganizationId(DEFAULT_ORGANIZATION_ID),
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
          .withSupportLevel(SupportLevel.CERTIFIED)
          .withInternalSupportLevel(200L),
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
          .withSupportLevel(SupportLevel.CERTIFIED)
          .withInternalSupportLevel(200L),
        listOf(),
      )

      actorDefinitionService.updateActorDefinitionDefaultVersionId(destinationDefinitionId, destinationDefinitionVersionId)

      val destinationId = UUID.randomUUID()
      destinationService.writeDestinationConnectionNoSecrets(
        DestinationConnection()
          .withDestinationId(destinationId)
          .withName("destination")
          .withDestinationDefinitionId(destinationDefinitionId)
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

  fun <T> getRepository(clazz: Class<T>): T {
    return context.getBean(clazz)
  }
}
