/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.specialized.LastJobWithStatsPerStreamRepository
import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.micronaut.context.ApplicationContext
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.testcontainers.containers.PostgreSQLContainer
import javax.sql.DataSource

/**
 * This class is meant to be extended by any repository test that needs to run against the Config
 * database. It will handle setting up the micronaut context and jooq dsl context, as well as
 * running migrations to set up a real database container.
 */
abstract class AbstractConfigRepositoryTest {
  companion object {
    lateinit var context: ApplicationContext
    lateinit var jooqDslContext: DSLContext

    // we run against an actual database to ensure micronaut data and jooq properly integrate
    private val container: PostgreSQLContainer<*> =
      PostgreSQLContainer(DatabaseConstants.DEFAULT_DATABASE_VERSION)
        .withDatabaseName("airbyte")
        .withUsername("docker")
        .withPassword("docker")

    @BeforeAll
    @JvmStatic
    fun setupBase() {
      container.start()

      // occasionally, the container is not yet accepting connections even though start() has returned.
      // this createConnection() call will block until the container is ready to accept connections.
      container.createConnection("").use { }

      // set the micronaut datasource properties to match our container we started up
      context =
        ApplicationContext.run(
          io.micronaut.context.env.PropertySource.of(
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
      databaseProviders.createNewJobsDatabase()
    }

    @AfterAll
    @JvmStatic
    fun tearDownBase() {
      container.close()
    }
  }

  /**
   * Each repository is made available to implementing classes through the following properties.
   * When adding a new repository, add it here so that it can be accessed by implementing classes
   * for inserting test data or testing repository functionality.
   */
  val streamStatsRepository = context.getBean(StreamStatsRepository::class.java)!!
  val attemptsRepository = context.getBean(AttemptsRepository::class.java)!!
  val jobsRepository = context.getBean(JobsRepository::class.java)!!
  val lastJobPerStreamRepository = context.getBean(LastJobWithStatsPerStreamRepository::class.java)!!
  val connectionTimelineEventRepository = context.getBean(ConnectionTimelineEventRepository::class.java)!!
  val declarativeManifestImageVersionRepository = context.getBean(DeclarativeManifestImageVersionRepository::class.java)!!
  val permissionRepository = context.getBean(PermissionRepository::class.java)!!
  val scopedConfigurationRepository = context.getBean(ScopedConfigurationRepository::class.java)!!
  val connectorRolloutRepository = context.getBean(ConnectorRolloutRepository::class.java)!!
  val userInvitationRepository = context.getBean(UserInvitationRepository::class.java)!!
  val organizationEmailDomainRepository = context.getBean(OrganizationEmailDomainRepository::class.java)!!
  val authRefreshTokenRepository = context.getBean(AuthRefreshTokenRepository::class.java)!!
  val jobsWithAttemptsRepository = context.getBean(JobsWithAttemptsRepository::class.java)!!
  val organizationRepository = context.getBean(OrganizationRepository::class.java)!!
  val workspaceRepository = context.getBean(WorkspaceRepository::class.java)!!
  val organizationPaymentConfigRepository = context.getBean(OrganizationPaymentConfigRepository::class.java)!!
  val tagRepository = context.getBean(TagRepository::class.java)!!
  val dataplaneGroupRepository = context.getBean(DataplaneGroupRepository::class.java)!!
  val dataplaneRepository = context.getBean(DataplaneRepository::class.java)!!
  val secretConfigRepository = context.getBean(SecretConfigRepository::class.java)!!
  val secretStorageRepository = context.getBean(SecretStorageRepository::class.java)!!
  val secretReferenceRepository = context.getBean(SecretReferenceRepository::class.java)!!
  val secretReferenceWithConfigRepository = context.getBean(SecretReferenceWithConfigRepository::class.java)!!
  val actorRepository = context.getBean(ActorRepository::class.java)!!
  val actorDefinitionRepository = context.getBean(ActorDefinitionRepository::class.java)!!
  val ssoConfigRepository = context.getBean(SsoConfigRepository::class.java)!!
}
