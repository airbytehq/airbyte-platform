package io.airbyte.data.repositories

import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.micronaut.context.ApplicationContext
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource
import io.micronaut.data.repository.CrudRepository
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.testcontainers.containers.PostgreSQLContainer
import javax.sql.DataSource
import kotlin.reflect.KClass

/**
 * This class is meant to be extended by any repository test that needs to run against the Config
 * database. It will handle setting up the micronaut context and jooq dsl context, as well as
 * running migrations to set up a real database container. Each implementing test class should
 * provide a specific repository to test by implementing the abstract getRepository() method.
 */
abstract class AbstractConfigRepositoryTest<T : CrudRepository<*, *>>(
  repositoryClass: KClass<T>,
) {
  companion object {
    lateinit var context: ApplicationContext
    lateinit var jooqDslContext: DSLContext

    // we run against an actual database to ensure micronaut data and jooq properly integrate
    private val container: PostgreSQLContainer<*> =
      PostgreSQLContainer("postgres:13-alpine")
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
    }

    @AfterAll
    @JvmStatic
    fun tearDownBase() {
      container.close()
    }
  }

  protected var repository = context.getBean(repositoryClass.java)

  @AfterEach
  fun cleanDb() {
    repository.deleteAll()
  }
}
