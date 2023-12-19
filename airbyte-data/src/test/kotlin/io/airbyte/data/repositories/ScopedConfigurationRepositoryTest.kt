package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.ScopedConfiguration
import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigOriginType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigResourceType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigScopeType
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.PropertySource
import io.micronaut.data.exceptions.DataAccessException
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.transaction.jdbc.DelegatingDataSource
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.testcontainers.containers.PostgreSQLContainer
import java.sql.Date
import java.time.LocalDate
import java.util.UUID
import javax.sql.DataSource

@MicronautTest
internal class ScopedConfigurationRepositoryTest {
  companion object {
    const val CONFIG_KEY = "config_key"

    private lateinit var context: ApplicationContext
    lateinit var repository: ScopedConfigurationRepository
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
      repository = context.getBean(ScopedConfigurationRepository::class.java)
    }

    @AfterAll
    @JvmStatic
    fun dbDown() {
      container.close()
    }
  }

  @AfterEach
  fun cleanDb() {
    repository.deleteAll()
  }

  @Test
  fun `test db insertion`() {
    val configId = UUID.randomUUID()
    val config =
      ScopedConfiguration(
        id = configId,
        key = CONFIG_KEY,
        value = "config_value",
        scopeType = ConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
        resourceId = UUID.randomUUID(),
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
        referenceUrl = "https://airbyte.io",
        expiresAt = Date.valueOf("2021-01-01"),
      )

    repository.save(config)
    assert(repository.count() == 1L)

    val persistedConfig = repository.findById(configId).get()

    assert(persistedConfig.id == configId)
    assert(persistedConfig.key == config.key)
    assert(persistedConfig.value == config.value)
    assert(persistedConfig.scopeType == config.scopeType)
    assert(persistedConfig.scopeId == config.scopeId)
    assert(persistedConfig.resourceType == config.resourceType)
    assert(persistedConfig.resourceId == config.resourceId)
    assert(persistedConfig.originType == config.originType)
    assert(persistedConfig.origin == config.origin)
    assert(persistedConfig.description == config.description)
    assert(persistedConfig.referenceUrl == config.referenceUrl)
    assert(persistedConfig.expiresAt == config.expiresAt)
  }

  @Test
  fun `test db update`() {
    val configId = UUID.randomUUID()
    val initialValue = "config_value"

    val config =
      ScopedConfiguration(
        id = configId,
        key = CONFIG_KEY,
        value = initialValue,
        scopeType = ConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
        resourceId = UUID.randomUUID(),
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
      )

    repository.save(config)
    val persistedConfig = repository.findById(configId)
    assert(persistedConfig.get().value == initialValue)

    val newValue = "new_config_value"
    config.value = newValue
    repository.update(config)

    val updatedConfig = repository.findById(configId)
    assert(updatedConfig.get().value == newValue)
  }

  @Test
  fun `test db delete`() {
    val configId = UUID.randomUUID()
    val config =
      ScopedConfiguration(
        id = configId,
        key = CONFIG_KEY,
        value = "config_value",
        scopeType = ConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
        resourceId = UUID.randomUUID(),
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
      )

    repository.save(config)
    assert(repository.count() == 1L)

    repository.deleteById(configId)
    assert(repository.count() == 0L)
  }

  @Test
  fun `test db insert same key+resource+scope throws`() {
    val config =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = CONFIG_KEY,
        value = "config_value",
        scopeType = ConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
        resourceId = UUID.randomUUID(),
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
        expiresAt = Date.valueOf(LocalDate.now()),
      )

    repository.save(config)
    assert(repository.count() == 1L)

    val config2 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = config.key,
        value = "my_config_value",
        scopeType = config.scopeType,
        scopeId = config.scopeId,
        resourceType = config.resourceType,
        resourceId = config.resourceId,
        originType = ConfigOriginType.user,
        origin = UUID.randomUUID().toString(),
        description = "description goes here",
      )

    assertThrows<DataAccessException> { repository.save(config2) }
  }

  @Test
  fun `test db get by resource, scope, and key`() {
    val configId = UUID.randomUUID()
    val config =
      ScopedConfiguration(
        id = configId,
        key = CONFIG_KEY,
        value = "config_value",
        scopeType = ConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
        resourceId = UUID.randomUUID(),
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
        expiresAt = Date.valueOf(LocalDate.now()),
      )

    repository.save(config)
    assert(repository.count() == 1L)

    val persistedConfig =
      repository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
        config.key,
        config.resourceType,
        config.resourceId,
        config.scopeType,
        config.scopeId,
      )
    assert(persistedConfig?.id == configId)
    assert(persistedConfig?.value == config.value)
  }

  @Test
  fun `test db get non-existent config by resource, scope, and key returns null`() {
    val persistedConfig =
      repository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
        CONFIG_KEY,
        ConfigResourceType.actor_definition,
        UUID.randomUUID(),
        ConfigScopeType.workspace,
        UUID.randomUUID(),
      )
    assert(persistedConfig == null)
  }
}
