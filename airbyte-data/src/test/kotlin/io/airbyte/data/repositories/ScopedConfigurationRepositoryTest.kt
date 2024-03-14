package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.ScopedConfiguration
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigOriginType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigResourceType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigScopeType
import io.micronaut.data.exceptions.DataAccessException
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.Date
import java.time.LocalDate
import java.util.UUID

@MicronautTest
internal class ScopedConfigurationRepositoryTest : AbstractConfigRepositoryTest<ScopedConfigurationRepository>(ScopedConfigurationRepository::class) {
  companion object {
    const val CONFIG_KEY = "config_key"
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
  fun `test db delete multi`() {
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
      )

    val config2 =
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
      )

    val config3 =
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
      )

    repository.saveAll(listOf(config, config2, config3))
    assert(repository.count() == 3L)

    repository.deleteByIdInList(listOf(config.id, config2.id))
    assert(repository.count() == 1L)

    assert(repository.findById(config3.id).isPresent)
    assert(repository.findById(config2.id).isEmpty)
    assert(repository.findById(config.id).isEmpty)
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

  @Test
  fun `test db find by key`() {
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

    val config2 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = CONFIG_KEY,
        value = "config_value2",
        scopeType = ConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
        resourceId = UUID.randomUUID(),
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
        expiresAt = Date.valueOf(LocalDate.now()),
      )

    repository.save(config2)

    val otherConfig =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = "other key",
        value = "config_value2",
        scopeType = ConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
        resourceId = UUID.randomUUID(),
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
        expiresAt = Date.valueOf(LocalDate.now()),
      )

    repository.save(otherConfig)
    assert(repository.count() == 3L)

    val persistedConfigs = repository.findByKey(config.key)
    assert(persistedConfigs.size == 2)

    val persistedIds = persistedConfigs.map { it.id }

    assert(persistedIds.containsAll(listOf(configId, config2.id)))
    assert(persistedIds.contains(otherConfig.id).not())

    val persistedConfigs2 = repository.findByKey(otherConfig.key)
    assert(persistedConfigs2.size == 1)

    val persistedIds2 = persistedConfigs2.map { it.id }

    assert(persistedIds2.containsAll(listOf(otherConfig.id)))
    assert(persistedIds2.contains(configId).not())
    assert(persistedIds2.contains(config2.id).not())
  }

  @Test
  fun `test db find by scope id list`() {
    val resourceId = UUID.randomUUID()
    val config =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = CONFIG_KEY,
        value = "config_value",
        scopeType = ConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = UUID.randomUUID().toString(),
      )

    repository.save(config)

    val config2 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = CONFIG_KEY,
        value = "config_value2",
        scopeType = ConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = UUID.randomUUID().toString(),
      )

    repository.save(config2)

    val otherConfig =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = CONFIG_KEY,
        value = "config_value2",
        scopeType = ConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = UUID.randomUUID().toString(),
      )

    repository.save(otherConfig)

    val otherConfig2 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = CONFIG_KEY,
        value = "config_value2",
        scopeType = ConfigScopeType.organization,
        scopeId = config.scopeId,
        resourceType = ConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = UUID.randomUUID().toString(),
      )

    repository.save(otherConfig2)
    assert(repository.count() == 4L)

    val findConfigsResult =
      repository.findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeIdInList(
        CONFIG_KEY,
        ConfigResourceType.actor_definition,
        resourceId,
        ConfigScopeType.workspace,
        listOf(config.scopeId, config2.scopeId),
      )
    assert(findConfigsResult.size == 2)

    val persistedIds = findConfigsResult.map { it.id }
    assert(persistedIds.containsAll(listOf(config.id, config2.id)))
  }

  @Test
  fun `test db find by origin in list`() {
    val resourceId = UUID.randomUUID()
    val originA = UUID.randomUUID().toString()
    val config =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = CONFIG_KEY,
        value = "config_value",
        scopeType = ConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = originA,
      )

    repository.save(config)

    val config2 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = CONFIG_KEY,
        value = "config_value2",
        scopeType = ConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = originA,
      )

    repository.save(config2)

    val originB = UUID.randomUUID().toString()
    val config3 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = CONFIG_KEY,
        value = "config_value2",
        scopeType = ConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = originB,
      )

    repository.save(config3)

    val originC = UUID.randomUUID().toString()
    val config4 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = CONFIG_KEY,
        value = "config_value2",
        scopeType = ConfigScopeType.organization,
        scopeId = config.scopeId,
        resourceType = ConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = originC,
      )

    repository.save(config4)
    assert(repository.count() == 4L)

    val findConfigsResult =
      repository.findByKeyAndResourceTypeAndResourceIdAndOriginTypeAndOriginInList(
        CONFIG_KEY,
        ConfigResourceType.actor_definition,
        resourceId,
        ConfigOriginType.user,
        listOf(originA, originB),
      )
    assert(findConfigsResult.size == 3)

    val persistedIds = findConfigsResult.map { it.id }
    assert(persistedIds.containsAll(listOf(config.id, config2.id, config3.id)))
  }
}
