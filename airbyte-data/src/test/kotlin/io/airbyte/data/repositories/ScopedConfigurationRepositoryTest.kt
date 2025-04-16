/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.ScopedConfiguration
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigOriginType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigResourceType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigScopeType
import io.micronaut.data.exceptions.DataAccessException
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.Date
import java.time.LocalDate
import java.util.UUID

@MicronautTest
internal class ScopedConfigurationRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    const val CONFIG_KEY = "config_key"
  }

  @AfterEach
  fun tearDown() {
    scopedConfigurationRepository.deleteAll()
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

    scopedConfigurationRepository.save(config)
    assert(scopedConfigurationRepository.count() == 1L)

    val persistedConfig = scopedConfigurationRepository.findById(configId).get()

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

    scopedConfigurationRepository.save(config)
    val persistedConfig = scopedConfigurationRepository.findById(configId)
    assert(persistedConfig.get().value == initialValue)

    val newValue = "new_config_value"
    config.value = newValue
    scopedConfigurationRepository.update(config)

    val updatedConfig = scopedConfigurationRepository.findById(configId)
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

    scopedConfigurationRepository.save(config)
    assert(scopedConfigurationRepository.count() == 1L)

    scopedConfigurationRepository.deleteById(configId)
    assert(scopedConfigurationRepository.count() == 0L)
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

    scopedConfigurationRepository.saveAll(listOf(config, config2, config3))
    assert(scopedConfigurationRepository.count() == 3L)

    scopedConfigurationRepository.deleteByIdInList(listOf(config.id, config2.id))
    assert(scopedConfigurationRepository.count() == 1L)

    assert(scopedConfigurationRepository.findById(config3.id).isPresent)
    assert(scopedConfigurationRepository.findById(config2.id).isEmpty)
    assert(scopedConfigurationRepository.findById(config.id).isEmpty)
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

    scopedConfigurationRepository.save(config)
    assert(scopedConfigurationRepository.count() == 1L)

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

    assertThrows<DataAccessException> { scopedConfigurationRepository.save(config2) }
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

    scopedConfigurationRepository.save(config)
    assert(scopedConfigurationRepository.count() == 1L)

    val persistedConfig =
      scopedConfigurationRepository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
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
      scopedConfigurationRepository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
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

    scopedConfigurationRepository.save(config)

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

    scopedConfigurationRepository.save(config2)

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

    scopedConfigurationRepository.save(otherConfig)
    assert(scopedConfigurationRepository.count() == 3L)

    val persistedConfigs = scopedConfigurationRepository.findByKey(config.key)
    assert(persistedConfigs.size == 2)

    val persistedIds = persistedConfigs.map { it.id }

    assert(persistedIds.containsAll(listOf(configId, config2.id)))
    assert(persistedIds.contains(otherConfig.id).not())

    val persistedConfigs2 = scopedConfigurationRepository.findByKey(otherConfig.key)
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

    scopedConfigurationRepository.save(config)

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

    scopedConfigurationRepository.save(config2)

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

    scopedConfigurationRepository.save(otherConfig)

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

    scopedConfigurationRepository.save(otherConfig2)
    assert(scopedConfigurationRepository.count() == 4L)

    val findConfigsResult =
      scopedConfigurationRepository.findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeIdInList(
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

    scopedConfigurationRepository.save(config)

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

    scopedConfigurationRepository.save(config2)

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

    scopedConfigurationRepository.save(config3)

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

    scopedConfigurationRepository.save(config4)
    assert(scopedConfigurationRepository.count() == 4L)

    val findConfigsResult =
      scopedConfigurationRepository.findByKeyAndResourceTypeAndResourceIdAndOriginTypeAndOriginInList(
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

  @Test
  fun `test db find by originType in list`() {
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

    scopedConfigurationRepository.save(config)

    val config2 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = CONFIG_KEY,
        value = "config_value2",
        scopeType = ConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.connector_rollout,
        origin = originA,
      )

    scopedConfigurationRepository.save(config2)

    assert(scopedConfigurationRepository.count() == 2L)

    val findConfigsResult =
      scopedConfigurationRepository.findByOriginType(ConfigOriginType.user)
    assert(findConfigsResult.size == 1)

    val persistedIds = findConfigsResult.map { it.id }
    assert(persistedIds.containsAll(listOf(config.id)))
  }

  @Test
  fun `test db find by value in list`() {
    val resourceId = UUID.randomUUID()
    val valueA = "version-1"
    val bcConfig1 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = CONFIG_KEY,
        value = valueA,
        scopeType = ConfigScopeType.actor,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.breaking_change,
        origin = "origin",
      )

    scopedConfigurationRepository.save(bcConfig1)

    val bcConfig2 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = CONFIG_KEY,
        value = valueA,
        scopeType = ConfigScopeType.actor,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.breaking_change,
        origin = "origin",
      )

    scopedConfigurationRepository.save(bcConfig2)

    val bcConfigOnOtherScopeType =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = CONFIG_KEY,
        value = valueA,
        scopeType = ConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.breaking_change,
        origin = "origin",
      )

    scopedConfigurationRepository.save(bcConfigOnOtherScopeType)

    val userConfig =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = CONFIG_KEY,
        value = valueA,
        scopeType = ConfigScopeType.actor,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "origin",
      )

    scopedConfigurationRepository.save(userConfig)

    val valueB = "version-2"
    val bcConfig3 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = CONFIG_KEY,
        value = valueB,
        scopeType = ConfigScopeType.actor,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.breaking_change,
        origin = "origin2",
      )

    scopedConfigurationRepository.save(bcConfig3)

    val valueC = "version-3"
    val bcConfig4 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = CONFIG_KEY,
        value = valueC,
        scopeType = ConfigScopeType.organization,
        scopeId = bcConfig1.scopeId,
        resourceType = ConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.breaking_change,
        origin = "origin3",
      )

    scopedConfigurationRepository.save(bcConfig4)
    assert(scopedConfigurationRepository.count() == 6L)

    val findConfigsResult =
      scopedConfigurationRepository.findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndOriginTypeAndValueInList(
        CONFIG_KEY,
        ConfigResourceType.actor_definition,
        resourceId,
        ConfigScopeType.actor,
        ConfigOriginType.breaking_change,
        listOf(valueA, valueB),
      )
    assert(findConfigsResult.size == 3)

    val persistedIds = findConfigsResult.map { it.id }
    assert(persistedIds.containsAll(listOf(bcConfig1.id, bcConfig2.id, bcConfig3.id)))
  }

  @Test
  fun `test db find by key and scope`() {
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val valueA = "version-1"
    val valueB = "version-2"
    val bcConfig1 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = CONFIG_KEY,
        value = valueA,
        scopeType = ConfigScopeType.organization,
        scopeId = organizationId,
        resourceType = ConfigResourceType.actor_definition,
        resourceId = UUID.randomUUID(),
        originType = ConfigOriginType.user,
        origin = "origin",
      )

    scopedConfigurationRepository.save(bcConfig1)

    val bcConfig2 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = CONFIG_KEY,
        value = valueB,
        scopeType = ConfigScopeType.workspace,
        scopeId = workspaceId,
        resourceType = ConfigResourceType.actor_definition,
        resourceId = UUID.randomUUID(),
        originType = ConfigOriginType.user,
        origin = "origin",
      )

    scopedConfigurationRepository.save(bcConfig2)

    val organizationScopedConfigs =
      scopedConfigurationRepository.findByKeyAndResourceTypeAndScopeTypeAndScopeId(
        CONFIG_KEY,
        ConfigResourceType.actor_definition,
        ConfigScopeType.organization,
        organizationId,
      )
    assert(organizationScopedConfigs.size == 1)
    assert(
      organizationScopedConfigs
        .stream()
        .findFirst()
        .get()
        .value == valueA,
    )

    val workspaceScopedConfigs =
      scopedConfigurationRepository.findByKeyAndResourceTypeAndScopeTypeAndScopeId(
        CONFIG_KEY,
        ConfigResourceType.actor_definition,
        ConfigScopeType.workspace,
        workspaceId,
      )
    assert(workspaceScopedConfigs.size == 1)
    assert(
      workspaceScopedConfigs
        .stream()
        .findFirst()
        .get()
        .value == valueB,
    )
  }
}
