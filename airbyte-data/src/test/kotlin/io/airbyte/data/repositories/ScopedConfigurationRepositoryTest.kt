/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.ScopedConfiguration
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigOriginType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigResourceType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigScopeType
import io.micronaut.data.exceptions.DataAccessException
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
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
  fun `authorization metadata omits arbitrary value and version selector cannot return network tokens`() {
    val scope = UUID.randomUUID()
    val link = UUID.randomUUID()
    val token =
      scopedConfigurationRepository.save(
        ScopedConfiguration(
          id = UUID.randomUUID(),
          key = "network_security_token",
          value = "sensitive-network-token",
          scopeType = ConfigScopeType.workspace,
          scopeId = scope,
          originType = ConfigOriginType.private_link,
          origin = link.toString(),
        ),
      )
    val metadata = scopedConfigurationRepository.findAuthorizationMetadataById(token.id).orElseThrow()
    assertEquals(token.id, metadata.id)
    assertEquals("network_security_token", metadata.key)
    assertEquals(scope, metadata.scopeId)
    assertEquals(ConfigScopeType.workspace, metadata.scopeType)
    assertEquals(ConfigOriginType.private_link, metadata.originType)
    assertEquals(link.toString(), metadata.origin)
    assertEquals(null, metadata.resourceType)
    assertEquals(null, metadata.resourceId)
    assertTrue(scopedConfigurationRepository.findConnectorVersionSelectorById(token.id).isEmpty)
    assertTrue(scopedConfigurationRepository.findAuthorizationMetadataById(UUID(0, 0)).isEmpty)
    val version = UUID.randomUUID()
    val pin =
      scopedConfigurationRepository.save(
        token.copy(
          id = UUID.randomUUID(),
          key = "connector_version",
          value = version.toString(),
          resourceType = ConfigResourceType.actor_definition,
          resourceId = UUID.randomUUID(),
        ),
      )
    assertEquals(version.toString(), scopedConfigurationRepository.findConnectorVersionSelectorById(pin.id).orElseThrow())
    assertTrue(scopedConfigurationRepository.findConnectorVersionSelectorById(UUID(0, 0)).isEmpty)
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
    assertEquals(1L, scopedConfigurationRepository.count())

    val persistedConfig = scopedConfigurationRepository.findById(configId).get()

    assertEquals(configId, persistedConfig.id)
    assertEquals(config.key, persistedConfig.key)
    assertEquals(config.value, persistedConfig.value)
    assertEquals(config.scopeType, persistedConfig.scopeType)
    assertEquals(config.scopeId, persistedConfig.scopeId)
    assertEquals(config.resourceType, persistedConfig.resourceType)
    assertEquals(config.resourceId, persistedConfig.resourceId)
    assertEquals(config.originType, persistedConfig.originType)
    assertEquals(config.origin, persistedConfig.origin)
    assertEquals(config.description, persistedConfig.description)
    assertEquals(config.referenceUrl, persistedConfig.referenceUrl)
    assertEquals(config.expiresAt, persistedConfig.expiresAt)
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
    assertEquals(initialValue, persistedConfig.get().value)

    val newValue = "new_config_value"
    config.value = newValue
    scopedConfigurationRepository.update(config)

    val updatedConfig = scopedConfigurationRepository.findById(configId)
    assertEquals(newValue, updatedConfig.get().value)
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
    assertEquals(1L, scopedConfigurationRepository.count())

    scopedConfigurationRepository.deleteById(configId)
    assertEquals(0L, scopedConfigurationRepository.count())
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
    assertEquals(3L, scopedConfigurationRepository.count())

    scopedConfigurationRepository.deleteByIdInList(listOf(config.id, config2.id))
    assertEquals(1L, scopedConfigurationRepository.count())

    assertTrue(scopedConfigurationRepository.findById(config3.id).isPresent)
    assertTrue(scopedConfigurationRepository.findById(config2.id).isEmpty)
    assertTrue(scopedConfigurationRepository.findById(config.id).isEmpty)
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
    assertEquals(1L, scopedConfigurationRepository.count())

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
    assertEquals(1L, scopedConfigurationRepository.count())

    val persistedConfig =
      scopedConfigurationRepository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
        config.key,
        config.resourceType,
        config.resourceId,
        config.scopeType,
        config.scopeId,
      )
    assertEquals(configId, persistedConfig?.id)
    assertEquals(config.value, persistedConfig?.value)
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
    assertNull(persistedConfig)
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
    assertEquals(3L, scopedConfigurationRepository.count())

    val persistedConfigs = scopedConfigurationRepository.findByKey(config.key)
    assertEquals(2, persistedConfigs.size)

    val persistedIds = persistedConfigs.map { it.id }

    assertTrue(persistedIds.containsAll(listOf(configId, config2.id)))
    assertFalse(persistedIds.contains(otherConfig.id))

    val persistedConfigs2 = scopedConfigurationRepository.findByKey(otherConfig.key)
    assertEquals(1, persistedConfigs2.size)

    val persistedIds2 = persistedConfigs2.map { it.id }

    assertTrue(persistedIds2.containsAll(listOf(otherConfig.id)))
    assertFalse(persistedIds2.contains(configId))
    assertFalse(persistedIds2.contains(config2.id))
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
    assertEquals(4L, scopedConfigurationRepository.count())

    val findConfigsResult =
      scopedConfigurationRepository.findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeIdInList(
        CONFIG_KEY,
        ConfigResourceType.actor_definition,
        resourceId,
        ConfigScopeType.workspace,
        listOf(config.scopeId, config2.scopeId),
      )
    assertEquals(2, findConfigsResult.size)

    val persistedIds = findConfigsResult.map { it.id }
    assertTrue(persistedIds.containsAll(listOf(config.id, config2.id)))
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
    assertEquals(4L, scopedConfigurationRepository.count())

    val findConfigsResult =
      scopedConfigurationRepository.findByKeyAndResourceTypeAndResourceIdAndOriginTypeAndOriginInList(
        CONFIG_KEY,
        ConfigResourceType.actor_definition,
        resourceId,
        ConfigOriginType.user,
        listOf(originA, originB),
      )
    assertEquals(3, findConfigsResult.size)

    val persistedIds = findConfigsResult.map { it.id }
    assertTrue(persistedIds.containsAll(listOf(config.id, config2.id, config3.id)))
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

    assertEquals(2L, scopedConfigurationRepository.count())

    val findConfigsResult =
      scopedConfigurationRepository.findByOriginType(ConfigOriginType.user)
    assertEquals(1, findConfigsResult.size)

    val persistedIds = findConfigsResult.map { it.id }
    assertTrue(persistedIds.containsAll(listOf(config.id)))
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
    assertEquals(6L, scopedConfigurationRepository.count())

    val findConfigsResult =
      scopedConfigurationRepository.findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndOriginTypeAndValueInList(
        CONFIG_KEY,
        ConfigResourceType.actor_definition,
        resourceId,
        ConfigScopeType.actor,
        ConfigOriginType.breaking_change,
        listOf(valueA, valueB),
      )
    assertEquals(3, findConfigsResult.size)

    val persistedIds = findConfigsResult.map { it.id }
    assertTrue(persistedIds.containsAll(listOf(bcConfig1.id, bcConfig2.id, bcConfig3.id)))
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
    assertEquals(1, organizationScopedConfigs.size)
    assertEquals(
      valueA,
      organizationScopedConfigs
        .stream()
        .findFirst()
        .get()
        .value,
    )

    val workspaceScopedConfigs =
      scopedConfigurationRepository.findByKeyAndResourceTypeAndScopeTypeAndScopeId(
        CONFIG_KEY,
        ConfigResourceType.actor_definition,
        ConfigScopeType.workspace,
        workspaceId,
      )
    assertEquals(1, workspaceScopedConfigs.size)
    assertEquals(
      valueB,
      workspaceScopedConfigs
        .stream()
        .findFirst()
        .get()
        .value,
    )
  }
}
