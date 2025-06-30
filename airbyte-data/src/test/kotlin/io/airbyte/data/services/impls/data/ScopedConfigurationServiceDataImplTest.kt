/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.ScopedConfigurationRepository
import io.airbyte.data.repositories.entities.ScopedConfiguration
import io.airbyte.data.services.impls.data.mappers.EntityConfigOriginType
import io.airbyte.data.services.impls.data.mappers.ModelConfigOriginType
import io.airbyte.data.services.impls.data.mappers.ModelConfigScopeType
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.shared.ConfigScopeMapWithId
import io.airbyte.data.services.shared.ScopedConfigurationKey
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigOriginType
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.just
import io.mockk.justRun
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.Date
import java.time.LocalDate
import java.util.Optional
import java.util.UUID

typealias EntityConfigScopeType = io.airbyte.db.instance.configs.jooq.generated.enums.ConfigScopeType
typealias EntityConfigResourceType = io.airbyte.db.instance.configs.jooq.generated.enums.ConfigResourceType
typealias ModelConfigResourceType = io.airbyte.config.ConfigResourceType

internal class ScopedConfigurationServiceDataImplTest {
  private val scopedConfigurationRepository = mockk<ScopedConfigurationRepository>()
  private val scopedConfigurationService = ScopedConfigurationServiceDataImpl(scopedConfigurationRepository)

  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  @Test
  fun `test get configuration by id`() {
    val configId = UUID.randomUUID()

    val expectedConfig =
      ScopedConfiguration(
        id = configId,
        key = "key",
        value = "value",
        scopeType = EntityConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = UUID.randomUUID(),
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
      )

    every { scopedConfigurationRepository.findById(configId) } returns Optional.of(expectedConfig)

    val config = scopedConfigurationService.getScopedConfiguration(configId)
    assert(config == expectedConfig.toConfigModel())

    verify { scopedConfigurationRepository.findById(configId) }
  }

  @Test
  fun `test get configuration by non-existent id throws`() {
    every { scopedConfigurationRepository.findById(any()) } returns Optional.empty()

    assertThrows<ConfigNotFoundException> { scopedConfigurationService.getScopedConfiguration(UUID.randomUUID()) }

    verify { scopedConfigurationRepository.findById(any()) }
  }

  @Test
  fun `test get configuration by resource, scope and key`() {
    val configId = UUID.randomUUID()
    val scopeId = UUID.randomUUID()
    val resourceId = UUID.randomUUID()

    val config =
      ScopedConfiguration(
        id = configId,
        key = "key",
        value = "value",
        scopeType = EntityConfigScopeType.workspace,
        scopeId = scopeId,
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
      )

    every {
      scopedConfigurationRepository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
        "key",
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.workspace,
        scopeId,
      )
    } returns config

    val retrievedConfig =
      scopedConfigurationService.getScopedConfiguration(
        "key",
        ModelConfigResourceType.ACTOR_DEFINITION,
        resourceId,
        ModelConfigScopeType.WORKSPACE,
        scopeId,
      )

    assert(retrievedConfig.get() == config.toConfigModel())

    verify {
      scopedConfigurationRepository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
        "key",
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.workspace,
        scopeId,
      )
    }
  }

  @Test
  fun `test get configuration by resource, scope and key object`() {
    val configKey =
      ScopedConfigurationKey(
        key = "test-key",
        supportedScopes = listOf(ModelConfigScopeType.WORKSPACE),
      )

    val configId = UUID.randomUUID()
    val scopeId = UUID.randomUUID()
    val resourceId = UUID.randomUUID()

    val config =
      ScopedConfiguration(
        id = configId,
        key = configKey.key,
        value = "value",
        scopeType = EntityConfigScopeType.workspace,
        scopeId = scopeId,
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
      )

    every {
      scopedConfigurationRepository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
        configKey.key,
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.workspace,
        scopeId,
      )
    } returns config

    val retrievedConfig =
      scopedConfigurationService.getScopedConfiguration(
        configKey,
        ModelConfigResourceType.ACTOR_DEFINITION,
        resourceId,
        ModelConfigScopeType.WORKSPACE,
        scopeId,
      )

    assert(retrievedConfig.get() == config.toConfigModel())

    verify {
      scopedConfigurationRepository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
        configKey.key,
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.workspace,
        scopeId,
      )
    }
  }

  @Test
  fun `test get configuration by only scope and key object`() {
    val configKey =
      ScopedConfigurationKey(
        key = "test-key",
        supportedScopes = listOf(ModelConfigScopeType.WORKSPACE),
      )

    val configId = UUID.randomUUID()
    val scopeId = UUID.randomUUID()

    val config =
      ScopedConfiguration(
        id = configId,
        key = configKey.key,
        value = "value",
        scopeType = EntityConfigScopeType.workspace,
        scopeId = scopeId,
        resourceType = null,
        resourceId = null,
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
      )

    every {
      scopedConfigurationRepository.findByKeyAndScopeTypeAndScopeId(
        configKey.key,
        EntityConfigScopeType.workspace,
        scopeId,
      )
    } returns listOf(config)

    val retrievedConfig =
      scopedConfigurationService.getScopedConfigurations(
        configKey,
        mapOf(ModelConfigScopeType.WORKSPACE to scopeId),
      )

    assert(retrievedConfig == listOf(config.toConfigModel()))

    verify {
      scopedConfigurationRepository.findByKeyAndScopeTypeAndScopeId(
        configKey.key,
        EntityConfigScopeType.workspace,
        scopeId,
      )
    }
  }

  @Test
  fun `test get configurations by scope map and key object`() {
    val configKey =
      ScopedConfigurationKey(
        key = "test-key",
        supportedScopes = listOf(ModelConfigScopeType.WORKSPACE, ModelConfigScopeType.ORGANIZATION),
      )

    val configId = UUID.randomUUID()
    val scopeId1 = UUID.randomUUID()
    val scopeId2 = UUID.randomUUID()
    val resourceId = UUID.randomUUID()

    val config1 =
      ScopedConfiguration(
        id = configId,
        key = configKey.key,
        value = "value-1",
        scopeType = EntityConfigScopeType.workspace,
        scopeId = scopeId1,
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
      )

    every {
      scopedConfigurationRepository.findByKeyAndResourceTypeAndScopeTypeAndScopeId(
        configKey.key,
        EntityConfigResourceType.actor_definition,
        EntityConfigScopeType.workspace,
        scopeId1,
      )
    } returns listOf(config1)

    val config2 =
      ScopedConfiguration(
        id = configId,
        key = configKey.key,
        value = "value-2",
        scopeType = EntityConfigScopeType.organization,
        scopeId = scopeId2,
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
      )

    every {
      scopedConfigurationRepository.findByKeyAndResourceTypeAndScopeTypeAndScopeId(
        configKey.key,
        EntityConfigResourceType.actor_definition,
        EntityConfigScopeType.organization,
        scopeId2,
      )
    } returns listOf(config2)

    val retrievedConfig =
      scopedConfigurationService.getScopedConfigurations(
        configKey,
        mapOf(
          ModelConfigScopeType.WORKSPACE to scopeId1,
          ModelConfigScopeType.ORGANIZATION to scopeId2,
        ),
        ModelConfigResourceType.ACTOR_DEFINITION,
      )

    assert(retrievedConfig.size == 1)
    val firstConfig1 = retrievedConfig.stream().findFirst().get()
    assert(firstConfig1 == config1.toConfigModel())
    assert(firstConfig1.scopeType == ModelConfigScopeType.WORKSPACE)

    val retrievedConfig2 =
      scopedConfigurationService.getScopedConfigurations(
        configKey,
        mapOf(
          ModelConfigScopeType.ORGANIZATION to scopeId2,
        ),
        ModelConfigResourceType.ACTOR_DEFINITION,
      )

    assert(retrievedConfig2.size == 1)
    val firstConfig2 = retrievedConfig2.stream().findFirst().get()
    assert(firstConfig2 == config2.toConfigModel())
    assert(firstConfig2.scopeType == ModelConfigScopeType.ORGANIZATION)
  }

  @Test
  fun `test get configuration with unsupported scope throws`() {
    val configKey =
      ScopedConfigurationKey(
        key = "test-key-mismatched-supported-scope",
        supportedScopes = listOf(ModelConfigScopeType.WORKSPACE),
      )

    assertThrows<IllegalArgumentException> {
      scopedConfigurationService.getScopedConfiguration(
        configKey,
        ModelConfigResourceType.ACTOR_DEFINITION,
        UUID.randomUUID(),
        ModelConfigScopeType.ORGANIZATION,
        UUID.randomUUID(),
      )
    }
  }

  @Test
  fun `test get configuration by key, resource and scope map`() {
    val configKey =
      ScopedConfigurationKey(
        key = "test-key",
        supportedScopes = listOf(ModelConfigScopeType.WORKSPACE, ModelConfigScopeType.ORGANIZATION),
      )

    val configId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val resourceId = UUID.randomUUID()

    val config =
      ScopedConfiguration(
        id = configId,
        key = configKey.key,
        value = "value",
        scopeType = EntityConfigScopeType.organization,
        scopeId = organizationId,
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
      )

    every {
      scopedConfigurationRepository.findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeIdInList(
        configKey.key,
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.workspace,
        listOf(workspaceId),
      )
    } returns listOf()

    every {
      scopedConfigurationRepository.findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeIdInList(
        configKey.key,
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.organization,
        listOf(organizationId),
      )
    } returns listOf(config)

    val retrievedConfig =
      scopedConfigurationService.getScopedConfiguration(
        configKey,
        ModelConfigResourceType.ACTOR_DEFINITION,
        resourceId,
        mapOf(
          ModelConfigScopeType.WORKSPACE to workspaceId,
          ModelConfigScopeType.ORGANIZATION to organizationId,
        ),
      )

    assert(retrievedConfig.get() == config.toConfigModel())

    verify {
      scopedConfigurationRepository.findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeIdInList(
        configKey.key,
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.workspace,
        listOf(workspaceId),
      )
      scopedConfigurationRepository.findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeIdInList(
        configKey.key,
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.organization,
        listOf(organizationId),
      )
    }
  }

  @Test
  fun `test get configuration with unsupported scope in map throws`() {
    val configKey =
      ScopedConfigurationKey(
        key = "test-key-mismatched-supported-scope-2",
        supportedScopes = listOf(ModelConfigScopeType.WORKSPACE),
      )

    assertThrows<IllegalArgumentException> {
      scopedConfigurationService.getScopedConfiguration(
        configKey,
        ModelConfigResourceType.ACTOR_DEFINITION,
        UUID.randomUUID(),
        mapOf(
          ModelConfigScopeType.ACTOR to UUID.randomUUID(),
          ModelConfigScopeType.WORKSPACE to UUID.randomUUID(),
        ),
      )
    }
  }

  @Test
  fun `test bulk get configurations by key, resource and scope map`() {
    val configKey =
      ScopedConfigurationKey(
        key = "test-key",
        supportedScopes = listOf(ModelConfigScopeType.WORKSPACE, ModelConfigScopeType.ORGANIZATION),
      )

    val resourceId = UUID.randomUUID()

    val organizationId = UUID.randomUUID()
    val workspaceId1 = UUID.randomUUID()
    val workspaceId2 = UUID.randomUUID()

    val organizationId2 = UUID.randomUUID()
    val workspaceId3 = UUID.randomUUID()

    val orgConfig =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = configKey.key,
        value = "value",
        scopeType = EntityConfigScopeType.organization,
        scopeId = organizationId,
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
      )

    val workspace1Config =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = configKey.key,
        value = "value2",
        scopeType = EntityConfigScopeType.workspace,
        scopeId = workspaceId1,
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
      )

    every {
      scopedConfigurationRepository.findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeIdInList(
        configKey.key,
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.workspace,
        listOf(workspaceId1, workspaceId2, workspaceId3),
      )
    } returns listOf(workspace1Config)

    every {
      scopedConfigurationRepository.findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeIdInList(
        configKey.key,
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.organization,
        listOf(organizationId, organizationId2),
      )
    } returns listOf(orgConfig)

    val retrievedConfigs =
      scopedConfigurationService.getScopedConfigurations(
        configKey,
        ModelConfigResourceType.ACTOR_DEFINITION,
        resourceId,
        listOf(
          ConfigScopeMapWithId(
            workspaceId1,
            mapOf(
              ModelConfigScopeType.WORKSPACE to workspaceId1,
              ModelConfigScopeType.ORGANIZATION to organizationId,
            ),
          ),
          ConfigScopeMapWithId(
            workspaceId2,
            mapOf(
              ModelConfigScopeType.WORKSPACE to workspaceId2,
              ModelConfigScopeType.ORGANIZATION to organizationId,
            ),
          ),
          ConfigScopeMapWithId(
            workspaceId3,
            mapOf(
              ModelConfigScopeType.WORKSPACE to workspaceId3,
              ModelConfigScopeType.ORGANIZATION to organizationId2,
            ),
          ),
        ),
      )

    // keys that have a config are in the result map, with the resolved config as the value
    assert(retrievedConfigs[workspaceId1] == workspace1Config.toConfigModel())
    assert(retrievedConfigs[workspaceId2] == orgConfig.toConfigModel())

    // keys that don't have a config are not included in the result map
    assert(!retrievedConfigs.containsKey(workspaceId3))

    verifyAll {
      scopedConfigurationRepository.findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeIdInList(
        configKey.key,
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.workspace,
        listOf(workspaceId1, workspaceId2, workspaceId3),
      )
      scopedConfigurationRepository.findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeIdInList(
        configKey.key,
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.organization,
        listOf(organizationId, organizationId2),
      )
    }
  }

  @Test
  fun `test bulk get configurations with unsupported scope in map throws`() {
    val configKey =
      ScopedConfigurationKey(
        key = "test-key-mismatched-supported-scope-3",
        supportedScopes = listOf(ModelConfigScopeType.WORKSPACE),
      )

    assertThrows<IllegalArgumentException> {
      scopedConfigurationService.getScopedConfigurations(
        configKey,
        ModelConfigResourceType.ACTOR_DEFINITION,
        UUID.randomUUID(),
        listOf(
          ConfigScopeMapWithId(
            UUID.randomUUID(),
            mapOf(
              ModelConfigScopeType.ACTOR to UUID.randomUUID(),
              ModelConfigScopeType.WORKSPACE to UUID.randomUUID(),
            ),
          ),
        ),
      )
    }
  }

  @Test
  fun `test get non-existent configuration by resource, scope and key returns empty opt`() {
    val scopeId = UUID.randomUUID()
    val resourceId = UUID.randomUUID()

    every {
      scopedConfigurationRepository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(any(), any(), any(), any(), any())
    } returns null

    val retrievedConfig =
      scopedConfigurationService.getScopedConfiguration(
        "key",
        ModelConfigResourceType.ACTOR_DEFINITION,
        resourceId,
        ModelConfigScopeType.WORKSPACE,
        scopeId,
      )

    assert(retrievedConfig.isEmpty)

    verify {
      scopedConfigurationRepository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
        "key",
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.workspace,
        scopeId,
      )
    }
  }

  @Test
  fun `test get non-existent configuration by scope map returns empty opt`() {
    val organizationId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val resourceId = UUID.randomUUID()

    val configKey =
      ScopedConfigurationKey(
        key = "test-key-no-config",
        supportedScopes = listOf(ModelConfigScopeType.ORGANIZATION, ModelConfigScopeType.WORKSPACE),
      )

    every {
      scopedConfigurationRepository.findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeIdInList(any(), any(), any(), any(), any())
    } returns listOf()

    val retrievedConfig =
      scopedConfigurationService.getScopedConfiguration(
        configKey,
        ModelConfigResourceType.ACTOR_DEFINITION,
        resourceId,
        mapOf(
          ModelConfigScopeType.ORGANIZATION to organizationId,
          ModelConfigScopeType.WORKSPACE to workspaceId,
        ),
      )

    assert(retrievedConfig.isEmpty)

    verify {
      scopedConfigurationRepository.findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeIdInList(
        configKey.key,
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.workspace,
        listOf(workspaceId),
      )
      scopedConfigurationRepository.findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeIdInList(
        configKey.key,
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.organization,
        listOf(organizationId),
      )
    }
  }

  @Test
  fun `test write new configuration`() {
    val configId = UUID.randomUUID()
    val scopeId = UUID.randomUUID()
    val resourceId = UUID.randomUUID()

    val config =
      ScopedConfiguration(
        id = configId,
        key = "key",
        value = "value",
        scopeType = EntityConfigScopeType.workspace,
        scopeId = scopeId,
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
      )

    every { scopedConfigurationRepository.existsById(configId) } returns false
    every { scopedConfigurationRepository.save(config) } returns config

    val res = scopedConfigurationService.writeScopedConfiguration(config.toConfigModel())
    assert(res == config.toConfigModel())

    verify {
      scopedConfigurationRepository.existsById(configId)
      scopedConfigurationRepository.save(config)
    }
  }

  @Test
  fun `test write existing configuration`() {
    val configId = UUID.randomUUID()
    val scopeId = UUID.randomUUID()
    val resourceId = UUID.randomUUID()

    val config =
      ScopedConfiguration(
        id = configId,
        key = "key",
        value = "value",
        scopeType = EntityConfigScopeType.workspace,
        scopeId = scopeId,
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
      )

    every { scopedConfigurationRepository.existsById(configId) } returns true
    every { scopedConfigurationRepository.update(config) } returns config

    val res = scopedConfigurationService.writeScopedConfiguration(config.toConfigModel())
    assert(res == config.toConfigModel())

    verify {
      scopedConfigurationRepository.existsById(configId)
      scopedConfigurationRepository.update(config)
    }
  }

  @Test
  fun `test bulk insert new configurations`() {
    val resourceId = UUID.randomUUID()

    val config =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = "key",
        value = "value",
        scopeType = EntityConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
      )

    val config2 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = "key",
        value = "value",
        scopeType = EntityConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
      )

    every { scopedConfigurationRepository.saveAll(listOf(config, config2)) } returns listOf(config, config2)

    val res = scopedConfigurationService.insertScopedConfigurations(listOf(config.toConfigModel(), config2.toConfigModel()))
    assert(res == listOf(config.toConfigModel(), config2.toConfigModel()))

    verifyAll {
      scopedConfigurationRepository.saveAll(listOf(config, config2))
    }
  }

  @Test
  fun `test list configurations`() {
    val resourceId = UUID.randomUUID()

    val config =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = "key",
        value = "value",
        scopeType = EntityConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
      )

    val config2 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = "key",
        value = "value2",
        scopeType = EntityConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id2",
        description = "my_description2",
        referenceUrl = "https://github.com/",
        expiresAt = Date.valueOf(LocalDate.now()),
      )

    every { scopedConfigurationRepository.findAll() } returns listOf(config, config2)

    val res = scopedConfigurationService.listScopedConfigurations()
    assert(res == listOf(config.toConfigModel(), config2.toConfigModel()))

    verify {
      scopedConfigurationRepository.findAll()
    }
  }

  @Test
  fun `test list configurations by key`() {
    val resourceId = UUID.randomUUID()

    val config =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = "key",
        value = "value",
        scopeType = EntityConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
      )

    val config2 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = "key",
        value = "value2",
        scopeType = EntityConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id2",
        description = "my_description2",
        referenceUrl = "https://github.com/",
        expiresAt = Date.valueOf(LocalDate.now()),
      )

    every { scopedConfigurationRepository.findByKey("key") } returns listOf(config, config2)

    val res = scopedConfigurationService.listScopedConfigurations("key")
    assert(res == listOf(config.toConfigModel(), config2.toConfigModel()))

    verify {
      scopedConfigurationRepository.findByKey("key")
    }
  }

  @Test
  fun `test list configurations with scopes`() {
    val resourceId = UUID.randomUUID()

    val config =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = "key",
        value = "value",
        scopeType = EntityConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id",
      )

    val config2 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = "key",
        value = "value2",
        scopeType = EntityConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id2",
      )

    every {
      scopedConfigurationRepository.findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeIdInList(
        "key",
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.workspace,
        listOf(config.scopeId, config2.scopeId),
      )
    } returns listOf(config, config2)

    val res =
      scopedConfigurationService.listScopedConfigurationsWithScopes(
        "key",
        ModelConfigResourceType.ACTOR_DEFINITION,
        resourceId,
        ModelConfigScopeType.WORKSPACE,
        listOf(config.scopeId, config2.scopeId),
      )
    assert(res == listOf(config.toConfigModel(), config2.toConfigModel()))

    verify {
      scopedConfigurationRepository.findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeIdInList(
        "key",
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.workspace,
        listOf(config.scopeId, config2.scopeId),
      )
    }
  }

  @Test
  fun `test list configurations with values`() {
    val resourceId = UUID.randomUUID()

    val config =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = "key",
        value = "value",
        scopeType = EntityConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id",
      )

    val config2 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = "key",
        value = "value2",
        scopeType = EntityConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id2",
      )

    every {
      scopedConfigurationRepository.findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndOriginTypeAndValueInList(
        "key",
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.workspace,
        EntityConfigOriginType.user,
        listOf(config.value, config2.value),
      )
    } returns listOf(config, config2)

    val res =
      scopedConfigurationService.listScopedConfigurationsWithValues(
        "key",
        ModelConfigResourceType.ACTOR_DEFINITION,
        resourceId,
        ModelConfigScopeType.WORKSPACE,
        ModelConfigOriginType.USER,
        listOf(config.value, config2.value),
      )
    assert(res == listOf(config.toConfigModel(), config2.toConfigModel()))

    verify {
      scopedConfigurationRepository.findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndOriginTypeAndValueInList(
        "key",
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.workspace,
        EntityConfigOriginType.user,
        listOf(config.value, config2.value),
      )
    }
  }

  @Test
  fun `test list configurations with originType`() {
    val resourceId = UUID.randomUUID()

    val config =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = "key",
        value = "value",
        scopeType = EntityConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
      )

    val config2 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = "key",
        value = "value2",
        scopeType = EntityConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id2",
        description = "my_description2",
        referenceUrl = "https://github.com/",
        expiresAt = Date.valueOf(LocalDate.now()),
      )

    every { scopedConfigurationRepository.findByOriginType(ConfigOriginType.user) } returns listOf(config, config2)

    val res =
      scopedConfigurationService.listScopedConfigurations(io.airbyte.config.ConfigOriginType.USER)
    assert(res == listOf(config.toConfigModel(), config2.toConfigModel()))

    verify {
      scopedConfigurationRepository.findByOriginType(ConfigOriginType.user)
    }
  }

  @Test
  fun `test delete scoped configuration`() {
    val configId = UUID.randomUUID()

    justRun { scopedConfigurationRepository.deleteById(configId) }

    scopedConfigurationService.deleteScopedConfiguration(configId)

    verify { scopedConfigurationRepository.deleteById(configId) }
  }

  @Test
  fun `test delete multiple scoped configuration`() {
    val configIds = listOf(UUID.randomUUID(), UUID.randomUUID())

    justRun { scopedConfigurationRepository.deleteByIdInList(configIds) }

    scopedConfigurationService.deleteScopedConfigurations(configIds)

    verifyAll { scopedConfigurationRepository.deleteByIdInList(configIds) }
  }

  @Test
  fun `test update scoped configurations values for origin in list`() {
    val resourceId = UUID.randomUUID()
    val newValue = "updated_value"
    val origin1 = UUID.randomUUID().toString()
    val origin2 = UUID.randomUUID().toString()
    val newOrigin = UUID.randomUUID().toString()

    val config1 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = "key",
        value = "old_value",
        scopeType = EntityConfigScopeType.actor,
        scopeId = UUID.randomUUID(),
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.connector_rollout,
        origin = origin1,
        description = "description1",
      )

    val config2 =
      ScopedConfiguration(
        id = UUID.randomUUID(),
        key = "key",
        value = "old_value2",
        scopeType = EntityConfigScopeType.actor,
        scopeId = UUID.randomUUID(),
        resourceType = EntityConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.connector_rollout,
        origin = origin2,
        description = "description2",
      )

    val origins = listOf(origin1, origin2)

    every {
      scopedConfigurationRepository.updateByKeyAndResourceTypeAndResourceIdAndOriginTypeAndOriginIn(
        "key",
        EntityConfigResourceType.actor_definition,
        resourceId,
        ConfigOriginType.connector_rollout,
        origins,
        newOrigin,
        newValue,
      )
    } just Runs

    scopedConfigurationService.updateScopedConfigurationsOriginAndValuesForOriginInList(
      key = "key",
      resourceType = ModelConfigResourceType.ACTOR_DEFINITION,
      resourceId = resourceId,
      originType = ModelConfigOriginType.CONNECTOR_ROLLOUT,
      origins = origins,
      newOrigin = newOrigin,
      newValue = newValue,
    )

    verify {
      scopedConfigurationRepository.updateByKeyAndResourceTypeAndResourceIdAndOriginTypeAndOriginIn(
        "key",
        EntityConfigResourceType.actor_definition,
        resourceId,
        ConfigOriginType.connector_rollout,
        origins,
        newOrigin,
        newValue,
      )
    }
  }
}
