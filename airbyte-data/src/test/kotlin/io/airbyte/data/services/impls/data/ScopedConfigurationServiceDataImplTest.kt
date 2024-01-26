package io.airbyte.data.services.impls.data

import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.repositories.ScopedConfigurationRepository
import io.airbyte.data.repositories.entities.ScopedConfiguration
import io.airbyte.data.services.impls.data.mappers.ModelConfigScopeType
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.shared.ScopedConfigurationKey
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigOriginType
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.justRun
import io.mockk.mockk
import io.mockk.verify
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
        EntityConfigResourceType.actor_definition, resourceId,
        EntityConfigScopeType.workspace, scopeId,
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
        EntityConfigResourceType.actor_definition, resourceId,
        EntityConfigScopeType.workspace, scopeId,
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
      scopedConfigurationRepository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
        configKey.key,
        EntityConfigResourceType.actor_definition, resourceId,
        EntityConfigScopeType.workspace, workspaceId,
      )
    } returns null

    every {
      scopedConfigurationRepository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
        configKey.key,
        EntityConfigResourceType.actor_definition, resourceId,
        EntityConfigScopeType.organization, organizationId,
      )
    } returns config

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
      scopedConfigurationRepository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
        configKey.key,
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.workspace,
        workspaceId,
      )
      scopedConfigurationRepository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
        configKey.key,
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.organization,
        organizationId,
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
      scopedConfigurationRepository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(any(), any(), any(), any(), any())
    } returns null

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
      scopedConfigurationRepository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
        configKey.key,
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.workspace,
        workspaceId,
      )
      scopedConfigurationRepository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
        configKey.key,
        EntityConfigResourceType.actor_definition,
        resourceId,
        EntityConfigScopeType.organization,
        organizationId,
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
  fun `test delete scoped configuration`() {
    val configId = UUID.randomUUID()

    justRun { scopedConfigurationRepository.deleteById(configId) }

    scopedConfigurationService.deleteScopedConfiguration(configId)

    verify { scopedConfigurationRepository.deleteById(configId) }
  }
}
