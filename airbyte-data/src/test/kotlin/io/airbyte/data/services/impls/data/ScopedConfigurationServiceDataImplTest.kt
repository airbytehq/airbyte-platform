package io.airbyte.data.services.impls.data

import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.repositories.ScopedConfigurationRepository
import io.airbyte.data.repositories.entities.ScopedConfiguration
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigOriginType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigResourceType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigScopeType
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.Date
import java.time.LocalDate
import java.util.Optional
import java.util.UUID

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
        scopeType = ConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
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
        scopeType = ConfigScopeType.workspace,
        scopeId = scopeId,
        resourceType = ConfigResourceType.actor_definition,
        resourceId = resourceId,
        originType = ConfigOriginType.user,
        origin = "my_user_id",
        description = "my_description",
      )

    every {
      scopedConfigurationRepository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
        "key",
        ConfigResourceType.actor_definition, resourceId,
        ConfigScopeType.workspace, scopeId,
      )
    } returns config

    val retrievedConfig =
      scopedConfigurationService.getScopedConfiguration(
        "key",
        io.airbyte.config.ConfigResourceType.ACTOR_DEFINITION,
        resourceId,
        io.airbyte.config.ConfigScopeType.WORKSPACE,
        scopeId,
      )

    assert(retrievedConfig.get() == config.toConfigModel())

    verify {
      scopedConfigurationRepository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
        "key",
        ConfigResourceType.actor_definition,
        resourceId,
        ConfigScopeType.workspace,
        scopeId,
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
        io.airbyte.config.ConfigResourceType.ACTOR_DEFINITION,
        resourceId,
        io.airbyte.config.ConfigScopeType.WORKSPACE,
        scopeId,
      )

    assert(retrievedConfig.isEmpty)

    verify {
      scopedConfigurationRepository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
        "key",
        ConfigResourceType.actor_definition,
        resourceId,
        ConfigScopeType.workspace,
        scopeId,
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
        scopeType = ConfigScopeType.workspace,
        scopeId = scopeId,
        resourceType = ConfigResourceType.actor_definition,
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
        scopeType = ConfigScopeType.workspace,
        scopeId = scopeId,
        resourceType = ConfigResourceType.actor_definition,
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
        scopeType = ConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
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
        scopeType = ConfigScopeType.workspace,
        scopeId = UUID.randomUUID(),
        resourceType = ConfigResourceType.actor_definition,
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
}
