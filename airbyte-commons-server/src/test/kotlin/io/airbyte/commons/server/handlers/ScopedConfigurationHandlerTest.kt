/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ScopedConfigurationContextRequestBody
import io.airbyte.api.model.generated.ScopedConfigurationContextResponse
import io.airbyte.api.model.generated.ScopedConfigurationCreateRequestBody
import io.airbyte.api.model.generated.ScopedConfigurationRead
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.handlers.helpers.ScopedConfigurationRelationshipResolver
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.Organization
import io.airbyte.config.ScopedConfiguration
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.User
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.ConnectorVersionKey
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.justRun
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.LocalDate
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier

internal class ScopedConfigurationHandlerTest {
  private val scopedConfigurationService = mockk<ScopedConfigurationService>()
  private val actorDefinitionService = mockk<ActorDefinitionService>()
  private val sourceService = mockk<SourceService>()
  private val destinationService = mockk<DestinationService>()
  private val organizationService = mockk<OrganizationService>()
  private val workspaceService = mockk<WorkspaceService>()
  private val userPersistence = mockk<UserPersistence>()
  private val uuidGenerator = mockk<Supplier<UUID>>()
  private val scopedConfigurationRelationshipResolver = mockk<ScopedConfigurationRelationshipResolver>()
  private val scopedConfigurationHandler =
    ScopedConfigurationHandler(
      scopedConfigurationService,
      actorDefinitionService,
      sourceService,
      destinationService,
      organizationService,
      workspaceService,
      userPersistence,
      uuidGenerator,
      scopedConfigurationRelationshipResolver,
    )

  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  @Test
  fun `test getScopedConfiguration`() {
    val testId = UUID.randomUUID()
    val scopedConfiguration =
      ScopedConfiguration()
        .withId(testId)
        .withValue("value")
        .withKey("key")
        .withDescription("description")
        .withReferenceUrl("url")
        .withResourceId(UUID.randomUUID())
        .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
        .withScopeId(UUID.randomUUID())
        .withScopeType(ConfigScopeType.WORKSPACE)
        .withOrigin(UUID.randomUUID().toString())
        .withOriginType(ConfigOriginType.USER)
        .withExpiresAt("2023-01-01")

    every { scopedConfigurationService.getScopedConfiguration(testId) } returns scopedConfiguration
    every {
      sourceService.getStandardSourceDefinition(scopedConfiguration.resourceId)
    } returns StandardSourceDefinition().withName("source name")
    every {
      workspaceService.getStandardWorkspaceNoSecrets(scopedConfiguration.scopeId, true)
    } returns StandardWorkspace().withName("workspace name")
    every { userPersistence.getUser(UUID.fromString(scopedConfiguration.origin)) } returns
      Optional.of(
        User().withEmail("email@airbyte.io"),
      )

    val scopedConfigurationRead = scopedConfigurationHandler.getScopedConfiguration(testId)

    val expectedRead =
      ScopedConfigurationRead()
        .id(testId.toString())
        .value(scopedConfiguration.value)
        .valueName(null)
        .configKey(scopedConfiguration.key)
        .description(scopedConfiguration.description)
        .referenceUrl(scopedConfiguration.referenceUrl)
        .resourceId(scopedConfiguration.resourceId.toString())
        .resourceType(scopedConfiguration.resourceType.toString())
        .resourceName("source name")
        .scopeId(scopedConfiguration.scopeId.toString())
        .scopeType(scopedConfiguration.scopeType.toString())
        .scopeName("workspace name")
        .origin(scopedConfiguration.origin)
        .originType(scopedConfiguration.originType.toString())
        .originName("email@airbyte.io")
        .expiresAt(scopedConfiguration.expiresAt?.let { LocalDate.parse(scopedConfiguration.expiresAt) })

    assertEquals(expectedRead, scopedConfigurationRead)
  }

  @Test
  fun `test listScopedConfigurationsWithOriginType`() {
    val scopedConfigurations =
      listOf(
        ScopedConfiguration()
          .withId(UUID.randomUUID())
          .withValue("value1")
          .withKey("key1")
          .withDescription("description1")
          .withReferenceUrl("url1")
          .withResourceId(UUID.randomUUID())
          .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
          .withScopeId(UUID.randomUUID())
          .withScopeType(ConfigScopeType.ORGANIZATION)
          .withOrigin(UUID.randomUUID().toString())
          .withOriginType(ConfigOriginType.USER),
        ScopedConfiguration()
          .withId(UUID.randomUUID())
          .withValue("value2")
          .withKey("key1")
          .withDescription("description2")
          .withReferenceUrl("url2")
          .withResourceId(UUID.randomUUID())
          .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
          .withScopeId(UUID.randomUUID())
          .withScopeType(ConfigScopeType.ACTOR)
          .withOrigin(UUID.randomUUID().toString())
          .withOriginType(ConfigOriginType.USER)
          .withExpiresAt("2023-01-01"),
      )

    every { scopedConfigurationService.listScopedConfigurations(ConfigOriginType.USER) } returns scopedConfigurations
    every { organizationService.getOrganization(scopedConfigurations[0].scopeId) } returns Optional.of(Organization().withName("org name"))
    every { sourceService.getSourceConnection(scopedConfigurations[1].scopeId) } returns SourceConnection().withName("source actor name")
    every { sourceService.getStandardSourceDefinition(any()) } returns StandardSourceDefinition().withName("source def name")
    every { userPersistence.getUser(any()) } returns Optional.of(User().withEmail("me@airbyte.io"))

    val expectedScopedConfigurationReads =
      scopedConfigurations.map { scopedConfiguration ->
        ScopedConfigurationRead()
          .id(scopedConfiguration.id.toString())
          .value(scopedConfiguration.value)
          .configKey(scopedConfiguration.key)
          .description(scopedConfiguration.description)
          .referenceUrl(scopedConfiguration.referenceUrl)
          .resourceId(scopedConfiguration.resourceId.toString())
          .resourceType(scopedConfiguration.resourceType.toString())
          .resourceName("source def name")
          .scopeId(scopedConfiguration.scopeId.toString())
          .scopeType(scopedConfiguration.scopeType.toString())
          .scopeName(if (scopedConfiguration.scopeType == ConfigScopeType.ORGANIZATION) "org name" else "source actor name")
          .origin(scopedConfiguration.origin)
          .originType(scopedConfiguration.originType.toString())
          .originName("me@airbyte.io")
          .expiresAt(scopedConfiguration.expiresAt?.let { LocalDate.parse(scopedConfiguration.expiresAt) })
      }

    val actualScopedConfigurationReads = scopedConfigurationHandler.listScopedConfigurations(ConfigOriginType.USER)

    assertEquals(expectedScopedConfigurationReads, actualScopedConfigurationReads)
  }

  @Test
  fun `test listScopedConfigurations`() {
    val scopedConfigurations =
      listOf(
        ScopedConfiguration()
          .withId(UUID.randomUUID())
          .withValue("value1")
          .withKey("key1")
          .withDescription("description1")
          .withReferenceUrl("url1")
          .withResourceId(UUID.randomUUID())
          .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
          .withScopeId(UUID.randomUUID())
          .withScopeType(ConfigScopeType.ORGANIZATION)
          .withOrigin(UUID.randomUUID().toString())
          .withOriginType(ConfigOriginType.USER),
        ScopedConfiguration()
          .withId(UUID.randomUUID())
          .withValue("value2")
          .withKey("key1")
          .withDescription("description2")
          .withReferenceUrl("url2")
          .withResourceId(UUID.randomUUID())
          .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
          .withScopeId(UUID.randomUUID())
          .withScopeType(ConfigScopeType.ACTOR)
          .withOrigin(UUID.randomUUID().toString())
          .withOriginType(ConfigOriginType.USER)
          .withExpiresAt("2023-01-01"),
      )

    every { scopedConfigurationService.listScopedConfigurations("key1") } returns scopedConfigurations
    every { organizationService.getOrganization(scopedConfigurations[0].scopeId) } returns Optional.of(Organization().withName("org name"))
    every { sourceService.getSourceConnection(scopedConfigurations[1].scopeId) } returns SourceConnection().withName("source actor name")
    every { sourceService.getStandardSourceDefinition(any()) } returns StandardSourceDefinition().withName("source def name")
    every { userPersistence.getUser(any()) } returns Optional.of(User().withEmail("me@airbyte.io"))

    val expectedScopedConfigurationReads =
      scopedConfigurations.map { scopedConfiguration ->
        ScopedConfigurationRead()
          .id(scopedConfiguration.id.toString())
          .value(scopedConfiguration.value)
          .configKey(scopedConfiguration.key)
          .description(scopedConfiguration.description)
          .referenceUrl(scopedConfiguration.referenceUrl)
          .resourceId(scopedConfiguration.resourceId.toString())
          .resourceType(scopedConfiguration.resourceType.toString())
          .resourceName("source def name")
          .scopeId(scopedConfiguration.scopeId.toString())
          .scopeType(scopedConfiguration.scopeType.toString())
          .scopeName(if (scopedConfiguration.scopeType == ConfigScopeType.ORGANIZATION) "org name" else "source actor name")
          .origin(scopedConfiguration.origin)
          .originType(scopedConfiguration.originType.toString())
          .originName("me@airbyte.io")
          .expiresAt(scopedConfiguration.expiresAt?.let { LocalDate.parse(scopedConfiguration.expiresAt) })
      }

    val actualScopedConfigurationReads = scopedConfigurationHandler.listScopedConfigurations("key1")

    assertEquals(expectedScopedConfigurationReads, actualScopedConfigurationReads)
  }

  @Test
  fun `test insertScopedConfiguration all fields`() {
    val scopedConfigUUID = UUID.randomUUID()
    val scopedConfigurationCreate =
      ScopedConfigurationCreateRequestBody()
        .value("value")
        .configKey("key")
        .description("description")
        .referenceUrl("url")
        .resourceId(UUID.randomUUID().toString())
        .resourceType(ConfigResourceType.ACTOR_DEFINITION.toString())
        .scopeId(UUID.randomUUID().toString())
        .scopeType(ConfigScopeType.ORGANIZATION.toString())
        .origin(UUID.randomUUID().toString())
        .originType(ConfigOriginType.USER.toString())
        .expiresAt(LocalDate.parse("2023-01-01"))

    val scopedConfiguration =
      ScopedConfiguration()
        .withId(scopedConfigUUID)
        .withValue(scopedConfigurationCreate.value)
        .withKey(scopedConfigurationCreate.configKey)
        .withDescription(scopedConfigurationCreate.description)
        .withReferenceUrl(scopedConfigurationCreate.referenceUrl)
        .withResourceId(UUID.fromString(scopedConfigurationCreate.resourceId))
        .withResourceType(ConfigResourceType.fromValue(scopedConfigurationCreate.resourceType))
        .withScopeId(UUID.fromString(scopedConfigurationCreate.scopeId))
        .withScopeType(ConfigScopeType.fromValue(scopedConfigurationCreate.scopeType))
        .withOrigin(scopedConfigurationCreate.origin)
        .withOriginType(ConfigOriginType.fromValue(scopedConfigurationCreate.originType))
        .withExpiresAt(scopedConfigurationCreate.expiresAt?.toString())

    every { uuidGenerator.get() } returns scopedConfigUUID
    every { scopedConfigurationService.writeScopedConfiguration(any()) } returns scopedConfiguration
    every {
      sourceService.getStandardSourceDefinition(scopedConfiguration.resourceId)
    } returns StandardSourceDefinition().withName("source definition name")
    every { organizationService.getOrganization(scopedConfiguration.scopeId) } returns Optional.of(Organization().withName("wkspc name"))
    every { userPersistence.getUser(UUID.fromString(scopedConfiguration.origin)) } returns
      Optional.of(
        User().withEmail("user@airbyte.io"),
      )

    val expectedScopedConfigurationRead =
      ScopedConfigurationRead()
        .id(scopedConfiguration.id.toString())
        .value(scopedConfiguration.value)
        .configKey(scopedConfiguration.key)
        .description(scopedConfiguration.description)
        .referenceUrl(scopedConfiguration.referenceUrl)
        .resourceId(scopedConfiguration.resourceId.toString())
        .resourceType(scopedConfiguration.resourceType.toString())
        .resourceName("source definition name")
        .scopeId(scopedConfiguration.scopeId.toString())
        .scopeType(scopedConfiguration.scopeType.toString())
        .scopeName("wkspc name")
        .origin(scopedConfiguration.origin)
        .originType(scopedConfiguration.originType.toString())
        .originName("user@airbyte.io")
        .expiresAt(scopedConfiguration.expiresAt?.let { LocalDate.parse(scopedConfiguration.expiresAt) })

    val actualScopedConfigurationRead = scopedConfigurationHandler.insertScopedConfiguration(scopedConfigurationCreate)

    assertEquals(expectedScopedConfigurationRead, actualScopedConfigurationRead)
  }

  @Test
  fun `test insertScopedConfiguration only required`() {
    val scopedConfigUUID = UUID.randomUUID()
    val scopedConfigurationCreate =
      ScopedConfigurationCreateRequestBody()
        .value("value")
        .configKey("key")
        .resourceId(UUID.randomUUID().toString())
        .resourceType(ConfigResourceType.ACTOR_DEFINITION.toString())
        .scopeId(UUID.randomUUID().toString())
        .scopeType(ConfigScopeType.ORGANIZATION.toString())
        .origin(UUID.randomUUID().toString())
        .originType(ConfigOriginType.USER.toString())

    val scopedConfiguration =
      ScopedConfiguration()
        .withId(scopedConfigUUID)
        .withValue(scopedConfigurationCreate.value)
        .withKey(scopedConfigurationCreate.configKey)
        .withDescription(scopedConfigurationCreate.description)
        .withReferenceUrl(scopedConfigurationCreate.referenceUrl)
        .withResourceId(UUID.fromString(scopedConfigurationCreate.resourceId))
        .withResourceType(ConfigResourceType.fromValue(scopedConfigurationCreate.resourceType))
        .withScopeId(UUID.fromString(scopedConfigurationCreate.scopeId))
        .withScopeType(ConfigScopeType.fromValue(scopedConfigurationCreate.scopeType))
        .withOrigin(scopedConfigurationCreate.origin)
        .withOriginType(ConfigOriginType.fromValue(scopedConfigurationCreate.originType))
        .withExpiresAt(scopedConfigurationCreate.expiresAt?.toString())

    every { uuidGenerator.get() } returns scopedConfigUUID
    every { scopedConfigurationService.writeScopedConfiguration(any()) } returns scopedConfiguration
    every {
      sourceService.getStandardSourceDefinition(scopedConfiguration.resourceId)
    } returns StandardSourceDefinition().withName("source definition name")
    every { organizationService.getOrganization(scopedConfiguration.scopeId) } returns Optional.of(Organization().withName("organization name"))
    every { userPersistence.getUser(UUID.fromString(scopedConfiguration.origin)) } returns
      Optional.of(
        User().withEmail("user@airbyte.io"),
      )

    val expectedScopedConfigurationRead =
      ScopedConfigurationRead()
        .id(scopedConfiguration.id.toString())
        .value(scopedConfiguration.value)
        .configKey(scopedConfiguration.key)
        .description(scopedConfiguration.description)
        .referenceUrl(scopedConfiguration.referenceUrl)
        .resourceId(scopedConfiguration.resourceId.toString())
        .resourceType(scopedConfiguration.resourceType.toString())
        .resourceName("source definition name")
        .scopeId(scopedConfiguration.scopeId.toString())
        .scopeType(scopedConfiguration.scopeType.toString())
        .scopeName("organization name")
        .origin(scopedConfiguration.origin)
        .originType(scopedConfiguration.originType.toString())
        .originName("user@airbyte.io")
        .expiresAt(scopedConfiguration.expiresAt?.let { LocalDate.parse(scopedConfiguration.expiresAt) })

    val actualScopedConfigurationRead = scopedConfigurationHandler.insertScopedConfiguration(scopedConfigurationCreate)

    assertEquals(expectedScopedConfigurationRead, actualScopedConfigurationRead)
  }

  @Test
  fun `test updateScopedConfiguration`() {
    val scopedConfigUUID = UUID.randomUUID()
    every { uuidGenerator.get() } returns scopedConfigUUID

    val scopedConfigurationCreate =
      ScopedConfigurationCreateRequestBody()
        .value("value")
        .configKey("key")
        .description("description")
        .referenceUrl("url")
        .resourceId(UUID.randomUUID().toString())
        .resourceType(ConfigResourceType.ACTOR_DEFINITION.toString())
        .scopeId(UUID.randomUUID().toString())
        .scopeType(ConfigScopeType.ORGANIZATION.toString())
        .origin(UUID.randomUUID().toString())
        .originType(ConfigOriginType.USER.toString())
        .expiresAt(LocalDate.parse("2023-01-01"))

    val expectedScopedConfiguration =
      ScopedConfiguration()
        .withId(scopedConfigUUID)
        .withValue(scopedConfigurationCreate.value)
        .withKey(scopedConfigurationCreate.configKey)
        .withDescription(scopedConfigurationCreate.description)
        .withReferenceUrl(scopedConfigurationCreate.referenceUrl)
        .withResourceId(UUID.fromString(scopedConfigurationCreate.resourceId))
        .withResourceType(ConfigResourceType.fromValue(scopedConfigurationCreate.resourceType))
        .withScopeId(UUID.fromString(scopedConfigurationCreate.scopeId))
        .withScopeType(ConfigScopeType.fromValue(scopedConfigurationCreate.scopeType))
        .withOrigin(scopedConfigurationCreate.origin)
        .withOriginType(ConfigOriginType.fromValue(scopedConfigurationCreate.originType))
        .withExpiresAt(scopedConfigurationCreate.expiresAt?.toString())

    every { scopedConfigurationService.writeScopedConfiguration(any()) } returns expectedScopedConfiguration
    every {
      sourceService.getStandardSourceDefinition(expectedScopedConfiguration.resourceId)
    } returns StandardSourceDefinition().withName("source definition name")
    every {
      organizationService.getOrganization(expectedScopedConfiguration.scopeId)
    } returns Optional.of(Organization().withName("organization name"))
    every {
      userPersistence.getUser(UUID.fromString(expectedScopedConfiguration.origin))
    } returns Optional.of(User().withEmail("user@airbyte.io"))

    val updatedScopedConfiguration = scopedConfigurationHandler.updateScopedConfiguration(scopedConfigUUID, scopedConfigurationCreate)

    val expectedUpdatedConfigurationRead =
      ScopedConfigurationRead()
        .id(expectedScopedConfiguration.id.toString())
        .value(expectedScopedConfiguration.value)
        .configKey(expectedScopedConfiguration.key)
        .description(expectedScopedConfiguration.description)
        .referenceUrl(expectedScopedConfiguration.referenceUrl)
        .resourceId(expectedScopedConfiguration.resourceId.toString())
        .resourceType(expectedScopedConfiguration.resourceType.toString())
        .resourceName("source definition name")
        .scopeId(expectedScopedConfiguration.scopeId.toString())
        .scopeType(expectedScopedConfiguration.scopeType.toString())
        .scopeName("organization name")
        .origin(expectedScopedConfiguration.origin)
        .originType(expectedScopedConfiguration.originType.toString())
        .originName("user@airbyte.io")
        .expiresAt(expectedScopedConfiguration.expiresAt?.let { LocalDate.parse(expectedScopedConfiguration.expiresAt) })

    assertEquals(expectedUpdatedConfigurationRead, updatedScopedConfiguration)
  }

  @Test
  fun `test deleteScopedConfiguration`() {
    val testId = UUID.randomUUID()

    justRun { scopedConfigurationService.deleteScopedConfiguration(testId) }

    scopedConfigurationHandler.deleteScopedConfiguration(testId)

    verify { scopedConfigurationService.deleteScopedConfiguration(testId) }
  }

  @Test
  fun `test assertCreateRelatedRecordsExist`() {
    val scopedConfigurationCreate =
      ScopedConfigurationCreateRequestBody()
        .value(UUID.randomUUID().toString())
        .configKey("connector_version")
        .description("description")
        .referenceUrl("url")
        .resourceId(UUID.randomUUID().toString())
        .resourceType(ConfigResourceType.ACTOR_DEFINITION.toString())
        .scopeId(UUID.randomUUID().toString())
        .scopeType(ConfigScopeType.ORGANIZATION.toString())
        .origin(UUID.randomUUID().toString())
        .originType(ConfigOriginType.USER.toString())
        .expiresAt(LocalDate.parse("2023-01-01"))

    every { sourceService.getStandardSourceDefinition(any()) } returns StandardSourceDefinition().withName("source definition name")
    every { organizationService.getOrganization(any()) } returns Optional.of(Organization().withName("organization name"))
    every { userPersistence.getUser(any()) } returns Optional.of(User().withEmail("user@airbyte.io"))
    every { actorDefinitionService.getActorDefinitionVersion(any()) } returns ActorDefinitionVersion().withDockerImageTag("0.1.0")

    scopedConfigurationHandler.assertCreateRelatedRecordsExist(scopedConfigurationCreate)

    verifyAll {
      sourceService.getStandardSourceDefinition(any())
      organizationService.getOrganization(any())
      userPersistence.getUser(any())
      actorDefinitionService.getActorDefinitionVersion(any())
    }
  }

  @Test
  fun `test assertCreateRelatedRecordsExist with missing resource`() {
    val scopedConfigurationCreate =
      ScopedConfigurationCreateRequestBody()
        .value(UUID.randomUUID().toString())
        .configKey("connector_version")
        .resourceId(UUID.randomUUID().toString())
        .resourceType(ConfigResourceType.ACTOR_DEFINITION.toString())
        .scopeId(UUID.randomUUID().toString())
        .scopeType(ConfigScopeType.ORGANIZATION.toString())
        .origin(UUID.randomUUID().toString())
        .originType(ConfigOriginType.USER.toString())

    every { sourceService.getStandardSourceDefinition(any()) } throws
      ConfigNotFoundException(
        ConfigNotFoundType.STANDARD_SOURCE_DEFINITION,
        scopedConfigurationCreate.resourceId,
      )
    every { destinationService.getStandardDestinationDefinition(any()) } throws
      ConfigNotFoundException(
        ConfigNotFoundType.STANDARD_DESTINATION_DEFINITION,
        scopedConfigurationCreate.resourceId,
      )

    assertThrows<BadRequestException> {
      scopedConfigurationHandler.assertCreateRelatedRecordsExist(scopedConfigurationCreate)
    }

    verifyAll {
      sourceService.getStandardSourceDefinition(any())
      destinationService.getStandardDestinationDefinition(any())
    }
  }

  @Test
  fun `test assertCreateRelatedRecordsExist with missing scope`() {
    val scopedConfigurationCreate =
      ScopedConfigurationCreateRequestBody()
        .value(UUID.randomUUID().toString())
        .configKey("connector_version")
        .resourceId(UUID.randomUUID().toString())
        .resourceType(ConfigResourceType.ACTOR_DEFINITION.toString())
        .scopeId(UUID.randomUUID().toString())
        .scopeType(ConfigScopeType.ORGANIZATION.toString())
        .origin(UUID.randomUUID().toString())
        .originType(ConfigOriginType.USER.toString())

    every { sourceService.getStandardSourceDefinition(any()) } returns StandardSourceDefinition().withName("source definition name")
    every { organizationService.getOrganization(any()) } returns Optional.empty()

    assertThrows<BadRequestException> {
      scopedConfigurationHandler.assertCreateRelatedRecordsExist(scopedConfigurationCreate)
    }

    verifyAll {
      sourceService.getStandardSourceDefinition(any())
      organizationService.getOrganization(any())
    }
  }

  @Test
  fun `test assertCreateRelatedRecordsExist with missing origin`() {
    val scopedConfigurationCreate =
      ScopedConfigurationCreateRequestBody()
        .value(UUID.randomUUID().toString())
        .configKey("connector_version")
        .resourceId(UUID.randomUUID().toString())
        .resourceType(ConfigResourceType.ACTOR_DEFINITION.toString())
        .scopeId(UUID.randomUUID().toString())
        .scopeType(ConfigScopeType.ORGANIZATION.toString())
        .origin(UUID.randomUUID().toString())
        .originType(ConfigOriginType.USER.toString())

    every { sourceService.getStandardSourceDefinition(any()) } returns StandardSourceDefinition().withName("source definition name")
    every { organizationService.getOrganization(any()) } returns Optional.of(Organization().withName("organization name"))
    every { userPersistence.getUser(any()) } returns Optional.empty()

    assertThrows<BadRequestException> {
      scopedConfigurationHandler.assertCreateRelatedRecordsExist(scopedConfigurationCreate)
    }

    verifyAll {
      sourceService.getStandardSourceDefinition(any())
      organizationService.getOrganization(any())
      userPersistence.getUser(any())
    }
  }

  @Test
  fun `test assertCreateRelatedRecordsExist with missing actor definition version`() {
    val scopedConfigurationCreate =
      ScopedConfigurationCreateRequestBody()
        .value(UUID.randomUUID().toString())
        .configKey("connector_version")
        .resourceId(UUID.randomUUID().toString())
        .resourceType(ConfigResourceType.ACTOR_DEFINITION.toString())
        .scopeId(UUID.randomUUID().toString())
        .scopeType(ConfigScopeType.ORGANIZATION.toString())
        .origin(UUID.randomUUID().toString())
        .originType(ConfigOriginType.USER.toString())

    every { sourceService.getStandardSourceDefinition(any()) } returns StandardSourceDefinition().withName("source definition name")
    every { organizationService.getOrganization(any()) } returns Optional.of(Organization().withName("organization name"))
    every { userPersistence.getUser(any()) } returns Optional.of(User().withEmail("user@airbyte.io"))
    every { actorDefinitionService.getActorDefinitionVersion(any()) } throws
      ConfigNotFoundException(
        ConfigNotFoundType.ACTOR_DEFINITION_VERSION,
        scopedConfigurationCreate.resourceId,
      )

    assertThrows<BadRequestException> {
      scopedConfigurationHandler.assertCreateRelatedRecordsExist(scopedConfigurationCreate)
    }

    verifyAll {
      sourceService.getStandardSourceDefinition(any())
      organizationService.getOrganization(any())
      userPersistence.getUser(any())
      actorDefinitionService.getActorDefinitionVersion(any())
    }
  }

  @Test
  fun `test get scoped configuration context for non-existent key throws`() {
    val contextReq =
      ScopedConfigurationContextRequestBody()
        .scopeType(ConfigScopeType.WORKSPACE.toString())
        .scopeId(UUID.randomUUID())
        .resourceType(ConfigResourceType.ACTOR_DEFINITION.toString())
        .resourceId(UUID.randomUUID())
        .configKey("non_existent_key")

    assertThrows<BadRequestException> {
      scopedConfigurationHandler.getScopedConfigurationContext(contextReq)
    }
  }

  @Test
  fun `test get scoped configuration context`() {
    val actorDefId = UUID.randomUUID()
    val actorDefName = "source-faker"

    val workspaceId = UUID.randomUUID()
    val orgId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val sourceId2 = UUID.randomUUID()

    val orgVersionId = UUID.randomUUID()
    val orgVersion = "2.0.0"
    val workspaceVersionId = UUID.randomUUID()
    val workspaceVersion = "1.0.0"
    val sourceVersionId = UUID.randomUUID()
    val sourceVersion = "0.1.0"

    val origin = UUID.randomUUID().toString()
    val originName = "some-user@airbyte.io"

    every { sourceService.getSourceConnection(sourceId) } returns SourceConnection().withName("source actor")
    every { sourceService.getSourceConnection(sourceId2) } returns SourceConnection().withName("source actor 2")
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) } returns
      StandardWorkspace().withName("workspace name")
    every { organizationService.getOrganization(orgId) } returns Optional.of(Organization().withName("org name"))
    every { sourceService.getStandardSourceDefinition(actorDefId) } returns StandardSourceDefinition().withName(actorDefName)
    every { actorDefinitionService.getActorDefinitionVersion(orgVersionId) } returns ActorDefinitionVersion().withDockerImageTag(orgVersion)
    every { actorDefinitionService.getActorDefinitionVersion(workspaceVersionId) } returns
      ActorDefinitionVersion().withDockerImageTag(workspaceVersion)
    every { actorDefinitionService.getActorDefinitionVersion(sourceVersionId) } returns ActorDefinitionVersion().withDockerImageTag(sourceVersion)
    every { userPersistence.getUser(UUID.fromString(origin)) } returns Optional.of(User().withEmail(originName))

    every {
      scopedConfigurationRelationshipResolver.getAllAncestorScopes(ConnectorVersionKey.supportedScopes, ConfigScopeType.WORKSPACE, workspaceId)
    } returns
      mapOf(
        ConfigScopeType.ORGANIZATION to orgId,
      )
    every {
      scopedConfigurationRelationshipResolver.getAllDescendantScopes(ConnectorVersionKey.supportedScopes, ConfigScopeType.WORKSPACE, workspaceId)
    } returns
      mapOf(
        ConfigScopeType.ACTOR to listOf(sourceId, sourceId2),
      )

    every {
      scopedConfigurationService.getScopedConfiguration(
        ConnectorVersionKey,
        ConfigResourceType.ACTOR_DEFINITION,
        actorDefId,
        mapOf(
          ConfigScopeType.WORKSPACE to workspaceId,
          ConfigScopeType.ORGANIZATION to orgId,
        ),
      )
    } returns
      Optional.of(
        ScopedConfiguration()
          .withId(workspaceId)
          .withKey(ConnectorVersionKey.key)
          .withValue(workspaceVersionId.toString())
          .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
          .withResourceId(actorDefId)
          .withScopeType(ConfigScopeType.WORKSPACE)
          .withScopeId(workspaceId)
          .withOriginType(ConfigOriginType.USER)
          .withOrigin(origin),
      )

    every {
      scopedConfigurationService.listScopedConfigurationsWithScopes(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        actorDefId,
        ConfigScopeType.ORGANIZATION,
        listOf(orgId),
      )
    } returns
      listOf(
        ScopedConfiguration()
          .withId(orgId)
          .withKey(ConnectorVersionKey.key)
          .withScopeId(orgId)
          .withScopeType(ConfigScopeType.ORGANIZATION)
          .withValue(orgVersionId.toString())
          .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
          .withResourceId(actorDefId)
          .withOriginType(ConfigOriginType.USER)
          .withOrigin(origin),
      )
    every {
      scopedConfigurationService.listScopedConfigurationsWithScopes(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        actorDefId,
        ConfigScopeType.ACTOR,
        listOf(sourceId, sourceId2),
      )
    } returns
      listOf(
        ScopedConfiguration()
          .withId(sourceId)
          .withKey(ConnectorVersionKey.key)
          .withScopeId(sourceId)
          .withScopeType(ConfigScopeType.ACTOR)
          .withValue(sourceVersionId.toString())
          .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
          .withResourceId(actorDefId)
          .withOriginType(ConfigOriginType.USER)
          .withOrigin(origin),
        ScopedConfiguration()
          .withId(sourceId2)
          .withKey(ConnectorVersionKey.key)
          .withScopeId(sourceId2)
          .withScopeType(ConfigScopeType.ACTOR)
          .withValue(sourceVersionId.toString())
          .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
          .withResourceId(actorDefId)
          .withOriginType(ConfigOriginType.USER)
          .withOrigin(origin),
      )

    val contextReq =
      ScopedConfigurationContextRequestBody()
        .scopeType(ConfigScopeType.WORKSPACE.toString())
        .scopeId(workspaceId)
        .resourceType(ConfigResourceType.ACTOR_DEFINITION.toString())
        .resourceId(actorDefId)
        .configKey("connector_version")
    val scopedConfigurationContext = scopedConfigurationHandler.getScopedConfigurationContext(contextReq)

    val expectedRes =
      ScopedConfigurationContextResponse()
        .activeConfiguration(
          ScopedConfigurationRead()
            .id(workspaceId.toString())
            .value(workspaceVersionId.toString())
            .valueName(workspaceVersion)
            .configKey(ConnectorVersionKey.key)
            .resourceId(actorDefId.toString())
            .resourceType(ConfigResourceType.ACTOR_DEFINITION.toString())
            .resourceName(actorDefName)
            .scopeId(workspaceId.toString())
            .scopeType(ConfigScopeType.WORKSPACE.toString())
            .scopeName("workspace name")
            .origin(origin)
            .originType(ConfigOriginType.USER.toString())
            .originName(originName),
        ).ancestorConfigurations(
          listOf(
            ScopedConfigurationRead()
              .id(orgId.toString())
              .value(orgVersionId.toString())
              .valueName(orgVersion)
              .scopeId(orgId.toString())
              .scopeType(ConfigScopeType.ORGANIZATION.toString())
              .scopeName("org name")
              .configKey(ConnectorVersionKey.key)
              .resourceId(actorDefId.toString())
              .resourceType(ConfigResourceType.ACTOR_DEFINITION.toString())
              .resourceName(actorDefName)
              .origin(origin)
              .originType(ConfigOriginType.USER.toString())
              .originName(originName),
          ),
        ).descendantConfigurations(
          listOf(
            ScopedConfigurationRead()
              .id(sourceId.toString())
              .value(sourceVersionId.toString())
              .valueName(sourceVersion)
              .scopeType(ConfigScopeType.ACTOR.toString())
              .scopeId(sourceId.toString())
              .scopeName("source actor")
              .configKey(ConnectorVersionKey.key)
              .resourceId(actorDefId.toString())
              .resourceType(ConfigResourceType.ACTOR_DEFINITION.toString())
              .resourceName(actorDefName)
              .origin(origin)
              .originType(ConfigOriginType.USER.toString())
              .originName(originName),
            ScopedConfigurationRead()
              .id(sourceId2.toString())
              .value(sourceVersionId.toString())
              .valueName(sourceVersion)
              .configKey(ConnectorVersionKey.key)
              .scopeType(ConfigScopeType.ACTOR.toString())
              .scopeId(sourceId2.toString())
              .scopeName("source actor 2")
              .resourceId(actorDefId.toString())
              .resourceType(ConfigResourceType.ACTOR_DEFINITION.toString())
              .resourceName(actorDefName)
              .origin(origin)
              .originType(ConfigOriginType.USER.toString())
              .originName(originName),
          ),
        )

    assertEquals(expectedRes, scopedConfigurationContext)

    verifyAll {
      sourceService.getSourceConnection(sourceId)
      sourceService.getSourceConnection(sourceId2)
      workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)
      organizationService.getOrganization(orgId)
      sourceService.getStandardSourceDefinition(actorDefId)
      actorDefinitionService.getActorDefinitionVersion(orgVersionId)
      actorDefinitionService.getActorDefinitionVersion(workspaceVersionId)
      actorDefinitionService.getActorDefinitionVersion(sourceVersionId)
      userPersistence.getUser(UUID.fromString(origin))
      scopedConfigurationRelationshipResolver.getAllAncestorScopes(ConnectorVersionKey.supportedScopes, ConfigScopeType.WORKSPACE, workspaceId)
      scopedConfigurationRelationshipResolver.getAllDescendantScopes(ConnectorVersionKey.supportedScopes, ConfigScopeType.WORKSPACE, workspaceId)
      scopedConfigurationService.getScopedConfiguration(
        ConnectorVersionKey,
        ConfigResourceType.ACTOR_DEFINITION,
        actorDefId,
        mapOf(
          ConfigScopeType.WORKSPACE to workspaceId,
          ConfigScopeType.ORGANIZATION to orgId,
        ),
      )
      scopedConfigurationService.listScopedConfigurationsWithScopes(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        actorDefId,
        ConfigScopeType.ORGANIZATION,
        listOf(orgId),
      )
      scopedConfigurationService.listScopedConfigurationsWithScopes(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        actorDefId,
        ConfigScopeType.ACTOR,
        listOf(sourceId, sourceId2),
      )
    }
  }
}
