/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.model.generated.ActorListCursorPaginatedRequestBody
import io.airbyte.api.model.generated.ActorListFilters
import io.airbyte.api.model.generated.ActorStatus
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionReadList
import io.airbyte.api.model.generated.ResourceRequirements
import io.airbyte.api.model.generated.ScopedResourceRequirements
import io.airbyte.api.model.generated.SourceCreate
import io.airbyte.api.model.generated.SourceDefinitionSpecificationRead
import io.airbyte.api.model.generated.SourceDiscoverSchemaWriteRequestBody
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.api.model.generated.SourceSearch
import io.airbyte.api.model.generated.SourceUpdate
import io.airbyte.api.model.generated.SupportState
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.problems.throwable.generated.LicenseEntitlementProblem
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.converters.ConfigurationUpdate
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.server.helpers.ConnectionHelpers
import io.airbyte.commons.server.helpers.ConnectorSpecificationHelpers
import io.airbyte.commons.server.helpers.DestinationHelpers
import io.airbyte.commons.server.helpers.SourceHelpers
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.Configs
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.SuggestedStreams
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus
import io.airbyte.config.secrets.ConfigWithSecretReferences
import io.airbyte.config.secrets.JsonSecretsProcessor
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.SecretsHelpers.SecretReferenceHelpers
import io.airbyte.config.secrets.SecretsHelpers.SecretReferenceHelpers.ConfigWithSecretReferenceIdsInjected
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.config.secrets.buildConfigWithSecretRefsJava
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.PartialUserConfigService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.shared.SourceConnectionWithCount
import io.airbyte.data.services.shared.WorkspaceResourceCursorPagination
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.domain.services.entitlements.ConnectorConfigEntitlementService
import io.airbyte.domain.services.secrets.SecretPersistenceService
import io.airbyte.domain.services.secrets.SecretReferenceService
import io.airbyte.domain.services.secrets.SecretStorageService
import io.airbyte.persistence.job.factory.OAuthConfigSupplier
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.protocol.models.v0.Field
import io.airbyte.validation.json.JsonSchemaValidator
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import jakarta.validation.Valid
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID
import java.util.function.Consumer
import java.util.function.Supplier

internal class SourceHandlerTest {
  lateinit var secretsRepositoryReader: SecretsRepositoryReader
  lateinit var standardSourceDefinition: StandardSourceDefinition
  lateinit var sourceDefinitionVersion: ActorDefinitionVersion
  lateinit var sourceDefinitionVersionWithOverrideStatus: ActorDefinitionVersionWithOverrideStatus
  lateinit var sourceDefinitionSpecificationRead: SourceDefinitionSpecificationRead
  lateinit var sourceConnection: SourceConnection
  lateinit var sourceConnectionWithCount: SourceConnectionWithCount
  lateinit var sourceHandler: SourceHandler
  lateinit var validator: JsonSchemaValidator
  lateinit var connectionsHandler: ConnectionsHandler
  lateinit var configurationUpdate: ConfigurationUpdate
  lateinit var uuidGenerator: Supplier<UUID>
  lateinit var secretsProcessor: JsonSecretsProcessor
  lateinit var connectorSpecification: ConnectorSpecification
  lateinit var oAuthConfigSupplier: OAuthConfigSupplier
  lateinit var actorDefinitionVersionHelper: ActorDefinitionVersionHelper
  lateinit var actorDefinitionVersionUpdater: ActorDefinitionVersionUpdater
  lateinit var partialUserConfigService: PartialUserConfigService

  lateinit var sourceService: SourceService
  lateinit var workspaceHelper: WorkspaceHelper
  lateinit var actorDefinitionHandlerHelper: ActorDefinitionHandlerHelper
  lateinit var licenseEntitlementChecker: LicenseEntitlementChecker
  lateinit var connectorConfigEntitlementService: ConnectorConfigEntitlementService
  lateinit var catalogService: CatalogService
  lateinit var secretsRepositoryWriter: SecretsRepositoryWriter
  lateinit var secretPersistenceService: SecretPersistenceService
  lateinit var secretStorageService: SecretStorageService
  lateinit var secretReferenceService: SecretReferenceService
  lateinit var currentUserService: CurrentUserService
  lateinit var secretPersistence: SecretPersistence

  private val catalogConverter = CatalogConverter(FieldGenerator(), mutableListOf())
  private val apiPojoConverters = ApiPojoConverters(catalogConverter)

  @BeforeEach
  fun setUp() {
    catalogService = mockk(relaxed = true)
    secretsRepositoryReader = mockk(relaxed = true)
    validator = mockk(relaxed = true)
    connectionsHandler = mockk(relaxed = true)
    configurationUpdate = mockk(relaxed = true)
    uuidGenerator = mockk(relaxed = true)
    secretsProcessor = mockk(relaxed = true)
    oAuthConfigSupplier = mockk(relaxed = true)
    actorDefinitionVersionHelper = mockk(relaxed = true)
    sourceService = mockk(relaxed = true)
    workspaceHelper = mockk(relaxed = true)
    secretPersistenceService = mockk(relaxed = true)
    actorDefinitionHandlerHelper = mockk(relaxed = true)
    actorDefinitionVersionUpdater = mockk(relaxed = true)
    licenseEntitlementChecker = mockk(relaxed = true)
    connectorConfigEntitlementService = mockk(relaxed = true)
    secretsRepositoryWriter = mockk(relaxed = true)
    secretStorageService = mockk(relaxed = true)
    secretReferenceService = mockk(relaxed = true)
    currentUserService = mockk(relaxed = true)
    secretPersistence = mockk(relaxed = true)
    partialUserConfigService = mockk(relaxed = true)

    every {
      licenseEntitlementChecker.checkEntitlement(
        any(),
        any(),
        any(),
      )
    } returns true
    every { workspaceHelper.getOrganizationForWorkspace(any()) } returns ORG_ID
    every { secretPersistenceService.getPersistenceFromWorkspaceId(any()) } returns secretPersistence
    every { actorDefinitionHandlerHelper.getVersionBreakingChanges(any()) } returns Optional.empty()

    connectorSpecification = ConnectorSpecificationHelpers.generateConnectorSpecification()

    standardSourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withName("marketo")
        .withIcon(ICON_URL)

    sourceDefinitionVersion =
      ActorDefinitionVersion()
        .withDockerRepository("thebestrepo")
        .withDocumentationUrl("https://wikipedia.org")
        .withDockerImageTag("thelatesttag")
        .withSpec(connectorSpecification)
        .withSupportState(ActorDefinitionVersion.SupportState.SUPPORTED)

    sourceDefinitionVersionWithOverrideStatus = ActorDefinitionVersionWithOverrideStatus(sourceDefinitionVersion, IS_VERSION_OVERRIDE_APPLIED)

    sourceDefinitionSpecificationRead =
      SourceDefinitionSpecificationRead()
        .sourceDefinitionId(standardSourceDefinition.sourceDefinitionId)
        .connectionSpecification(connectorSpecification.connectionSpecification)
        .documentationUrl(connectorSpecification.documentationUrl.toString())

    sourceConnection =
      SourceHelpers.generateSource(
        standardSourceDefinition.sourceDefinitionId,
        apiPojoConverters.scopedResourceReqsToInternal(RESOURCE_ALLOCATION),
      )

    sourceConnectionWithCount = SourceHelpers.generateSourceWithCount(sourceConnection)

    sourceHandler =
      SourceHandler(
        catalogService = catalogService,
        secretsRepositoryReader = secretsRepositoryReader,
        validator = validator,
        connectionsHandler = connectionsHandler,
        uuidGenerator = uuidGenerator,
        secretsProcessor = secretsProcessor,
        configurationUpdate = configurationUpdate,
        oAuthConfigSupplier = oAuthConfigSupplier,
        actorDefinitionVersionHelper = actorDefinitionVersionHelper,
        sourceService = sourceService,
        workspaceHelper = workspaceHelper,
        secretPersistenceService = secretPersistenceService,
        actorDefinitionHandlerHelper = actorDefinitionHandlerHelper,
        actorDefinitionVersionUpdater = actorDefinitionVersionUpdater,
        licenseEntitlementChecker = licenseEntitlementChecker,
        connectorConfigEntitlementService = connectorConfigEntitlementService,
        catalogConverter = catalogConverter,
        apiPojoConverters = apiPojoConverters,
        airbyteEdition = Configs.AirbyteEdition.COMMUNITY,
        secretsRepositoryWriter = secretsRepositoryWriter,
        secretStorageService = secretStorageService,
        secretReferenceService = secretReferenceService,
        currentUserService = currentUserService,
        partialUserConfigService = partialUserConfigService,
      )
  }

  @Test
  fun testCreateSource() {
    // ===== GIVEN =====
    // Create the SourceCreate request with the necessary fields.
    val sourceCreate =
      SourceCreate()
        .name(sourceConnection.name)
        .workspaceId(sourceConnection.workspaceId)
        .sourceDefinitionId(standardSourceDefinition.sourceDefinitionId)
        .connectionConfiguration(sourceConnection.configuration)
        .resourceAllocation(RESOURCE_ALLOCATION)

    // Set up basic mocks.
    every { uuidGenerator.get() } returns sourceConnection.sourceId
    every { sourceService.getStandardSourceDefinition(sourceDefinitionSpecificationRead.sourceDefinitionId) } returns standardSourceDefinition
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        standardSourceDefinition,
        sourceConnection.workspaceId,
      )
    } returns sourceDefinitionVersion
    every {
      oAuthConfigSupplier.maskSourceOAuthParameters(
        sourceDefinitionSpecificationRead.sourceDefinitionId,
        sourceConnection.workspaceId,
        sourceCreate.connectionConfiguration,
        sourceDefinitionVersion.spec,
      )
    } returns sourceCreate.connectionConfiguration
    every {
      actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(
        standardSourceDefinition,
        sourceConnection.workspaceId,
        sourceConnection.sourceId,
      )
    } returns sourceDefinitionVersionWithOverrideStatus

    // Set up current user context.
    val currentUserId = UUID.randomUUID()
    every { currentUserService.getCurrentUserIdIfExists() } returns Optional.of(currentUserId)

    // Set up secret storage mocks.
    val secretStorage = mockk<SecretStorage>()
    val secretStorageId = SecretStorageId(UUID.randomUUID())
    every { secretStorage.id } returns secretStorageId
    every { secretStorageService.getByWorkspaceId(WorkspaceId(sourceConnection.workspaceId)) } returns secretStorage

    // Set up secret reference service mocks for the input configuration.
    val configWithRefs = buildConfigWithSecretRefsJava(sourceConnection.configuration)
    every {
      secretReferenceService.getConfigWithSecretReferences(
        ActorId(sourceConnection.sourceId),
        sourceCreate.connectionConfiguration,
        WorkspaceId(sourceConnection.workspaceId),
      )
    } returns configWithRefs

    // Simulate secret persistence and reference ID insertion.
    val configWithProcessedSecrets =
      SecretReferenceHelpers.processConfigSecrets(
        sourceCreate.connectionConfiguration,
        sourceDefinitionSpecificationRead.connectionSpecification,
        secretStorageId,
      )
    every {
      secretsRepositoryWriter.createFromConfig(
        sourceConnection.workspaceId,
        configWithProcessedSecrets,
        secretPersistence,
      )
    } returns sourceCreate.connectionConfiguration

    val configWithSecretRefIds = clone<JsonNode>(sourceCreate.connectionConfiguration)
    (configWithSecretRefIds as ObjectNode).put("updated_with", "secret_reference_ids")
    every {
      secretReferenceService.createAndInsertSecretReferencesWithStorageId(
        configWithProcessedSecrets,
        ActorId(sourceConnection.sourceId),
        secretStorageId,
        any(),
      )
    } returns ConfigWithSecretReferenceIdsInjected(configWithSecretRefIds)

    // Mock the persisted config that is retrieved after creation and persistence.
    val persistedConfig = clone(sourceConnection).withConfiguration(configWithSecretRefIds)
    every { sourceService.getSourceConnection(sourceConnection.sourceId) } returns persistedConfig
    val configWithRefsAfterPersist = buildConfigWithSecretRefsJava(configWithSecretRefIds)
    every {
      secretReferenceService.getConfigWithSecretReferences(
        ActorId(sourceConnection.sourceId),
        configWithSecretRefIds,
        WorkspaceId(sourceConnection.workspaceId),
      )
    } returns configWithRefsAfterPersist

    // Prepare secret output.
    every {
      secretsProcessor.prepareSecretsForOutput(
        configWithSecretRefIds,
        sourceDefinitionSpecificationRead.connectionSpecification,
      )
    } returns configWithSecretRefIds

    // ===== WHEN =====
    // Call the method under test.
    val actualSourceRead = sourceHandler.createSource(sourceCreate)

    // ===== THEN =====
    // Build the expected SourceRead using the updated configuration.
    val expectedSourceRead =
      SourceHelpers
        .getSourceRead(sourceConnection, standardSourceDefinition, IS_VERSION_OVERRIDE_APPLIED, IS_ENTITLED, SUPPORT_STATE, RESOURCE_ALLOCATION)
        .connectionConfiguration(configWithSecretRefIds)
        .resourceAllocation(RESOURCE_ALLOCATION)

    Assertions.assertEquals(expectedSourceRead, actualSourceRead)

    verify {
      secretsProcessor.prepareSecretsForOutput(configWithSecretRefIds, sourceDefinitionSpecificationRead.connectionSpecification)
    }
    verify {
      oAuthConfigSupplier.maskSourceOAuthParameters(
        sourceDefinitionSpecificationRead.sourceDefinitionId,
        sourceConnection.workspaceId,
        sourceCreate.connectionConfiguration,
        sourceDefinitionVersion.spec,
      )
    }
    verify { sourceService.writeSourceConnectionNoSecrets(persistedConfig) }
    verify {
      actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceConnection.workspaceId)
    }
    verify {
      validator.ensure(sourceDefinitionSpecificationRead.connectionSpecification, sourceCreate.connectionConfiguration)
    }
  }

  @Test
  fun testCreateSourceChecksConfigEntitlements() {
    val sourceCreate =
      SourceCreate()
        .name(sourceConnection.name)
        .workspaceId(sourceConnection.workspaceId)
        .sourceDefinitionId(standardSourceDefinition.sourceDefinitionId)
        .connectionConfiguration(sourceConnection.configuration)
        .resourceAllocation(RESOURCE_ALLOCATION)

    every { uuidGenerator.get() } returns sourceConnection.sourceId
    every { sourceService.getSourceConnection(sourceConnection.sourceId) } returns sourceConnection
    every { sourceService.getStandardSourceDefinition(sourceDefinitionSpecificationRead.sourceDefinitionId) } returns standardSourceDefinition
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        standardSourceDefinition,
        sourceConnection.workspaceId,
      )
    } returns sourceDefinitionVersion

    // Not entitled
    every {
      connectorConfigEntitlementService.ensureEntitledConfig(
        any(),
        sourceDefinitionVersion,
        sourceConnection.configuration,
      )
    } throws LicenseEntitlementProblem()

    Assertions.assertThrows(
      LicenseEntitlementProblem::class.java,
    ) { sourceHandler.createSource(sourceCreate) }

    verify {
      actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceConnection.workspaceId)
    }
    verify {
      validator.ensure(sourceDefinitionSpecificationRead.connectionSpecification, sourceConnection.configuration)
    }
  }

  @Test
  fun testCreateSourceNoEntitlementThrows() {
    val sourceCreate =
      SourceCreate()
        .name(sourceConnection.name)
        .workspaceId(sourceConnection.workspaceId)
        .sourceDefinitionId(standardSourceDefinition.sourceDefinitionId)
        .connectionConfiguration(sourceConnection.configuration)
        .resourceAllocation(RESOURCE_ALLOCATION)

    every { uuidGenerator.get() } returns sourceConnection.sourceId
    every { sourceService.getSourceConnection(sourceConnection.sourceId) } returns sourceConnection
    every { sourceService.getStandardSourceDefinition(sourceDefinitionSpecificationRead.sourceDefinitionId) } returns standardSourceDefinition
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        standardSourceDefinition,
        sourceConnection.workspaceId,
      )
    } returns sourceDefinitionVersion

    // Not entitled
    every {
      licenseEntitlementChecker.ensureEntitled(
        any(),
        Entitlement.SOURCE_CONNECTOR,
        standardSourceDefinition.sourceDefinitionId,
      )
    } throws LicenseEntitlementProblem()

    Assertions.assertThrows(
      LicenseEntitlementProblem::class.java,
    ) { sourceHandler.createSource(sourceCreate) }

    verify {
      actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceConnection.workspaceId)
    }
    verify {
      validator.ensure(sourceDefinitionSpecificationRead.connectionSpecification, sourceConnection.configuration)
    }
  }

  @Test
  fun testNonNullCreateSourceThrowsOnInvalidResourceAllocation() {
    val cloudSourceHandler =
      SourceHandler(
        catalogService,
        secretsRepositoryReader,
        validator,
        connectionsHandler,
        uuidGenerator,
        secretsProcessor,
        configurationUpdate,
        oAuthConfigSupplier,
        actorDefinitionVersionHelper,
        sourceService,
        workspaceHelper,
        secretPersistenceService,
        actorDefinitionHandlerHelper,
        actorDefinitionVersionUpdater,
        licenseEntitlementChecker,
        connectorConfigEntitlementService,
        catalogConverter,
        apiPojoConverters,
        Configs.AirbyteEdition.CLOUD,
        secretsRepositoryWriter,
        secretStorageService,
        secretReferenceService,
        currentUserService,
        partialUserConfigService,
      )

    val sourceCreate =
      SourceCreate()
        .name(sourceConnection.name)
        .workspaceId(sourceConnection.workspaceId)
        .sourceDefinitionId(standardSourceDefinition.sourceDefinitionId)
        .connectionConfiguration(DestinationHelpers.testDestinationJson)
        .resourceAllocation(RESOURCE_ALLOCATION)

    Assertions.assertThrows(
      BadRequestException::class.java,
      { cloudSourceHandler.createSource(sourceCreate) },
      "Expected createSource to throw BadRequestException",
    )
  }

  @Test
  fun testUpdateSource() {
    // ===== GIVEN =====
    // Update the source name and configuration.
    val updatedSourceName = "my updated source name"
    val newConfiguration = clone<JsonNode>(sourceConnection.configuration)
    (newConfiguration as ObjectNode).put(API_KEY_FIELD, API_KEY_VALUE)
    val newResourceAllocation: ScopedResourceRequirements? = getResourceRequirementsForSourceRequest("3", "3 GB")

    val updatedSource =
      clone(sourceConnection)
        .withName(updatedSourceName)
        .withConfiguration(newConfiguration)
        .withTombstone(false)
        .withResourceRequirements(apiPojoConverters.scopedResourceReqsToInternal(newResourceAllocation))

    val sourceUpdate =
      SourceUpdate()
        .name(updatedSourceName)
        .sourceId(sourceConnection.sourceId)
        .connectionConfiguration(newConfiguration)
        .resourceAllocation(newResourceAllocation)

    // Set up basic mocks for the update.
    every {
      oAuthConfigSupplier.maskSourceOAuthParameters(
        sourceDefinitionSpecificationRead.sourceDefinitionId,
        sourceConnection.workspaceId,
        newConfiguration,
        sourceDefinitionVersion.spec,
      )
    } returns newConfiguration
    every { sourceService.getStandardSourceDefinition(sourceDefinitionSpecificationRead.sourceDefinitionId) } returns standardSourceDefinition
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        standardSourceDefinition,
        sourceConnection.workspaceId,
        sourceConnection.sourceId,
      )
    } returns sourceDefinitionVersion
    every { sourceService.getSourceDefinitionFromSource(sourceConnection.sourceId) } returns standardSourceDefinition
    every { configurationUpdate.source(sourceConnection.sourceId, updatedSourceName, newConfiguration) } returns updatedSource
    every {
      actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(
        standardSourceDefinition,
        sourceConnection.workspaceId,
        sourceConnection.sourceId,
      )
    } returns sourceDefinitionVersionWithOverrideStatus
    every { sourceService.getSourceConnectionIfExists(sourceConnection.sourceId) } returns Optional.of<SourceConnection>(sourceConnection)

    // Set up current user context.
    val currentUserId = UUID.randomUUID()
    every { currentUserService.getCurrentUserIdIfExists() } returns Optional.of(currentUserId)

    // Set up secret storage mocks.
    val secretStorage = mockk<SecretStorage>()
    val secretStorageId = SecretStorageId(UUID.randomUUID())
    every { secretStorage.id } returns secretStorageId
    every { secretStorageService.getByWorkspaceId(WorkspaceId(sourceConnection.workspaceId)) } returns secretStorage

    // Set up secret reference service mocks for the previous config.
    val previousConfigWithRefs = buildConfigWithSecretRefsJava(sourceConnection.configuration)
    every {
      secretReferenceService.getConfigWithSecretReferences(
        ActorId(sourceConnection.sourceId),
        sourceConnection.configuration,
        WorkspaceId(sourceConnection.workspaceId),
      )
    } returns previousConfigWithRefs

    // Simulate the secret update and reference ID creation/insertion.
    val newConfigWithProcessedSecrets =
      SecretReferenceHelpers.processConfigSecrets(
        newConfiguration,
        sourceDefinitionSpecificationRead.connectionSpecification,
        secretStorageId,
      )
    every {
      secretsRepositoryWriter.updateFromConfig(
        sourceConnection.workspaceId,
        previousConfigWithRefs,
        newConfigWithProcessedSecrets,
        sourceDefinitionVersion.spec.connectionSpecification,
        secretPersistence,
      )
    } returns newConfigWithProcessedSecrets.originalConfig

    val newConfigWithSecretRefIds = clone<JsonNode>(newConfiguration)
    (newConfigWithSecretRefIds as ObjectNode).put("updated_with", "secret_reference_ids")
    every {
      secretReferenceService.createAndInsertSecretReferencesWithStorageId(
        newConfigWithProcessedSecrets,
        ActorId(sourceConnection.sourceId),
        secretStorageId,
        any(),
      )
    } returns ConfigWithSecretReferenceIdsInjected(newConfigWithSecretRefIds)

    // Mock the updated config that is persisted and retrieved for building the source read.
    val updatedSourceWithSecretRefIds = clone<SourceConnection>(updatedSource).withConfiguration(newConfigWithSecretRefIds)

    // First call returns the original source connection, second call returns the updated one.
    every { sourceService.getSourceConnection(sourceConnection.sourceId) } returns sourceConnection andThen updatedSourceWithSecretRefIds

    val configWithRefsAfterPersist = buildConfigWithSecretRefsJava(newConfigWithSecretRefIds)
    every {
      secretReferenceService.getConfigWithSecretReferences(
        ActorId(sourceConnection.sourceId),
        newConfigWithSecretRefIds,
        WorkspaceId(sourceConnection.workspaceId),
      )
    } returns configWithRefsAfterPersist

    // Prepare secret output.
    every {
      secretsProcessor.prepareSecretsForOutput(
        newConfigWithSecretRefIds,
        sourceDefinitionSpecificationRead.connectionSpecification,
      )
    } returns newConfigWithSecretRefIds

    // ===== WHEN =====
    // Call the method under test.
    val actualSourceRead = sourceHandler.updateSource(sourceUpdate)

    // ===== THEN =====
    // Build the expected SourceRead using the updated configuration.
    val expectedSourceRead =
      SourceHelpers
        .getSourceRead(
          updatedSourceWithSecretRefIds,
          standardSourceDefinition,
          IS_VERSION_OVERRIDE_APPLIED,
          IS_ENTITLED,
          SUPPORT_STATE,
          newResourceAllocation,
        ).connectionConfiguration(newConfigWithSecretRefIds)

    Assertions.assertEquals(expectedSourceRead, actualSourceRead)

    verify {
      secretsProcessor.prepareSecretsForOutput(newConfigWithSecretRefIds, sourceDefinitionSpecificationRead.connectionSpecification)
    }
    verify {
      oAuthConfigSupplier.maskSourceOAuthParameters(
        sourceDefinitionSpecificationRead.sourceDefinitionId,
        sourceConnection.workspaceId,
        newConfiguration,
        sourceDefinitionVersion.spec,
      )
    }
    verify { sourceService.writeSourceConnectionNoSecrets(updatedSourceWithSecretRefIds) }
    verify {
      actorDefinitionVersionHelper.getSourceVersion(
        standardSourceDefinition,
        sourceConnection.workspaceId,
        sourceConnection.sourceId,
      )
    }
    verify { validator.ensure(sourceDefinitionSpecificationRead.connectionSpecification, newConfiguration) }
  }

  @Test
  fun testUpdateSourceChecksConfigEntitlements() {
    val updatedSourceName = "my updated source name for config entitlements"
    val newConfiguration = sourceConnection.configuration
    (newConfiguration as ObjectNode).put(API_KEY_FIELD, API_KEY_VALUE)
    val newResourceAllocation: ScopedResourceRequirements? = getResourceRequirementsForSourceRequest("1", "1 GB")

    val expectedSourceConnection =
      clone(sourceConnection)
        .withName(updatedSourceName)
        .withConfiguration(newConfiguration)
        .withTombstone(false)
        .withResourceRequirements(apiPojoConverters.scopedResourceReqsToInternal(newResourceAllocation))

    val sourceUpdate =
      SourceUpdate()
        .name(updatedSourceName)
        .sourceId(sourceConnection.sourceId)
        .connectionConfiguration(newConfiguration)
        .resourceAllocation(newResourceAllocation)

    every {
      secretsProcessor.copySecrets(
        sourceConnection.configuration,
        newConfiguration,
        sourceDefinitionSpecificationRead.connectionSpecification,
      )
    } returns newConfiguration
    every { sourceService.getStandardSourceDefinition(sourceDefinitionSpecificationRead.sourceDefinitionId) } returns standardSourceDefinition
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        standardSourceDefinition,
        sourceConnection.workspaceId,
        sourceConnection.sourceId,
      )
    } returns sourceDefinitionVersion
    every { sourceService.getSourceDefinitionFromSource(sourceConnection.sourceId) } returns standardSourceDefinition
    every { sourceService.getSourceConnection(sourceConnection.sourceId) } returns sourceConnection andThen expectedSourceConnection
    every { configurationUpdate.source(sourceConnection.sourceId, updatedSourceName, newConfiguration) } returns expectedSourceConnection

    // Not entitled
    every {
      connectorConfigEntitlementService.ensureEntitledConfig(
        any(),
        sourceDefinitionVersion,
        newConfiguration,
      )
    } throws LicenseEntitlementProblem()

    Assertions.assertThrows(
      LicenseEntitlementProblem::class.java,
    ) { sourceHandler.updateSource(sourceUpdate) }

    verify {
      actorDefinitionVersionHelper.getSourceVersion(
        standardSourceDefinition,
        sourceConnection.workspaceId,
        sourceConnection.sourceId,
      )
    }
    verify { validator.ensure(sourceDefinitionSpecificationRead.connectionSpecification, newConfiguration) }
  }

  @Test
  fun testUpdateSourceNoEntitlementThrows() {
    val updatedSourceName = "my updated source name"
    val newConfiguration = sourceConnection.configuration
    (newConfiguration as ObjectNode).put(API_KEY_FIELD, API_KEY_VALUE)
    val newResourceAllocation: ScopedResourceRequirements? = getResourceRequirementsForSourceRequest("3", "3 GB")

    val expectedSourceConnection =
      clone(sourceConnection)
        .withName(updatedSourceName)
        .withConfiguration(newConfiguration)
        .withTombstone(false)
        .withResourceRequirements(apiPojoConverters.scopedResourceReqsToInternal(newResourceAllocation))

    val sourceUpdate =
      SourceUpdate()
        .name(updatedSourceName)
        .sourceId(sourceConnection.sourceId)
        .connectionConfiguration(newConfiguration)
        .resourceAllocation(newResourceAllocation)

    every {
      secretsProcessor.copySecrets(
        sourceConnection.configuration,
        newConfiguration,
        sourceDefinitionSpecificationRead.connectionSpecification,
      )
    } returns newConfiguration
    every { sourceService.getStandardSourceDefinition(sourceDefinitionSpecificationRead.sourceDefinitionId) } returns standardSourceDefinition
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        standardSourceDefinition,
        sourceConnection.workspaceId,
        sourceConnection.sourceId,
      )
    } returns sourceDefinitionVersion
    every { sourceService.getSourceDefinitionFromSource(sourceConnection.sourceId) } returns standardSourceDefinition
    every { sourceService.getSourceConnection(sourceConnection.sourceId) } returns sourceConnection andThen expectedSourceConnection
    every { configurationUpdate.source(sourceConnection.sourceId, updatedSourceName, newConfiguration) } returns expectedSourceConnection

    // Not entitled
    every {
      licenseEntitlementChecker.ensureEntitled(
        any(),
        Entitlement.SOURCE_CONNECTOR,
        standardSourceDefinition.sourceDefinitionId,
      )
    } throws LicenseEntitlementProblem()

    Assertions.assertThrows(
      LicenseEntitlementProblem::class.java,
    ) { sourceHandler.updateSource(sourceUpdate) }

    verify {
      actorDefinitionVersionHelper.getSourceVersion(
        standardSourceDefinition,
        sourceConnection.workspaceId,
        sourceConnection.sourceId,
      )
    }
    verify { validator.ensure(sourceDefinitionSpecificationRead.connectionSpecification, newConfiguration) }
  }

  @Test
  fun testNonNullUpdateSourceThrowsOnInvalidResourceAllocation() {
    val cloudSourceHandler =
      SourceHandler(
        catalogService,
        secretsRepositoryReader,
        validator,
        connectionsHandler,
        uuidGenerator,
        secretsProcessor,
        configurationUpdate,
        oAuthConfigSupplier,
        actorDefinitionVersionHelper,
        sourceService,
        workspaceHelper,
        secretPersistenceService,
        actorDefinitionHandlerHelper,
        actorDefinitionVersionUpdater,
        licenseEntitlementChecker,
        connectorConfigEntitlementService,
        catalogConverter,
        apiPojoConverters,
        Configs.AirbyteEdition.CLOUD,
        secretsRepositoryWriter,
        secretStorageService,
        secretReferenceService,
        currentUserService,
        partialUserConfigService,
      )

    val updatedSourceName = "my updated source name"
    val newConfiguration = sourceConnection.configuration
    (newConfiguration as ObjectNode).put(API_KEY_FIELD, API_KEY_VALUE)
    val newResourceAllocation: ScopedResourceRequirements? = getResourceRequirementsForSourceRequest("3", "3 GB")

    val sourceUpdate =
      SourceUpdate()
        .name(updatedSourceName)
        .sourceId(sourceConnection.sourceId)
        .connectionConfiguration(newConfiguration)
        .resourceAllocation(newResourceAllocation)

    Assertions.assertThrows(
      BadRequestException::class.java,
      { cloudSourceHandler.updateSource(sourceUpdate) },
      "Expected updateSource to throw BadRequestException",
    )
  }

  @Test
  fun testUpgradeSourceVersion() {
    val sourceIdRequestBody = SourceIdRequestBody().sourceId(sourceConnection.sourceId)

    every { sourceService.getSourceConnection(sourceConnection.sourceId) } returns sourceConnection
    every { sourceService.getStandardSourceDefinition(standardSourceDefinition.sourceDefinitionId) } returns standardSourceDefinition

    sourceHandler.upgradeSourceVersion(sourceIdRequestBody)

    // validate that we call the actorDefinitionVersionUpdater to upgrade the version to global default
    verify {
      actorDefinitionVersionUpdater.upgradeActorVersion(sourceConnection, standardSourceDefinition)
    }
  }

  @Test
  fun testGetSource() {
    val expectedSourceRead =
      SourceHelpers.getSourceRead(
        sourceConnection,
        standardSourceDefinition,
        IS_VERSION_OVERRIDE_APPLIED,
        IS_ENTITLED,
        SUPPORT_STATE,
        RESOURCE_ALLOCATION,
      )
    val sourceIdRequestBody = SourceIdRequestBody().sourceId(expectedSourceRead.sourceId)

    every { sourceService.getSourceConnection(sourceConnection.sourceId) } returns sourceConnection
    every { sourceService.getStandardSourceDefinition(sourceDefinitionSpecificationRead.sourceDefinitionId) } returns standardSourceDefinition
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        standardSourceDefinition,
        sourceConnection.workspaceId,
        sourceConnection.sourceId,
      )
    } returns sourceDefinitionVersion
    every { sourceService.getSourceDefinitionFromSource(sourceConnection.sourceId) } returns standardSourceDefinition
    every {
      secretsProcessor.prepareSecretsForOutput(
        sourceConnection.configuration,
        sourceDefinitionSpecificationRead.connectionSpecification,
      )
    } returns sourceConnection.configuration
    every {
      actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(
        standardSourceDefinition,
        sourceConnection.workspaceId,
        sourceConnection.sourceId,
      )
    } returns sourceDefinitionVersionWithOverrideStatus
    every {
      secretReferenceService.getConfigWithSecretReferences(
        any(),
        any(),
        any(),
      )
    } answers {
      ConfigWithSecretReferences(
        secondArg(),
        mapOf(),
      )
    }

    val actualSourceRead = sourceHandler.getSource(sourceIdRequestBody)

    Assertions.assertEquals(expectedSourceRead, actualSourceRead)

    verify {
      actorDefinitionVersionHelper.getSourceVersion(
        standardSourceDefinition,
        sourceConnection.workspaceId,
        sourceConnection.sourceId,
      )
    }
    verify {
      secretsProcessor.prepareSecretsForOutput(
        sourceConnection.configuration,
        sourceDefinitionSpecificationRead.connectionSpecification,
      )
    }
  }

  @Test
  fun testListSourcesForWorkspace() {
    val expectedSourceRead =
      SourceHelpers
        .getSourceRead(
          sourceConnectionWithCount.source,
          standardSourceDefinition,
          IS_VERSION_OVERRIDE_APPLIED,
          IS_ENTITLED,
          SUPPORT_STATE,
          RESOURCE_ALLOCATION,
        ).numConnections(0)
        .apply { status = ActorStatus.ACTIVE }

    val workspaceIdRequestBody =
      ActorListCursorPaginatedRequestBody()
        .workspaceId(sourceConnectionWithCount.source.workspaceId)

    every { sourceService.getSourceConnection(sourceConnectionWithCount.source.sourceId) } returns sourceConnectionWithCount.source
    every { sourceService.buildCursorPagination(any(), any(), any(), any(), any()) } returns WorkspaceResourceCursorPagination(null, 10)
    every { sourceService.countWorkspaceSourcesFiltered(any(), any()) } returns 1
    every { sourceService.listWorkspaceSourceConnectionsWithCounts(any(), any()) } returns listOf(sourceConnectionWithCount)
    every { sourceService.getStandardSourceDefinition(sourceDefinitionSpecificationRead.sourceDefinitionId) } returns standardSourceDefinition
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        standardSourceDefinition,
        sourceConnectionWithCount.source.workspaceId,
        sourceConnectionWithCount.source.sourceId,
      )
    } returns sourceDefinitionVersion
    every { sourceService.getSourceDefinitionFromSource(sourceConnectionWithCount.source.sourceId) } returns standardSourceDefinition
    every {
      secretsProcessor.prepareSecretsForOutput(
        sourceConnectionWithCount.source.configuration,
        sourceDefinitionSpecificationRead.connectionSpecification,
      )
    } returns sourceConnectionWithCount.source.configuration
    every {
      actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(
        standardSourceDefinition,
        sourceConnectionWithCount.source.workspaceId,
        sourceConnectionWithCount.source.sourceId,
      )
    } returns sourceDefinitionVersionWithOverrideStatus
    every { secretReferenceService.getConfigWithSecretReferences(any(), any(), any()) } answers
      { ConfigWithSecretReferences(secondArg(), emptyMap()) }

    val actualSourceReadList = sourceHandler.listSourcesForWorkspace(workspaceIdRequestBody)

    Assertions.assertEquals(expectedSourceRead, actualSourceReadList.sources[0])
    verify {
      secretsProcessor.prepareSecretsForOutput(
        sourceConnectionWithCount.source.configuration,
        sourceDefinitionSpecificationRead.connectionSpecification,
      )
    }
    verify {
      actorDefinitionVersionHelper.getSourceVersion(
        standardSourceDefinition,
        sourceConnectionWithCount.source.workspaceId,
        sourceConnectionWithCount.source.sourceId,
      )
    }
    verify { sourceService.buildCursorPagination(any(), any(), any(), any(), any()) }
    verify { sourceService.countWorkspaceSourcesFiltered(any(), any()) }
    verify { sourceService.listWorkspaceSourceConnectionsWithCounts(any(), any()) }
  }

  @Test
  fun testListSourcesForWorkspaceWithPagination() {
    // Create multiple sources for pagination testing
    val source1 = createSourceConnectionWithCount("source1", 2)
    val source2 = createSourceConnectionWithCount("source2", 1)
    val sources = listOf(source1, source2)

    val workspaceId = source1.source.workspaceId
    val requestBody =
      ActorListCursorPaginatedRequestBody()
        .workspaceId(workspaceId)
        .pageSize(10)

    // Mock pagination service methods
    every { sourceService.buildCursorPagination(any(), any(), any(), any(), any()) } returns WorkspaceResourceCursorPagination(null, 10)
    every { sourceService.countWorkspaceSourcesFiltered(any(), any()) } returns 2
    every { sourceService.listWorkspaceSourceConnectionsWithCounts(any(), any()) } returns sources

    // Mock methods for building reads
    every { sourceService.getStandardSourceDefinition(any()) } returns standardSourceDefinition
    every { sourceService.getSourceDefinitionFromSource(any()) } returns standardSourceDefinition
    every { actorDefinitionVersionHelper.getSourceVersion(any(), any(), any()) } returns sourceDefinitionVersion
    every { actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(any(), any(), any()) } returns
      ActorDefinitionVersionWithOverrideStatus(sourceDefinitionVersion, false)
    every { secretsProcessor.prepareSecretsForOutput(any(), any()) } returns emptyObject()
    every { secretReferenceService.getConfigWithSecretReferences(any(), any(), any()) } answers
      { ConfigWithSecretReferences(secondArg(), emptyMap()) }

    // Execute
    val result = sourceHandler.listSourcesForWorkspace(requestBody)

    // Verify results
    Assertions.assertNotNull(result)
    Assertions.assertEquals(2, result.sources.size)
    Assertions.assertEquals(2, result.numConnections)
    Assertions.assertEquals(10, result.pageSize)

    val firstSource = result.sources[0]
    Assertions.assertEquals("source1", firstSource.name)
    Assertions.assertEquals(2, firstSource.numConnections)
    Assertions.assertEquals(ActorStatus.ACTIVE, firstSource.status)

    val secondSource = result.sources[1]
    Assertions.assertEquals("source2", secondSource.name)
    Assertions.assertEquals(1, secondSource.numConnections)
    Assertions.assertEquals(ActorStatus.ACTIVE, secondSource.status)

    verify { sourceService.buildCursorPagination(any(), any(), any(), any(), any()) }
    verify { sourceService.countWorkspaceSourcesFiltered(workspaceId, any()) }
    verify { sourceService.listWorkspaceSourceConnectionsWithCounts(workspaceId, any()) }
  }

  @Test
  fun testListSourcesForWorkspaceWithFilters() {
    val workspaceId = sourceConnectionWithCount.source.workspaceId
    val requestBody =
      ActorListCursorPaginatedRequestBody()
        .workspaceId(workspaceId)
        .pageSize(5)
        .filters(
          ActorListFilters()
            .searchTerm("test")
            .states(listOf(ActorStatus.ACTIVE)),
        )

    // Mock the pagination service methods with filters
    every { sourceService.buildCursorPagination(any(), any(), any(), true, 5) } returns WorkspaceResourceCursorPagination(null, 5)
    every { sourceService.countWorkspaceSourcesFiltered(any(), any()) } returns 1
    every { sourceService.listWorkspaceSourceConnectionsWithCounts(any(), any()) } returns listOf(sourceConnectionWithCount)

    // Mock other service methods
    every { sourceService.getStandardSourceDefinition(any()) } returns standardSourceDefinition
    every { sourceService.getSourceDefinitionFromSource(any()) } returns standardSourceDefinition
    every { actorDefinitionVersionHelper.getSourceVersion(any(), any(), any()) } returns sourceDefinitionVersion
    every { actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(any(), any(), any()) } returns
      ActorDefinitionVersionWithOverrideStatus(sourceDefinitionVersion, false)
    every { secretsProcessor.prepareSecretsForOutput(any(), any()) } returns sourceConnectionWithCount.source.configuration
    every { secretReferenceService.getConfigWithSecretReferences(any(), any(), any()) } answers
      { ConfigWithSecretReferences(secondArg(), emptyMap()) }

    // Execute
    val result = sourceHandler.listSourcesForWorkspace(requestBody)

    // Verify results
    Assertions.assertNotNull(result)
    Assertions.assertEquals(1, result.sources.size)
    Assertions.assertEquals(1, result.numConnections)
    Assertions.assertEquals(5, result.pageSize)

    // Verify service calls
    verify {
      sourceService.buildCursorPagination(
        any(),
        any(),
        any(),
        true,
        5,
      )
    }
    verify { sourceService.countWorkspaceSourcesFiltered(workspaceId, any()) }
    verify { sourceService.listWorkspaceSourceConnectionsWithCounts(workspaceId, any()) }
  }

  private fun createSourceConnectionWithCount(
    name: String?,
    connectionCount: Int,
  ): SourceConnectionWithCount {
    val source =
      SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withWorkspaceId(UUID.randomUUID())
        .withSourceDefinitionId(standardSourceDefinition.sourceDefinitionId)
        .withName(name)
        .withConfiguration(emptyObject())
        .withTombstone(false)

    return SourceConnectionWithCount(
      source,
      "source-definition",
      connectionCount,
      null,
      emptyMap(),
      true,
    )
  }

  @Test
  fun testListSourcesForSourceDefinition() {
    val expectedSourceRead =
      SourceHelpers.getSourceRead(
        sourceConnection,
        standardSourceDefinition,
        IS_VERSION_OVERRIDE_APPLIED,
        IS_ENTITLED,
        SUPPORT_STATE,
        RESOURCE_ALLOCATION,
      )

    every { sourceService.getSourceConnection(sourceConnection.sourceId) } returns sourceConnection
    every { sourceService.listSourcesForDefinition(sourceConnection.sourceDefinitionId) } returns
      listOf(sourceConnection)
    every { sourceService.getStandardSourceDefinition(sourceDefinitionSpecificationRead.sourceDefinitionId) } returns standardSourceDefinition
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        standardSourceDefinition,
        sourceConnection.workspaceId,
        sourceConnection.sourceId,
      )
    } returns sourceDefinitionVersion
    every { sourceService.getSourceDefinitionFromSource(sourceConnection.sourceId) } returns standardSourceDefinition
    every {
      secretsProcessor.prepareSecretsForOutput(
        sourceConnection.configuration,
        sourceDefinitionSpecificationRead.connectionSpecification,
      )
    } returns sourceConnection.configuration
    every {
      actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(
        standardSourceDefinition,
        sourceConnection.workspaceId,
        sourceConnection.sourceId,
      )
    } returns sourceDefinitionVersionWithOverrideStatus
    every {
      secretReferenceService.getConfigWithSecretReferences(
        any(),
        any(),
        any(),
      )
    } answers {
      ConfigWithSecretReferences(
        secondArg(),
        mapOf(),
      )
    }

    val actualSourceReadList = sourceHandler.listSourcesForSourceDefinition(sourceConnection.sourceDefinitionId)

    Assertions.assertEquals(expectedSourceRead, actualSourceReadList.sources[0])
    verify {
      secretsProcessor.prepareSecretsForOutput(
        sourceConnection.configuration,
        sourceDefinitionSpecificationRead.connectionSpecification,
      )
    }
  }

  @Test
  fun testSearchSources() {
    val expectedSourceRead =
      SourceHelpers.getSourceRead(
        sourceConnection,
        standardSourceDefinition,
        IS_VERSION_OVERRIDE_APPLIED,
        IS_ENTITLED,
        SUPPORT_STATE,
        RESOURCE_ALLOCATION,
      )

    every { sourceService.getSourceConnection(sourceConnection.sourceId) } returns sourceConnection
    every { sourceService.listSourceConnection() } returns listOf(sourceConnection)
    every { sourceService.getStandardSourceDefinition(sourceDefinitionSpecificationRead.sourceDefinitionId) } returns standardSourceDefinition
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        standardSourceDefinition,
        sourceConnection.workspaceId,
        sourceConnection.sourceId,
      )
    } returns sourceDefinitionVersion
    every { sourceService.getSourceDefinitionFromSource(sourceConnection.sourceId) } returns standardSourceDefinition
    every {
      secretsProcessor.prepareSecretsForOutput(
        sourceConnection.configuration,
        sourceDefinitionSpecificationRead.connectionSpecification,
      )
    } returns sourceConnection.configuration
    every {
      actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(
        standardSourceDefinition,
        sourceConnection.workspaceId,
        sourceConnection.sourceId,
      )
    } returns sourceDefinitionVersionWithOverrideStatus
    every {
      secretReferenceService.getConfigWithSecretReferences(
        any(),
        any(),
        any(),
      )
    } answers {
      ConfigWithSecretReferences(
        secondArg(),
        mapOf(),
      )
    }

    val validSourceSearch = SourceSearch().name(sourceConnection.name)
    var actualSourceReadList = sourceHandler.searchSources(validSourceSearch)
    Assertions.assertEquals(1, actualSourceReadList.sources.size)
    Assertions.assertEquals(expectedSourceRead, actualSourceReadList.sources[0])

    val invalidSourceSearch = SourceSearch().name("invalid")
    actualSourceReadList = sourceHandler.searchSources(invalidSourceSearch)
    Assertions.assertEquals(0, actualSourceReadList.sources.size)
  }

  @Test
  fun testDeleteSourceAndDeleteSecrets() {
    val newConfiguration = sourceConnection.configuration
    (newConfiguration as ObjectNode).put(API_KEY_FIELD, API_KEY_VALUE)

    val expectedSourceConnection = clone(sourceConnection).withTombstone(true)

    val sourceIdRequestBody = SourceIdRequestBody().sourceId(sourceConnection.sourceId)
    val standardSync = ConnectionHelpers.generateSyncWithSourceId(sourceConnection.sourceId)
    val connectionRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync)
    val connectionReadList = ConnectionReadList().connections(mutableListOf<@Valid ConnectionRead?>(connectionRead))
    val workspaceIdRequestBody = WorkspaceIdRequestBody().workspaceId(sourceConnection.workspaceId)

    every { sourceService.getSourceConnection(sourceConnection.sourceId) } returns sourceConnection andThen expectedSourceConnection
    every {
      oAuthConfigSupplier.maskSourceOAuthParameters(
        sourceDefinitionSpecificationRead.sourceDefinitionId,
        sourceConnection.workspaceId,
        newConfiguration,
        sourceDefinitionVersion.spec,
      )
    } returns newConfiguration
    every { sourceService.getStandardSourceDefinition(sourceDefinitionSpecificationRead.sourceDefinitionId) } returns standardSourceDefinition
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        standardSourceDefinition,
        sourceConnection.workspaceId,
        sourceConnection.sourceId,
      )
    } returns sourceDefinitionVersion
    every { sourceService.getSourceDefinitionFromSource(sourceConnection.sourceId) } returns standardSourceDefinition
    every { connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody) } returns connectionReadList
    every {
      secretsProcessor.prepareSecretsForOutput(
        sourceConnection.configuration,
        sourceDefinitionSpecificationRead.connectionSpecification,
      )
    } returns sourceConnection.configuration
    every {
      actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(
        standardSourceDefinition,
        sourceConnection.workspaceId,
        sourceConnection.sourceId,
      )
    } returns sourceDefinitionVersionWithOverrideStatus
    every {
      secretReferenceService.getConfigWithSecretReferences(
        any(),
        any(),
        any(),
      )
    } answers {
      ConfigWithSecretReferences(
        secondArg(),
        mapOf(),
      )
    }

    sourceHandler.deleteSource(sourceIdRequestBody)

    verify { sourceService.tombstoneSource(any(), any(), any()) }
    verify { connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody) }
    verify { connectionsHandler.deleteConnection(connectionRead.connectionId) }
    verify { secretReferenceService.deleteActorSecretReferences(ActorId(sourceConnection.sourceId)) }
  }

  @Test
  fun testWriteDiscoverCatalogResult() {
    val actorId = UUID.randomUUID()
    val catalogId = UUID.randomUUID()
    val connectorVersion = "0.0.1"
    val hashValue = "0123456789abcd"
    val expectedCatalog = clone(airbyteCatalog)
    expectedCatalog.streams.forEach(Consumer { s: AirbyteStream? -> s!!.withSourceDefinedCursor(false) })

    val request =
      SourceDiscoverSchemaWriteRequestBody()
        .catalog(catalogConverter.toApi(expectedCatalog, ActorDefinitionVersion()))
        .sourceId(actorId)
        .connectorVersion(connectorVersion)
        .configurationHash(hashValue)

    every { catalogService.writeActorCatalogWithFetchEvent(expectedCatalog, actorId, connectorVersion, hashValue) } returns catalogId
    val result = sourceHandler.writeDiscoverCatalogResult(request)

    verify { catalogService.writeActorCatalogWithFetchEvent(expectedCatalog, actorId, connectorVersion, hashValue) }
    Assertions.assertEquals(result.catalogId, catalogId)
  }

  @Test
  fun testCatalogResultSelectedStreams() {
    val actorId = UUID.randomUUID()
    val connectorVersion = "0.0.1"
    val hashValue = "0123456789abcd"

    val advNoSuggestedStreams = ActorDefinitionVersion()
    val advOneSuggestedStream =
      ActorDefinitionVersion().withSuggestedStreams(
        SuggestedStreams().withStreams(mutableListOf<String?>("streamA")),
      )

    val airbyteCatalogWithOneStream =
      AirbyteCatalog().withStreams(
        listOf(CatalogHelpers.createAirbyteStream("streamA", Field.of(SKU, JsonSchemaType.STRING))),
      )
    val airbyteCatalogWithTwoUnsuggestedStreams =
      AirbyteCatalog().withStreams(
        listOf(
          CatalogHelpers.createAirbyteStream("streamA", Field.of(SKU, JsonSchemaType.STRING)),
          CatalogHelpers.createAirbyteStream("streamB", Field.of(SKU, JsonSchemaType.STRING)),
        ),
      )
    val airbyteCatalogWithOneSuggestedAndOneUnsuggestedStream =
      AirbyteCatalog().withStreams(
        listOf(
          CatalogHelpers.createAirbyteStream("streamA", Field.of(SKU, JsonSchemaType.STRING)),
          CatalogHelpers.createAirbyteStream("streamB", Field.of(SKU, JsonSchemaType.STRING)),
        ),
      )

    val requestOne =
      SourceDiscoverSchemaWriteRequestBody()
        .catalog(
          catalogConverter.toApi(airbyteCatalogWithOneStream, advNoSuggestedStreams),
        ).sourceId(actorId)
        .connectorVersion(connectorVersion)
        .configurationHash(hashValue)
    val requestTwo =
      SourceDiscoverSchemaWriteRequestBody()
        .catalog(
          catalogConverter.toApi(airbyteCatalogWithTwoUnsuggestedStreams, advNoSuggestedStreams),
        ).sourceId(actorId)
        .connectorVersion(connectorVersion)
        .configurationHash(hashValue)
    val requestThree =
      SourceDiscoverSchemaWriteRequestBody()
        .catalog(
          catalogConverter.toApi(airbyteCatalogWithOneSuggestedAndOneUnsuggestedStream, advOneSuggestedStream),
        ).sourceId(actorId)
        .connectorVersion(connectorVersion)
        .configurationHash(hashValue)

    Assertions.assertEquals(1, requestOne.catalog.streams.size)
    requestOne
      .catalog
      .streams
      .forEach(Consumer { s: AirbyteStreamAndConfiguration? -> Assertions.assertEquals(true, s!!.config.selected) })
    requestOne
      .catalog
      .streams
      .forEach(Consumer { s: AirbyteStreamAndConfiguration? -> Assertions.assertEquals(true, s!!.config.suggested) })

    Assertions.assertEquals(2, requestTwo.catalog.streams.size)
    requestTwo
      .catalog
      .streams
      .forEach(Consumer { s: AirbyteStreamAndConfiguration? -> Assertions.assertEquals(false, s!!.config.selected) })
    requestTwo
      .catalog
      .streams
      .forEach(Consumer { s: AirbyteStreamAndConfiguration? -> Assertions.assertEquals(false, s!!.config.suggested) })

    Assertions.assertEquals(2, requestThree.catalog.streams.size)
    val firstStreamConfig =
      requestThree
        .catalog
        .streams[0]
        .config
    Assertions.assertEquals(true, firstStreamConfig.suggested)
    Assertions.assertEquals(true, firstStreamConfig.selected)
    val secondStreamConfig =
      requestThree
        .catalog
        .streams[1]
        .config
    Assertions.assertEquals(false, secondStreamConfig.suggested)
    Assertions.assertEquals(false, secondStreamConfig.selected)
  }

  @Test
  fun testCreateSourceHandleSecret() {
    val oauthDefinitionVersion =
      ActorDefinitionVersion()
        .withDockerImageTag("thelatesttag")
        .withSpec(ConnectorSpecificationHelpers.generateAdvancedAuthConnectorSpecification())

    val sourceHandlerSpy = spyk(sourceHandler)
    val sourceCreate =
      SourceCreate()
        .name(sourceConnection.name)
        .workspaceId(sourceConnection.workspaceId)
        .sourceDefinitionId(standardSourceDefinition.sourceDefinitionId)
        .connectionConfiguration(sourceConnection.configuration)

    every { sourceHandlerSpy.createSource(any()) } returns SourceRead()
    every { sourceHandlerSpy.hydrateOAuthResponseSecret(any(), any()) } returns emptyObject()
    every { sourceService.getStandardSourceDefinition(sourceCreate.sourceDefinitionId) } returns standardSourceDefinition
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        standardSourceDefinition,
        sourceCreate.workspaceId,
      )
    } returns oauthDefinitionVersion

    // Test that calling createSourceHandleSecret only hits old code path if nothing is passed for
    // secretId
    sourceHandlerSpy.createSourceWithOptionalSecret(sourceCreate)
    verify { sourceHandlerSpy.createSource(sourceCreate) }
    verify(exactly = 0) {
      sourceHandlerSpy.hydrateOAuthResponseSecret(any(), any())
    }

    // Test that calling createSourceHandleSecret hits new code path if we have a secretId set.
    val secretCoordinate = AirbyteManagedSecretCoordinate("airbyte_test", 1)
    sourceCreate.secretId = secretCoordinate.fullCoordinate
    sourceHandlerSpy.createSourceWithOptionalSecret(sourceCreate)
    verify(exactly = 2) { sourceHandlerSpy.createSource(sourceCreate) }
    verify { sourceHandlerSpy.hydrateOAuthResponseSecret(any(), any()) }
  }

  companion object {
    private const val API_KEY_FIELD = "apiKey"
    private const val API_KEY_VALUE = "987-xyz"
    private const val SHOES = "shoes"
    private const val SKU = "sku"
    private val airbyteCatalog: AirbyteCatalog =
      CatalogHelpers.createAirbyteCatalog(
        SHOES,
        Field.of(SKU, JsonSchemaType.STRING),
      )

    private const val ICON_URL = "https://connectors.airbyte.com/files/metadata/airbyte/destination-test/latest/icon.svg"
    private const val IS_VERSION_OVERRIDE_APPLIED = true
    private const val IS_ENTITLED = true
    private val SUPPORT_STATE = SupportState.SUPPORTED
    private const val DEFAULT_MEMORY = "2 GB"
    private const val DEFAULT_CPU = "2"
    private val RESOURCE_ALLOCATION: ScopedResourceRequirements? = getResourceRequirementsForSourceRequest(DEFAULT_CPU, DEFAULT_MEMORY)
    private val ORG_ID: UUID = UUID.randomUUID()

    fun getResourceRequirementsForSourceRequest(
      defaultCpuRequest: String?,
      defaultMemoryRequest: String?,
    ): ScopedResourceRequirements? =
      ScopedResourceRequirements()._default(ResourceRequirements().cpuRequest(defaultCpuRequest).memoryRequest(defaultMemoryRequest))
  }
}
