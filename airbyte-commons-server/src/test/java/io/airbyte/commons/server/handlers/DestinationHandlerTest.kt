/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.common.collect.Lists
import io.airbyte.api.model.generated.ActorListCursorPaginatedRequestBody
import io.airbyte.api.model.generated.ActorListFilters
import io.airbyte.api.model.generated.ActorStatus
import io.airbyte.api.model.generated.ConnectionReadList
import io.airbyte.api.model.generated.DestinationCreate
import io.airbyte.api.model.generated.DestinationDefinitionSpecificationRead
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.api.model.generated.DestinationSearch
import io.airbyte.api.model.generated.DestinationUpdate
import io.airbyte.api.model.generated.ResourceRequirements
import io.airbyte.api.model.generated.ScopedResourceRequirements
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
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.Configs
import io.airbyte.config.DestinationConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.config.secrets.ConfigWithSecretReferences
import io.airbyte.config.secrets.JsonSecretsProcessor
import io.airbyte.config.secrets.SecretsHelpers.SecretReferenceHelpers
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.config.secrets.buildConfigWithSecretRefsJava
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.shared.DestinationConnectionWithCount
import io.airbyte.data.services.shared.WorkspaceResourceCursorPagination
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.domain.services.entitlements.ConnectorConfigEntitlementService
import io.airbyte.domain.services.secrets.SecretPersistenceService
import io.airbyte.domain.services.secrets.SecretReferenceService
import io.airbyte.domain.services.secrets.SecretStorageService
import io.airbyte.persistence.job.WorkspaceHelper
import io.airbyte.persistence.job.factory.OAuthConfigSupplier
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.validation.json.JsonValidationException
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier

internal class DestinationHandlerTest {
  private lateinit var standardDestinationDefinition: StandardDestinationDefinition
  private lateinit var destinationDefinitionVersion: ActorDefinitionVersion
  private lateinit var destinationDefinitionVersionWithOverrideStatus: ActorDefinitionVersionWithOverrideStatus
  private lateinit var destinationDefinitionSpecificationRead: DestinationDefinitionSpecificationRead
  private lateinit var destinationConnection: DestinationConnection
  private lateinit var destinationConnectionWithCount: DestinationConnectionWithCount
  private lateinit var destinationHandler: DestinationHandler
  private lateinit var connectionsHandler: ConnectionsHandler
  private lateinit var configurationUpdate: ConfigurationUpdate
  private lateinit var validator: JsonSchemaValidator
  private lateinit var uuidGenerator: Supplier<UUID>
  private lateinit var secretsProcessor: JsonSecretsProcessor
  private lateinit var connectorSpecification: ConnectorSpecification
  private lateinit var oAuthConfigSupplier: OAuthConfigSupplier
  private lateinit var actorDefinitionVersionHelper: ActorDefinitionVersionHelper
  private lateinit var actorDefinitionVersionUpdater: ActorDefinitionVersionUpdater
  private lateinit var actorDefinitionHandlerHelper: ActorDefinitionHandlerHelper
  private lateinit var licenseEntitlementChecker: LicenseEntitlementChecker
  private lateinit var connectorConfigEntitlementService: ConnectorConfigEntitlementService
  private val apiPojoConverters = ApiPojoConverters(CatalogConverter(FieldGenerator(), mutableListOf()))

  private lateinit var destinationService: DestinationService
  private lateinit var workspaceHelper: WorkspaceHelper
  private lateinit var secretsRepositoryWriter: SecretsRepositoryWriter
  private lateinit var secretPersistenceService: SecretPersistenceService
  private lateinit var secretStorageService: SecretStorageService
  private lateinit var secretReferenceService: SecretReferenceService
  private lateinit var currentUserService: CurrentUserService
  private lateinit var secretPersistence: SecretPersistence

  @BeforeEach
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun setUp() {
    validator = mockk(relaxed = true)
    uuidGenerator = mockk(relaxed = true)
    connectionsHandler = mockk(relaxed = true)
    configurationUpdate = mockk(relaxed = true)
    secretsProcessor = mockk(relaxed = true)
    oAuthConfigSupplier = mockk(relaxed = true)
    actorDefinitionVersionHelper = mockk(relaxed = true)
    destinationService = mockk(relaxed = true)
    actorDefinitionHandlerHelper = mockk(relaxed = true)
    actorDefinitionVersionUpdater = mockk(relaxed = true)
    workspaceHelper = mockk(relaxed = true)
    licenseEntitlementChecker = mockk(relaxed = true)
    connectorConfigEntitlementService = mockk(relaxed = true)
    secretsRepositoryWriter = mockk(relaxed = true)
    secretPersistenceService = mockk(relaxed = true)
    secretStorageService = mockk(relaxed = true)
    secretReferenceService = mockk(relaxed = true)
    currentUserService = mockk(relaxed = true)
    secretPersistence = mockk(relaxed = true)

    connectorSpecification = ConnectorSpecificationHelpers.generateConnectorSpecification()

    standardDestinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
        .withName("db2")
        .withIconUrl(ICON_URL)

    destinationDefinitionVersion =
      ActorDefinitionVersion()
        .withDockerImageTag("thelatesttag")
        .withSpec(connectorSpecification)

    destinationDefinitionVersionWithOverrideStatus =
      ActorDefinitionVersionWithOverrideStatus(
        destinationDefinitionVersion,
        IS_VERSION_OVERRIDE_APPLIED,
      )

    destinationDefinitionSpecificationRead =
      DestinationDefinitionSpecificationRead()
        .connectionSpecification(connectorSpecification.getConnectionSpecification())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .documentationUrl(connectorSpecification.getDocumentationUrl().toString())

    destinationConnection =
      DestinationHelpers.generateDestination(
        standardDestinationDefinition.getDestinationDefinitionId(),
        apiPojoConverters.scopedResourceReqsToInternal(RESOURCE_ALLOCATION),
      )

    destinationConnectionWithCount = DestinationHelpers.generateDestinationWithCount(destinationConnection)

    destinationHandler =
      DestinationHandler(
        validator,
        connectionsHandler,
        uuidGenerator,
        secretsProcessor,
        configurationUpdate,
        oAuthConfigSupplier,
        actorDefinitionVersionHelper,
        destinationService,
        actorDefinitionHandlerHelper,
        actorDefinitionVersionUpdater,
        apiPojoConverters,
        workspaceHelper,
        licenseEntitlementChecker,
        Configs.AirbyteEdition.COMMUNITY,
        secretsRepositoryWriter,
        secretPersistenceService,
        secretStorageService,
        secretReferenceService,
        currentUserService,
        connectorConfigEntitlementService,
      )

    every {
      actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(
        standardDestinationDefinition,
        destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId(),
      )
    } returns destinationDefinitionVersionWithOverrideStatus
    every { workspaceHelper.getOrganizationForWorkspace(any()) } returns UUID.randomUUID()
    every {
      licenseEntitlementChecker.checkEntitlement(
        any(),
        Entitlement.DESTINATION_CONNECTOR,
        any(),
      )
    } returns IS_ENTITLED
    every { secretPersistenceService.getPersistenceFromWorkspaceId(any()) } returns secretPersistence
    every { actorDefinitionHandlerHelper.getVersionBreakingChanges(any()) } returns Optional.empty()
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun testCreateDestination() {
    // ===== GIVEN =====
    // Create the DestinationCreate request with the necessary fields.
    val destinationCreate =
      DestinationCreate()
        .name(destinationConnection.getName())
        .workspaceId(destinationConnection.getWorkspaceId())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .connectionConfiguration(destinationConnection.getConfiguration())
        .resourceAllocation(RESOURCE_ALLOCATION)

    // Set up basic mocks.
    every { uuidGenerator.get() } returns destinationConnection.getDestinationId()
    every {
      destinationService.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId())
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.getWorkspaceId(),
      )
    } returns destinationDefinitionVersion
    every {
      oAuthConfigSupplier.maskDestinationOAuthParameters(
        destinationDefinitionSpecificationRead.getDestinationDefinitionId(),
        destinationConnection.getWorkspaceId(),
        destinationCreate.getConnectionConfiguration(),
        destinationDefinitionVersion.getSpec(),
      )
    } returns destinationCreate.getConnectionConfiguration()
    every {
      actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(
        standardDestinationDefinition,
        destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId(),
      )
    } returns destinationDefinitionVersionWithOverrideStatus

    // Set up current user context.
    val currentUserId = UUID.randomUUID()
    every { currentUserService.getCurrentUserIdIfExists() } returns Optional.of(currentUserId)

    // Set up secret storage mocks.
    val secretStorage = mockk<SecretStorage>()
    val secretStorageId = UUID.randomUUID()
    every { secretStorage.id } returns SecretStorageId(secretStorageId)
    every { secretStorageService.getByWorkspaceId(any()) } returns secretStorage

    // Set up secret reference service mocks for the input configuration.
    val configWithRefs = buildConfigWithSecretRefsJava(destinationConnection.getConfiguration())
    every {
      secretReferenceService.getConfigWithSecretReferences(
        any(),
        destinationCreate.getConnectionConfiguration(),
        any(),
      )
    } returns configWithRefs

    // Simulate secret persistence and reference ID insertion.
    val configWithProcessedSecrets =
      SecretReferenceHelpers.processConfigSecrets(
        destinationCreate.getConnectionConfiguration(),
        destinationDefinitionSpecificationRead.getConnectionSpecification(),
        SecretStorageId(secretStorageId),
      )
    every {
      secretsRepositoryWriter.createFromConfig(
        destinationConnection.getWorkspaceId(),
        configWithProcessedSecrets,
        secretPersistence,
      )
    } returns destinationCreate.getConnectionConfiguration()

    val configWithSecretRefIds = clone(destinationCreate.getConnectionConfiguration())
    (configWithSecretRefIds as ObjectNode).put("updated_with", "secret_reference_ids")
    every {
      secretReferenceService.createAndInsertSecretReferencesWithStorageId(
        configWithProcessedSecrets,
        any(),
        any(),
        any(),
        any(),
      )
    } returns SecretReferenceHelpers.ConfigWithSecretReferenceIdsInjected(configWithSecretRefIds)

    // Mock the persisted destination connection that is retrieved after creation.
    val persistedConnection = clone(destinationConnection).withConfiguration(configWithSecretRefIds)
    every {
      destinationService.getDestinationConnection(destinationConnection.getDestinationId())
    } returns persistedConnection
    val configWithRefsAfterPersist = buildConfigWithSecretRefsJava(configWithSecretRefIds)
    every {
      secretReferenceService.getConfigWithSecretReferences(
        any(),
        configWithSecretRefIds,
        any(),
      )
    } returns configWithRefsAfterPersist

    // Prepare secret output.
    every {
      secretsProcessor.prepareSecretsForOutput(
        configWithSecretRefIds,
        destinationDefinitionSpecificationRead.getConnectionSpecification(),
      )
    } returns configWithSecretRefIds

    // ===== WHEN =====
    val actualDestinationRead = destinationHandler.createDestination(destinationCreate)

    // ===== THEN =====
    val expectedDestinationRead =
      DestinationHelpers
        .getDestinationRead(
          destinationConnection,
          standardDestinationDefinition,
          IS_VERSION_OVERRIDE_APPLIED,
          IS_ENTITLED,
          SUPPORT_STATE,
          RESOURCE_ALLOCATION,
        ).connectionConfiguration(configWithSecretRefIds)

    Assertions.assertEquals(expectedDestinationRead, actualDestinationRead)

    verify {
      secretsProcessor.prepareSecretsForOutput(configWithSecretRefIds, destinationDefinitionSpecificationRead.getConnectionSpecification())
    }
    verify {
      oAuthConfigSupplier.maskDestinationOAuthParameters(
        destinationDefinitionSpecificationRead.getDestinationDefinitionId(),
        destinationConnection.getWorkspaceId(),
        destinationCreate.getConnectionConfiguration(),
        destinationDefinitionVersion.getSpec(),
      )
    }
    verify { destinationService.writeDestinationConnectionNoSecrets(persistedConnection) }
    verify {
      actorDefinitionVersionHelper.getDestinationVersion(standardDestinationDefinition, destinationConnection.getWorkspaceId())
    }
    verify {
      validator.ensure(destinationDefinitionSpecificationRead.getConnectionSpecification(), destinationCreate.getConnectionConfiguration())
    }
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun testCreateDestinationNoEntitlementThrows() {
    every { uuidGenerator.get() } returns destinationConnection.getDestinationId()
    every {
      destinationService.getDestinationConnection(destinationConnection.getDestinationId())
    } returns destinationConnection
    every {
      destinationService.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId())
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.getWorkspaceId(),
      )
    } returns destinationDefinitionVersion

    val destinationCreate =
      DestinationCreate()
        .name(destinationConnection.getName())
        .workspaceId(destinationConnection.getWorkspaceId())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .connectionConfiguration(DestinationHelpers.testDestinationJson)
        .resourceAllocation(RESOURCE_ALLOCATION)

    // Not entitled
    every {
      licenseEntitlementChecker.ensureEntitled(
        any(),
        Entitlement.DESTINATION_CONNECTOR,
        standardDestinationDefinition.getDestinationDefinitionId(),
      )
    } throws LicenseEntitlementProblem()

    Assertions.assertThrows(
      LicenseEntitlementProblem::class.java,
      { destinationHandler.createDestination(destinationCreate) },
    )

    verify {
      validator.ensure(destinationDefinitionSpecificationRead.getConnectionSpecification(), destinationConnection.getConfiguration())
    }
    verify {
      actorDefinitionVersionHelper.getDestinationVersion(standardDestinationDefinition, destinationConnection.getWorkspaceId())
    }
  }

  @Test
  @Throws(JsonValidationException::class, io.airbyte.data.ConfigNotFoundException::class, IOException::class)
  fun testCreateDestinationChecksConfigEntitlements() {
    every { uuidGenerator.get() } returns destinationConnection.getDestinationId()
    every {
      destinationService.getDestinationConnection(destinationConnection.getDestinationId())
    } returns destinationConnection
    every {
      destinationService.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId())
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.getWorkspaceId(),
      )
    } returns destinationDefinitionVersion

    val destinationCreate =
      DestinationCreate()
        .name(destinationConnection.getName())
        .workspaceId(destinationConnection.getWorkspaceId())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .connectionConfiguration(DestinationHelpers.testDestinationJson)
        .resourceAllocation(RESOURCE_ALLOCATION)

    // Not entitled
    every {
      connectorConfigEntitlementService.ensureEntitledConfig(
        any(),
        destinationDefinitionVersion,
        destinationConnection.getConfiguration(),
      )
    } throws LicenseEntitlementProblem()

    Assertions.assertThrows(
      LicenseEntitlementProblem::class.java,
      { destinationHandler.createDestination(destinationCreate) },
    )

    verify {
      validator.ensure(destinationDefinitionSpecificationRead.getConnectionSpecification(), destinationConnection.getConfiguration())
    }
    verify {
      actorDefinitionVersionHelper.getDestinationVersion(standardDestinationDefinition, destinationConnection.getWorkspaceId())
    }
  }

  @Test
  @Throws(IOException::class)
  fun testNonNullCreateDestinationThrowsOnInvalidResourceAllocation() {
    val cloudDestinationHandler =
      DestinationHandler(
        validator,
        connectionsHandler,
        uuidGenerator,
        secretsProcessor,
        configurationUpdate,
        oAuthConfigSupplier,
        actorDefinitionVersionHelper,
        destinationService,
        actorDefinitionHandlerHelper,
        actorDefinitionVersionUpdater,
        apiPojoConverters,
        workspaceHelper,
        licenseEntitlementChecker,
        Configs.AirbyteEdition.CLOUD,
        secretsRepositoryWriter,
        secretPersistenceService,
        secretStorageService,
        secretReferenceService,
        currentUserService,
        connectorConfigEntitlementService,
      )

    val destinationCreate =
      DestinationCreate()
        .name(destinationConnection.getName())
        .workspaceId(destinationConnection.getWorkspaceId())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .connectionConfiguration(DestinationHelpers.testDestinationJson)
        .resourceAllocation(RESOURCE_ALLOCATION)

    Assertions.assertThrows(
      BadRequestException::class.java,
      { cloudDestinationHandler.createDestination(destinationCreate) },
      "Expected createDestination to throw BadRequestException",
    )
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun testUpdateDestination() {
    // ===== GIVEN =====
    // Update the destination name and configuration.
    val updatedDestName = "my updated dest name"
    val newConfiguration = clone(destinationConnection.getConfiguration())
    (newConfiguration as ObjectNode).put(API_KEY_FIELD, API_KEY_VALUE)
    val newResourceAllocation: ScopedResourceRequirements? = getResourceRequirementsForDestinationRequest("3", "3 GB")

    val updatedDestinationConnection =
      clone(destinationConnection)
        .withName(updatedDestName)
        .withConfiguration(newConfiguration)
        .withTombstone(false)
        .withResourceRequirements(apiPojoConverters.scopedResourceReqsToInternal(newResourceAllocation))

    val destinationUpdate =
      DestinationUpdate()
        .name(updatedDestName)
        .destinationId(destinationConnection.getDestinationId())
        .connectionConfiguration(newConfiguration)
        .resourceAllocation(newResourceAllocation)

    // Set up basic mocks for the update.
    every {
      oAuthConfigSupplier.maskDestinationOAuthParameters(
        destinationDefinitionSpecificationRead.getDestinationDefinitionId(),
        destinationConnection.getWorkspaceId(),
        newConfiguration,
        destinationDefinitionVersion.getSpec(),
      )
    } returns newConfiguration
    every {
      destinationService.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId())
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId(),
      )
    } returns destinationDefinitionVersion
    every {
      destinationService.getDestinationDefinitionFromDestination(destinationConnection.getDestinationId())
    } returns standardDestinationDefinition
    every {
      configurationUpdate.destination(
        destinationConnection.getDestinationId(),
        updatedDestName,
        newConfiguration,
      )
    } returns updatedDestinationConnection
    every {
      actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(
        standardDestinationDefinition,
        destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId(),
      )
    } returns destinationDefinitionVersionWithOverrideStatus
    every {
      destinationService.getDestinationConnectionIfExists(destinationConnection.getDestinationId())
    } returns Optional.of(destinationConnection)

    // Set up current user context.
    val currentUserId = UUID.randomUUID()
    every { currentUserService.getCurrentUserIdIfExists() } returns Optional.of(currentUserId)

    // Set up secret storage mocks.
    val secretStorage = mockk<SecretStorage>()
    val secretStorageId = UUID.randomUUID()
    every { secretStorage.id } returns SecretStorageId(secretStorageId)
    every { secretStorageService.getByWorkspaceId(any()) } returns secretStorage

    // Set up secret reference service mocks for the previous config.
    val previousConfigWithRefs = buildConfigWithSecretRefsJava(destinationConnection.getConfiguration())
    every {
      secretReferenceService.getConfigWithSecretReferences(
        ActorId(destinationConnection.getDestinationId()),
        destinationConnection.getConfiguration(),
        WorkspaceId(destinationConnection.getWorkspaceId()),
      )
    } returns previousConfigWithRefs

    // Simulate the secret update and reference ID creation/insertion.
    val newConfigWithProcessedSecrets =
      SecretReferenceHelpers.processConfigSecrets(
        newConfiguration,
        destinationDefinitionSpecificationRead.getConnectionSpecification(),
        SecretStorageId(secretStorageId),
      )
    every {
      secretsRepositoryWriter.updateFromConfig(
        destinationConnection.getWorkspaceId(),
        previousConfigWithRefs,
        newConfigWithProcessedSecrets,
        destinationDefinitionVersion.getSpec().getConnectionSpecification(),
        secretPersistence,
      )
    } returns newConfigWithProcessedSecrets.originalConfig

    val newConfigWithSecretRefIds = clone(newConfiguration)
    (newConfigWithSecretRefIds as ObjectNode).put("updated_with", "secret_reference_ids")
    every {
      secretReferenceService.createAndInsertSecretReferencesWithStorageId(
        newConfigWithProcessedSecrets,
        ActorId(destinationConnection.getDestinationId()),
        WorkspaceId(destinationConnection.getWorkspaceId()),
        SecretStorageId(secretStorageId),
        any(),
      )
    } returns SecretReferenceHelpers.ConfigWithSecretReferenceIdsInjected(newConfigWithSecretRefIds)

    // Mock the updated config that is persisted and retrieved for building the destination read.
    val updatedDestinationWithSecretRefIds =
      clone(updatedDestinationConnection).withConfiguration(newConfigWithSecretRefIds)
    every {
      destinationService.getDestinationConnection(destinationConnection.getDestinationId())
    } returns updatedDestinationWithSecretRefIds

    val configWithRefsAfterPersist = buildConfigWithSecretRefsJava(newConfigWithSecretRefIds)
    every {
      secretReferenceService.getConfigWithSecretReferences(
        ActorId(destinationConnection.getDestinationId()),
        newConfigWithSecretRefIds,
        WorkspaceId(destinationConnection.getWorkspaceId()),
      )
    } returns configWithRefsAfterPersist

    // Prepare secret output.
    every {
      secretsProcessor.prepareSecretsForOutput(
        newConfigWithSecretRefIds,
        destinationDefinitionSpecificationRead.getConnectionSpecification(),
      )
    } returns newConfigWithSecretRefIds

    // ===== WHEN =====
    // Call the method under test.
    val actualDestinationRead = destinationHandler.updateDestination(destinationUpdate)

    // ===== THEN =====
    // Build the expected DestinationRead using the updated configuration.
    val expectedDestinationRead =
      DestinationHelpers
        .getDestinationRead(
          updatedDestinationWithSecretRefIds,
          standardDestinationDefinition,
          IS_VERSION_OVERRIDE_APPLIED,
          IS_ENTITLED,
          SUPPORT_STATE,
          newResourceAllocation,
        ).connectionConfiguration(newConfigWithSecretRefIds)

    Assertions.assertEquals(expectedDestinationRead, actualDestinationRead)

    verify {
      secretsProcessor.prepareSecretsForOutput(newConfigWithSecretRefIds, destinationDefinitionSpecificationRead.getConnectionSpecification())
    }
    verify {
      oAuthConfigSupplier.maskDestinationOAuthParameters(
        destinationDefinitionSpecificationRead.getDestinationDefinitionId(),
        destinationConnection.getWorkspaceId(),
        newConfiguration,
        destinationDefinitionVersion.getSpec(),
      )
    }
    verify { destinationService.writeDestinationConnectionNoSecrets(updatedDestinationWithSecretRefIds) }
    verify {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId(),
      )
    }
    verify {
      validator.ensure(destinationDefinitionSpecificationRead.getConnectionSpecification(), newConfiguration)
    }
  }

  @Test
  @Throws(JsonValidationException::class, io.airbyte.data.ConfigNotFoundException::class, IOException::class, ConfigNotFoundException::class)
  fun testUpdateDestinationChecksConfigEntitlements() {
    val updatedDestName = "my updated dest name for config entitlements"
    val newConfiguration = destinationConnection.getConfiguration()
    (newConfiguration as ObjectNode).put(API_KEY_FIELD, API_KEY_VALUE)
    val newResourceAllocation: ScopedResourceRequirements? = getResourceRequirementsForDestinationRequest("1", "1 GB")
    val destinationUpdate =
      DestinationUpdate()
        .name(updatedDestName)
        .destinationId(destinationConnection.getDestinationId())
        .connectionConfiguration(newConfiguration)
        .resourceAllocation(newResourceAllocation)

    val expectedDestinationConnection =
      clone(destinationConnection)
        .withName(updatedDestName)
        .withConfiguration(newConfiguration)
        .withTombstone(false)
        .withResourceRequirements(apiPojoConverters.scopedResourceReqsToInternal(newResourceAllocation))

    every {
      secretsProcessor
        .copySecrets(
          destinationConnection.getConfiguration(),
          newConfiguration,
          destinationDefinitionSpecificationRead.getConnectionSpecification(),
        )
    } returns newConfiguration
    every {
      destinationService.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId())
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId(),
      )
    } returns destinationDefinitionVersion
    every {
      destinationService.getDestinationDefinitionFromDestination(destinationConnection.getDestinationId())
    } returns standardDestinationDefinition
    every {
      destinationService.getDestinationConnection(destinationConnection.getDestinationId())
    } returns expectedDestinationConnection
    every {
      configurationUpdate.destination(
        destinationConnection.getDestinationId(),
        updatedDestName,
        newConfiguration,
      )
    } returns expectedDestinationConnection

    // Not entitled
    every {
      connectorConfigEntitlementService
        .ensureEntitledConfig(
          any(),
          destinationDefinitionVersion,
          newConfiguration,
        )
    } throws LicenseEntitlementProblem()

    Assertions.assertThrows(
      LicenseEntitlementProblem::class.java,
      { destinationHandler.updateDestination(destinationUpdate) },
    )

    verify {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId(),
      )
    }
    verify {
      validator.ensure(destinationDefinitionSpecificationRead.getConnectionSpecification(), newConfiguration)
    }
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun testUpdateDestinationNoEntitlementThrows() {
    val updatedDestName = "my updated dest name"
    val newConfiguration = destinationConnection.getConfiguration()
    (newConfiguration as ObjectNode).put(API_KEY_FIELD, API_KEY_VALUE)
    val newResourceAllocation: ScopedResourceRequirements? = getResourceRequirementsForDestinationRequest("3", "3 GB")
    val destinationUpdate =
      DestinationUpdate()
        .name(updatedDestName)
        .destinationId(destinationConnection.getDestinationId())
        .connectionConfiguration(newConfiguration)
        .resourceAllocation(newResourceAllocation)

    val expectedDestinationConnection =
      clone(destinationConnection)
        .withName(updatedDestName)
        .withConfiguration(newConfiguration)
        .withTombstone(false)
        .withResourceRequirements(apiPojoConverters.scopedResourceReqsToInternal(newResourceAllocation))

    every {
      secretsProcessor
        .copySecrets(
          destinationConnection.getConfiguration(),
          newConfiguration,
          destinationDefinitionSpecificationRead.getConnectionSpecification(),
        )
    } returns newConfiguration
    every {
      destinationService.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId())
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId(),
      )
    } returns destinationDefinitionVersion
    every {
      destinationService.getDestinationDefinitionFromDestination(destinationConnection.getDestinationId())
    } returns standardDestinationDefinition
    every {
      destinationService.getDestinationConnection(destinationConnection.getDestinationId())
    } returns expectedDestinationConnection
    every {
      configurationUpdate.destination(
        destinationConnection.getDestinationId(),
        updatedDestName,
        newConfiguration,
      )
    } returns expectedDestinationConnection

    // Not entitled
    every {
      licenseEntitlementChecker
        .ensureEntitled(
          any(),
          Entitlement.DESTINATION_CONNECTOR,
          standardDestinationDefinition.getDestinationDefinitionId(),
        )
    } throws LicenseEntitlementProblem()

    Assertions.assertThrows(
      LicenseEntitlementProblem::class.java,
      { destinationHandler.updateDestination(destinationUpdate) },
    )

    verify {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId(),
      )
    }
    verify {
      validator.ensure(destinationDefinitionSpecificationRead.getConnectionSpecification(), newConfiguration)
    }
  }

  @Test
  fun testNonNullUpdateDestinationThrowsOnInvalidResourceAllocation() {
    val cloudDestinationHandler =
      DestinationHandler(
        validator,
        connectionsHandler,
        uuidGenerator,
        secretsProcessor,
        configurationUpdate,
        oAuthConfigSupplier,
        actorDefinitionVersionHelper,
        destinationService,
        actorDefinitionHandlerHelper,
        actorDefinitionVersionUpdater,
        apiPojoConverters,
        workspaceHelper,
        licenseEntitlementChecker,
        Configs.AirbyteEdition.CLOUD,
        secretsRepositoryWriter,
        secretPersistenceService,
        secretStorageService,
        secretReferenceService,
        currentUserService,
        connectorConfigEntitlementService,
      )

    val updatedDestName = "my updated dest name"
    val newConfiguration = destinationConnection.getConfiguration()
    (newConfiguration as ObjectNode).put(API_KEY_FIELD, API_KEY_VALUE)
    val newResourceAllocation: ScopedResourceRequirements? = getResourceRequirementsForDestinationRequest("3", "3 GB")
    val destinationUpdate =
      DestinationUpdate()
        .name(updatedDestName)
        .destinationId(destinationConnection.getDestinationId())
        .connectionConfiguration(newConfiguration)
        .resourceAllocation(newResourceAllocation)

    Assertions.assertThrows(
      BadRequestException::class.java,
      { cloudDestinationHandler.updateDestination(destinationUpdate) },
      "Expected updateDestination to throw BadRequestException",
    )
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun testUpgradeDestinationVersion() {
    val requestBody = DestinationIdRequestBody().destinationId(destinationConnection.getDestinationId())

    every {
      destinationService.getDestinationConnection(destinationConnection.getDestinationId())
    } returns destinationConnection
    every {
      destinationService.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId())
    } returns standardDestinationDefinition

    destinationHandler.upgradeDestinationVersion(requestBody)

    // validate that we call the actorDefinitionVersionUpdater to upgrade the version to global default
    verify {
      actorDefinitionVersionUpdater.upgradeActorVersion(destinationConnection, standardDestinationDefinition)
    }
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun testGetDestination() {
    val expectedDestinationRead =
      DestinationRead()
        .name(destinationConnection.getName())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .workspaceId(destinationConnection.getWorkspaceId())
        .destinationId(destinationConnection.getDestinationId())
        .connectionConfiguration(destinationConnection.getConfiguration())
        .destinationName(standardDestinationDefinition.getName())
        .icon(ICON_URL)
        .isEntitled(IS_ENTITLED)
        .isVersionOverrideApplied(IS_VERSION_OVERRIDE_APPLIED)
        .supportState(SUPPORT_STATE)
        .resourceAllocation(RESOURCE_ALLOCATION)
    val destinationIdRequestBody =
      DestinationIdRequestBody().destinationId(expectedDestinationRead.getDestinationId())

    every {
      secretsProcessor.prepareSecretsForOutput(
        destinationConnection.getConfiguration(),
        destinationDefinitionSpecificationRead.getConnectionSpecification(),
      )
    } returns destinationConnection.getConfiguration()
    every {
      destinationService.getDestinationConnection(destinationConnection.getDestinationId())
    } returns destinationConnection
    every {
      destinationService.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId())
    } returns standardDestinationDefinition
    every {
      destinationService.getDestinationDefinitionFromDestination(destinationConnection.getDestinationId())
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId(),
      )
    } returns destinationDefinitionVersion
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

    val actualDestinationRead = destinationHandler.getDestination(destinationIdRequestBody)

    Assertions.assertEquals(expectedDestinationRead, actualDestinationRead)

    // make sure the icon was loaded into actual svg content
    Assertions.assertTrue(expectedDestinationRead.getIcon().startsWith("https://"))

    verify {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId(),
      )
    }
    verify {
      secretsProcessor.prepareSecretsForOutput(
        destinationConnection.getConfiguration(),
        destinationDefinitionSpecificationRead.getConnectionSpecification(),
      )
    }
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun testListDestinationForWorkspace() {
    val expectedDestinationRead =
      DestinationRead()
        .name(destinationConnectionWithCount.destination.getName())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .workspaceId(destinationConnectionWithCount.destination.getWorkspaceId())
        .destinationId(destinationConnectionWithCount.destination.getDestinationId())
        .connectionConfiguration(destinationConnectionWithCount.destination.getConfiguration())
        .destinationName(standardDestinationDefinition.getName())
        .icon(ICON_URL)
        .isEntitled(IS_ENTITLED)
        .isVersionOverrideApplied(IS_VERSION_OVERRIDE_APPLIED)
        .supportState(SUPPORT_STATE)
        .status(ActorStatus.ACTIVE)
        .resourceAllocation(RESOURCE_ALLOCATION)
        .numConnections(0)
    val workspaceIdRequestBody =
      WorkspaceIdRequestBody().workspaceId(destinationConnectionWithCount.destination.getWorkspaceId())

    every {
      destinationService.getDestinationConnection(destinationConnectionWithCount.destination.getDestinationId())
    } returns destinationConnectionWithCount.destination
    every {
      destinationService.buildCursorPagination(
        any(),
        any(),
        any(),
        any(),
        any(),
      )
    } returns WorkspaceResourceCursorPagination(null, 10)
    every {
      destinationService.countWorkspaceDestinationsFiltered(any(), any())
    } returns 1
    every {
      destinationService.listWorkspaceDestinationConnectionsWithCounts(any(), any())
    } returns listOf(destinationConnectionWithCount)
    every {
      destinationService.getStandardDestinationDefinition(
        standardDestinationDefinition.getDestinationDefinitionId(),
      )
    } returns standardDestinationDefinition
    every {
      destinationService.getDestinationDefinitionFromDestination(
        destinationConnectionWithCount.destination.getDestinationId(),
      )
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnectionWithCount.destination.getWorkspaceId(),
        destinationConnectionWithCount.destination.getDestinationId(),
      )
    } returns destinationDefinitionVersion
    every {
      secretsProcessor.prepareSecretsForOutput(
        destinationConnectionWithCount.destination.getConfiguration(),
        destinationDefinitionSpecificationRead.getConnectionSpecification(),
      )
    } returns destinationConnectionWithCount.destination.getConfiguration()
    every {
      secretReferenceService.getConfigWithSecretReferences(any(), any(), any())
    } answers {
      ConfigWithSecretReferences(
        secondArg(),
        mapOf(),
      )
    }

    val actualDestinationRead =
      destinationHandler
        .listDestinationsForWorkspace(
          ActorListCursorPaginatedRequestBody()
            .workspaceId(workspaceIdRequestBody.getWorkspaceId())
            .pageSize(10),
        )

    Assertions.assertEquals(expectedDestinationRead, actualDestinationRead.getDestinations().get(0))
    Assertions.assertEquals(1, actualDestinationRead.getNumConnections())
    Assertions.assertEquals(10, actualDestinationRead.getPageSize())

    verify {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnectionWithCount.destination.getWorkspaceId(),
        destinationConnectionWithCount.destination.getDestinationId(),
      )
    }
    verify {
      secretsProcessor.prepareSecretsForOutput(
        destinationConnectionWithCount.destination.getConfiguration(),
        destinationDefinitionSpecificationRead.getConnectionSpecification(),
      )
    }
    verify {
      destinationService.buildCursorPagination(
        any(),
        any(),
        any(),
        any(),
        any(),
      )
    }

    verify {
      destinationService.countWorkspaceDestinationsFiltered(
        any(),
        any(),
      )
    }

    verify {
      destinationService.listWorkspaceDestinationConnectionsWithCounts(
        any(),
        any(),
      )
    }
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun testListDestinationsForWorkspaceWithPagination() {
    // Create multiple destinations for pagination testing
    val destination1 = createDestinationConnectionWithCount("dest1", 2)
    val destination2 = createDestinationConnectionWithCount("dest2", 1)
    val destinations: List<DestinationConnectionWithCount> =
      listOf(destination1, destination2)

    val workspaceId = destination1.destination.getWorkspaceId()
    val requestBody =
      ActorListCursorPaginatedRequestBody()
        .workspaceId(workspaceId)
        .pageSize(10)

    every {
      destinationService.buildCursorPagination(
        any(),
        any(),
        any(),
        any(),
        any(),
      )
    } returns WorkspaceResourceCursorPagination(null, 10)
    every {
      destinationService.countWorkspaceDestinationsFiltered(
        any(),
        any(),
      )
    } returns 2
    every {
      destinationService.listWorkspaceDestinationConnectionsWithCounts(
        any(),
        any(),
      )
    } returns destinations

    every {
      destinationService.getStandardDestinationDefinition(any())
    } returns standardDestinationDefinition
    every {
      destinationService.getDestinationDefinitionFromDestination(any())
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        any(),
        any(),
        any(),
      )
    } returns destinationDefinitionVersion
    every {
      actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(
        any(),
        any(),
        any(),
      )
    } returns
      ActorDefinitionVersionWithOverrideStatus(
        destinationDefinitionVersion,
        false,
      )
    every {
      secretsProcessor.prepareSecretsForOutput(
        any(),
        any(),
      )
    } returns emptyObject()
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

    // Execute the test
    val result = destinationHandler.listDestinationsForWorkspace(requestBody)

    // Verify the results
    Assertions.assertNotNull(result)
    Assertions.assertEquals(2, result.getDestinations().size)
    Assertions.assertEquals(2, result.getNumConnections())
    Assertions.assertEquals(10, result.getPageSize())

    // Verify the first destination
    val firstDestination = result.getDestinations().get(0)
    Assertions.assertEquals("dest1", firstDestination.getName())
    Assertions.assertEquals(2, firstDestination.getNumConnections())
    Assertions.assertEquals(ActorStatus.ACTIVE, firstDestination.getStatus()) // Has connections, so should be active

    // Verify the second destination
    val secondDestination = result.getDestinations().get(1)
    Assertions.assertEquals("dest2", secondDestination.getName())
    Assertions.assertEquals(1, secondDestination.getNumConnections())
    Assertions.assertEquals(ActorStatus.ACTIVE, secondDestination.getStatus()) // Has connections, so should be active

    verify {
      destinationService.buildCursorPagination(
        any(),
        any(),
        any(),
        any(),
        any(),
      )
    }

    verify {
      destinationService.countWorkspaceDestinationsFiltered(
        workspaceId,
        any(),
      )
    }

    verify {
      destinationService.listWorkspaceDestinationConnectionsWithCounts(
        workspaceId,
        any(),
      )
    }
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun testListDestinationsForWorkspaceWithFilters() {
    val workspaceId = destinationConnectionWithCount.destination.getWorkspaceId()
    val requestBody =
      ActorListCursorPaginatedRequestBody()
        .workspaceId(workspaceId)
        .pageSize(5)
        .filters(
          ActorListFilters()
            .searchTerm("test")
            .states(Lists.newArrayList(ActorStatus.ACTIVE)),
        )

    every {
      destinationService.buildCursorPagination(
        any(),
        any(),
        any(),
        any(),
        any(),
      )
    } returns WorkspaceResourceCursorPagination(null, 5)

    every {
      destinationService.countWorkspaceDestinationsFiltered(
        any(),
        any(),
      )
    } returns 1

    every {
      destinationService.listWorkspaceDestinationConnectionsWithCounts(
        any(),
        any(),
      )
    } returns Lists.newArrayList(destinationConnectionWithCount)

    every {
      destinationService.getStandardDestinationDefinition(any())
    } returns standardDestinationDefinition
    every {
      destinationService.getDestinationDefinitionFromDestination(any())
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        any(),
        any(),
        any(),
      )
    } returns destinationDefinitionVersion
    every {
      actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(
        any(),
        any(),
        any(),
      )
    } returns
      ActorDefinitionVersionWithOverrideStatus(
        destinationDefinitionVersion,
        false,
      )
    every {
      secretsProcessor.prepareSecretsForOutput(
        any(),
        any(),
      )
    } returns destinationConnectionWithCount.destination.getConfiguration()
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

    // Execute the test
    val result = destinationHandler.listDestinationsForWorkspace(requestBody)

    // Verify the results
    Assertions.assertNotNull(result)
    Assertions.assertEquals(1, result.getDestinations().size)
    Assertions.assertEquals(1, result.getNumConnections())
    Assertions.assertEquals(5, result.getPageSize())

    verify {
      destinationService.buildCursorPagination(
        any(),
        any(),
        any(),
        true,
        5,
      )
    }

    verify {
      destinationService.countWorkspaceDestinationsFiltered(
        workspaceId,
        any(),
      )
    }

    verify {
      destinationService.listWorkspaceDestinationConnectionsWithCounts(
        workspaceId,
        any(),
      )
    }
  }

  private fun createDestinationConnectionWithCount(
    name: String?,
    connectionCount: Int,
  ): DestinationConnectionWithCount {
    val destination =
      DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withWorkspaceId(UUID.randomUUID())
        .withDestinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .withName(name)
        .withConfiguration(emptyObject())
        .withTombstone(false)

    return DestinationConnectionWithCount(
      destination,
      "destination-definition",
      connectionCount,
      null,
      mapOf(),
      true,
    )
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun testDeleteDestinationAndDeleteSecrets() {
    val newConfiguration = destinationConnection.getConfiguration()
    (newConfiguration as ObjectNode).put(API_KEY_FIELD, API_KEY_VALUE)

    val expectedSourceConnection = clone(destinationConnection).withTombstone(true)

    val destinationIdRequestBody = DestinationIdRequestBody().destinationId(destinationConnection.getDestinationId())
    val standardSync = ConnectionHelpers.generateSyncWithDestinationId(destinationConnection.getDestinationId())
    standardSync.setBreakingChange(false)
    val connectionRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync)
    val connectionReadList = ConnectionReadList().connections(mutableListOf(connectionRead))
    val workspaceIdRequestBody = WorkspaceIdRequestBody().workspaceId(destinationConnection.getWorkspaceId())

    every {
      destinationService.getDestinationConnection(destinationConnection.getDestinationId())
    } returns destinationConnection andThen expectedSourceConnection
    every {
      oAuthConfigSupplier.maskSourceOAuthParameters(
        destinationDefinitionSpecificationRead.getDestinationDefinitionId(),
        destinationConnection.getWorkspaceId(),
        newConfiguration,
        destinationDefinitionVersion.getSpec(),
      )
    } returns newConfiguration
    every {
      destinationService.getStandardDestinationDefinition(destinationDefinitionSpecificationRead.getDestinationDefinitionId())
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId(),
      )
    } returns destinationDefinitionVersion
    every {
      destinationService.getDestinationDefinitionFromDestination(destinationConnection.getDestinationId())
    } returns standardDestinationDefinition
    every { connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody) } returns connectionReadList
    every {
      secretsProcessor.prepareSecretsForOutput(
        destinationConnection.getConfiguration(),
        destinationDefinitionSpecificationRead.getConnectionSpecification(),
      )
    } returns destinationConnection.getConfiguration()
    every {
      actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(
        standardDestinationDefinition,
        destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId(),
      )
    } returns destinationDefinitionVersionWithOverrideStatus
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

    destinationHandler.deleteDestination(destinationIdRequestBody)

    // We should not no longer get secrets or write secrets anymore (since we are deleting the
    // destination).
    verify(exactly = 0) { destinationService.writeDestinationConnectionNoSecrets(expectedSourceConnection) }
    verify {
      destinationService.tombstoneDestination(any(), any(), any())
    }
    verify { connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody) }
    verify { connectionsHandler.deleteConnection(connectionRead.getConnectionId()) }
    verify { secretReferenceService.deleteActorSecretReferences(ActorId(destinationConnection.getDestinationId())) }
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, io.airbyte.data.ConfigNotFoundException::class)
  fun testSearchDestinations() {
    val expectedDestinationRead =
      DestinationRead()
        .name(destinationConnection.getName())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .workspaceId(destinationConnection.getWorkspaceId())
        .destinationId(destinationConnection.getDestinationId())
        .connectionConfiguration(destinationConnection.getConfiguration())
        .destinationName(standardDestinationDefinition.getName())
        .icon(ICON_URL)
        .isEntitled(IS_ENTITLED)
        .isVersionOverrideApplied(IS_VERSION_OVERRIDE_APPLIED)
        .supportState(SUPPORT_STATE)
        .resourceAllocation(RESOURCE_ALLOCATION)

    every {
      destinationService.getDestinationConnection(destinationConnection.getDestinationId())
    } returns destinationConnection
    every {
      destinationService.listDestinationConnection()
    } returns Lists.newArrayList(destinationConnection)
    every {
      destinationService.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId())
    } returns standardDestinationDefinition
    every {
      destinationService.getDestinationDefinitionFromDestination(destinationConnection.getDestinationId())
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId(),
      )
    } returns destinationDefinitionVersion
    every {
      secretsProcessor.prepareSecretsForOutput(
        destinationConnection.getConfiguration(),
        destinationDefinitionSpecificationRead.getConnectionSpecification(),
      )
    } returns destinationConnection.getConfiguration()
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

    val validDestinationSearch = DestinationSearch().name(destinationConnection.getName())
    var actualDestinationRead = destinationHandler.searchDestinations(validDestinationSearch)
    Assertions.assertEquals(1, actualDestinationRead.getDestinations().size)
    Assertions.assertEquals(expectedDestinationRead, actualDestinationRead.getDestinations().get(0))
    verify {
      secretsProcessor.prepareSecretsForOutput(
        destinationConnection.getConfiguration(),
        destinationDefinitionSpecificationRead.getConnectionSpecification(),
      )
    }

    val invalidDestinationSearch = DestinationSearch().name("invalid")
    actualDestinationRead = destinationHandler.searchDestinations(invalidDestinationSearch)
    Assertions.assertEquals(0, actualDestinationRead.getDestinations().size)
  }

  companion object {
    private const val API_KEY_FIELD = "apiKey"
    private const val API_KEY_VALUE = "987-xyz"

    // needs to match name of file in src/test/resources/icons
    private const val ICON_URL = "https://connectors.airbyte.com/files/metadata/airbyte/destination-test/latest/icon.svg"
    private const val IS_VERSION_OVERRIDE_APPLIED = true
    private const val IS_ENTITLED = true
    private val SUPPORT_STATE = SupportState.SUPPORTED
    private const val DEFAULT_MEMORY = "2 GB"
    private const val DEFAULT_CPU = "2"

    private val RESOURCE_ALLOCATION: ScopedResourceRequirements? = getResourceRequirementsForDestinationRequest(DEFAULT_CPU, DEFAULT_MEMORY)

    fun getResourceRequirementsForDestinationRequest(
      defaultCpuRequest: String?,
      defaultMemoryRequest: String?,
    ): ScopedResourceRequirements? =
      ScopedResourceRequirements()._default(ResourceRequirements().cpuRequest(defaultCpuRequest).memoryRequest(defaultMemoryRequest))
  }
}
