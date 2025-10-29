/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.databind.node.ObjectNode
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
import io.airbyte.data.helpers.WorkspaceHelper
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
        .connectionSpecification(connectorSpecification.connectionSpecification)
        .destinationDefinitionId(standardDestinationDefinition.destinationDefinitionId)
        .documentationUrl(connectorSpecification.documentationUrl.toString())

    destinationConnection =
      DestinationHelpers.generateDestination(
        standardDestinationDefinition.destinationDefinitionId,
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
        destinationConnection.workspaceId,
        destinationConnection.destinationId,
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
  fun testCreateDestination() {
    // ===== GIVEN =====
    // Create the DestinationCreate request with the necessary fields.
    val destinationCreate =
      DestinationCreate()
        .name(destinationConnection.name)
        .workspaceId(destinationConnection.workspaceId)
        .destinationDefinitionId(standardDestinationDefinition.destinationDefinitionId)
        .connectionConfiguration(destinationConnection.configuration)
        .resourceAllocation(RESOURCE_ALLOCATION)

    // Set up basic mocks.
    every { uuidGenerator.get() } returns destinationConnection.destinationId
    every {
      destinationService.getStandardDestinationDefinition(standardDestinationDefinition.destinationDefinitionId)
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.workspaceId,
      )
    } returns destinationDefinitionVersion
    every {
      oAuthConfigSupplier.maskDestinationOAuthParameters(
        destinationDefinitionSpecificationRead.destinationDefinitionId,
        destinationConnection.workspaceId,
        destinationCreate.connectionConfiguration,
        destinationDefinitionVersion.spec,
      )
    } returns destinationCreate.connectionConfiguration
    every {
      actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(
        standardDestinationDefinition,
        destinationConnection.workspaceId,
        destinationConnection.destinationId,
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
    val configWithRefs = buildConfigWithSecretRefsJava(destinationConnection.configuration)
    every {
      secretReferenceService.getConfigWithSecretReferences(
        any(),
        destinationCreate.connectionConfiguration,
        any(),
      )
    } returns configWithRefs

    // Simulate secret persistence and reference ID insertion.
    val configWithProcessedSecrets =
      SecretReferenceHelpers.processConfigSecrets(
        destinationCreate.connectionConfiguration,
        destinationDefinitionSpecificationRead.connectionSpecification,
        SecretStorageId(secretStorageId),
      )
    every {
      secretsRepositoryWriter.createFromConfig(
        destinationConnection.workspaceId,
        configWithProcessedSecrets,
        secretPersistence,
      )
    } returns destinationCreate.connectionConfiguration

    val configWithSecretRefIds = clone(destinationCreate.connectionConfiguration)
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
      destinationService.getDestinationConnection(destinationConnection.destinationId)
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
        destinationDefinitionSpecificationRead.connectionSpecification,
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
      secretsProcessor.prepareSecretsForOutput(configWithSecretRefIds, destinationDefinitionSpecificationRead.connectionSpecification)
    }
    verify {
      oAuthConfigSupplier.maskDestinationOAuthParameters(
        destinationDefinitionSpecificationRead.destinationDefinitionId,
        destinationConnection.workspaceId,
        destinationCreate.connectionConfiguration,
        destinationDefinitionVersion.spec,
      )
    }
    verify { destinationService.writeDestinationConnectionNoSecrets(any()) }
    verify {
      actorDefinitionVersionHelper.getDestinationVersion(standardDestinationDefinition, destinationConnection.workspaceId)
    }
    verify {
      validator.ensure(destinationDefinitionSpecificationRead.connectionSpecification, destinationCreate.connectionConfiguration)
    }
  }

  @Test
  fun testCreateDestinationNoEntitlementThrows() {
    every { uuidGenerator.get() } returns destinationConnection.destinationId
    every {
      destinationService.getDestinationConnection(destinationConnection.destinationId)
    } returns destinationConnection
    every {
      destinationService.getStandardDestinationDefinition(standardDestinationDefinition.destinationDefinitionId)
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.workspaceId,
      )
    } returns destinationDefinitionVersion

    val destinationCreate =
      DestinationCreate()
        .name(destinationConnection.name)
        .workspaceId(destinationConnection.workspaceId)
        .destinationDefinitionId(standardDestinationDefinition.destinationDefinitionId)
        .connectionConfiguration(DestinationHelpers.testDestinationJson)
        .resourceAllocation(RESOURCE_ALLOCATION)

    // Not entitled
    every {
      licenseEntitlementChecker.ensureEntitled(
        any(),
        Entitlement.DESTINATION_CONNECTOR,
        standardDestinationDefinition.destinationDefinitionId,
      )
    } throws LicenseEntitlementProblem()

    Assertions.assertThrows(
      LicenseEntitlementProblem::class.java,
      { destinationHandler.createDestination(destinationCreate) },
    )

    verify {
      validator.ensure(destinationDefinitionSpecificationRead.connectionSpecification, destinationConnection.configuration)
    }
    verify {
      actorDefinitionVersionHelper.getDestinationVersion(standardDestinationDefinition, destinationConnection.workspaceId)
    }
  }

  @Test
  fun testCreateDestinationChecksConfigEntitlements() {
    every { uuidGenerator.get() } returns destinationConnection.destinationId
    every {
      destinationService.getDestinationConnection(destinationConnection.destinationId)
    } returns destinationConnection
    every {
      destinationService.getStandardDestinationDefinition(standardDestinationDefinition.destinationDefinitionId)
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.workspaceId,
      )
    } returns destinationDefinitionVersion

    val destinationCreate =
      DestinationCreate()
        .name(destinationConnection.name)
        .workspaceId(destinationConnection.workspaceId)
        .destinationDefinitionId(standardDestinationDefinition.destinationDefinitionId)
        .connectionConfiguration(DestinationHelpers.testDestinationJson)
        .resourceAllocation(RESOURCE_ALLOCATION)

    // Not entitled
    every {
      connectorConfigEntitlementService.ensureEntitledConfig(
        any(),
        destinationDefinitionVersion,
        destinationConnection.configuration,
      )
    } throws LicenseEntitlementProblem()

    Assertions.assertThrows(
      LicenseEntitlementProblem::class.java,
      { destinationHandler.createDestination(destinationCreate) },
    )

    verify {
      validator.ensure(destinationDefinitionSpecificationRead.connectionSpecification, destinationConnection.configuration)
    }
    verify {
      actorDefinitionVersionHelper.getDestinationVersion(standardDestinationDefinition, destinationConnection.workspaceId)
    }
  }

  @Test
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
        .name(destinationConnection.name)
        .workspaceId(destinationConnection.workspaceId)
        .destinationDefinitionId(standardDestinationDefinition.destinationDefinitionId)
        .connectionConfiguration(DestinationHelpers.testDestinationJson)
        .resourceAllocation(RESOURCE_ALLOCATION)

    Assertions.assertThrows(
      BadRequestException::class.java,
      { cloudDestinationHandler.createDestination(destinationCreate) },
      "Expected createDestination to throw BadRequestException",
    )
  }

  @Test
  fun testUpdateDestination() {
    // ===== GIVEN =====
    // Update the destination name and configuration.
    val updatedDestName = "my updated dest name"
    val newConfiguration = clone(destinationConnection.configuration)
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
        .destinationId(destinationConnection.destinationId)
        .connectionConfiguration(newConfiguration)
        .resourceAllocation(newResourceAllocation)

    // Set up basic mocks for the update.
    every {
      oAuthConfigSupplier.maskDestinationOAuthParameters(
        destinationDefinitionSpecificationRead.destinationDefinitionId,
        destinationConnection.workspaceId,
        newConfiguration,
        destinationDefinitionVersion.spec,
      )
    } returns newConfiguration
    every {
      destinationService.getStandardDestinationDefinition(standardDestinationDefinition.destinationDefinitionId)
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.workspaceId,
        destinationConnection.destinationId,
      )
    } returns destinationDefinitionVersion
    every {
      destinationService.getDestinationDefinitionFromDestination(destinationConnection.destinationId)
    } returns standardDestinationDefinition
    every {
      configurationUpdate.destination(
        destinationConnection.destinationId,
        updatedDestName,
        newConfiguration,
      )
    } returns updatedDestinationConnection
    every {
      actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(
        standardDestinationDefinition,
        destinationConnection.workspaceId,
        destinationConnection.destinationId,
      )
    } returns destinationDefinitionVersionWithOverrideStatus
    every {
      destinationService.getDestinationConnectionIfExists(destinationConnection.destinationId)
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
    val previousConfigWithRefs = buildConfigWithSecretRefsJava(destinationConnection.configuration)
    every {
      secretReferenceService.getConfigWithSecretReferences(
        ActorId(destinationConnection.destinationId),
        destinationConnection.configuration,
        WorkspaceId(destinationConnection.workspaceId),
      )
    } returns previousConfigWithRefs

    // Simulate the secret update and reference ID creation/insertion.
    val newConfigWithProcessedSecrets =
      SecretReferenceHelpers.processConfigSecrets(
        newConfiguration,
        destinationDefinitionSpecificationRead.connectionSpecification,
        SecretStorageId(secretStorageId),
      )
    every {
      secretsRepositoryWriter.updateFromConfig(
        destinationConnection.workspaceId,
        previousConfigWithRefs,
        newConfigWithProcessedSecrets,
        destinationDefinitionVersion.spec.connectionSpecification,
        secretPersistence,
      )
    } returns newConfigWithProcessedSecrets.originalConfig

    val newConfigWithSecretRefIds = clone(newConfiguration)
    newConfigWithSecretRefIds.put("updated_with", "secret_reference_ids")
    every {
      secretReferenceService.createAndInsertSecretReferencesWithStorageId(
        newConfigWithProcessedSecrets,
        ActorId(destinationConnection.destinationId),
        SecretStorageId(secretStorageId),
        any(),
      )
    } returns SecretReferenceHelpers.ConfigWithSecretReferenceIdsInjected(newConfigWithSecretRefIds)

    // Mock the updated config that is persisted and retrieved for building the destination read.
    val updatedDestinationWithSecretRefIds =
      clone(updatedDestinationConnection).withConfiguration(newConfigWithSecretRefIds)
    every {
      destinationService.getDestinationConnection(destinationConnection.destinationId)
    } returns updatedDestinationWithSecretRefIds

    val configWithRefsAfterPersist = buildConfigWithSecretRefsJava(newConfigWithSecretRefIds)
    every {
      secretReferenceService.getConfigWithSecretReferences(
        ActorId(destinationConnection.destinationId),
        newConfigWithSecretRefIds,
        WorkspaceId(destinationConnection.workspaceId),
      )
    } returns configWithRefsAfterPersist

    // Prepare secret output.
    every {
      secretsProcessor.prepareSecretsForOutput(
        newConfigWithSecretRefIds,
        destinationDefinitionSpecificationRead.connectionSpecification,
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
      secretsProcessor.prepareSecretsForOutput(newConfigWithSecretRefIds, destinationDefinitionSpecificationRead.connectionSpecification)
    }
    verify {
      oAuthConfigSupplier.maskDestinationOAuthParameters(
        destinationDefinitionSpecificationRead.destinationDefinitionId,
        destinationConnection.workspaceId,
        newConfiguration,
        destinationDefinitionVersion.spec,
      )
    }
    verify { destinationService.writeDestinationConnectionNoSecrets(updatedDestinationWithSecretRefIds) }
    verify {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.workspaceId,
        destinationConnection.destinationId,
      )
    }
    verify {
      validator.ensure(destinationDefinitionSpecificationRead.connectionSpecification, newConfiguration)
    }
  }

  @Test
  fun testUpdateDestinationChecksConfigEntitlements() {
    val updatedDestName = "my updated dest name for config entitlements"
    val newConfiguration = destinationConnection.configuration
    (newConfiguration as ObjectNode).put(API_KEY_FIELD, API_KEY_VALUE)
    val newResourceAllocation: ScopedResourceRequirements? = getResourceRequirementsForDestinationRequest("1", "1 GB")
    val destinationUpdate =
      DestinationUpdate()
        .name(updatedDestName)
        .destinationId(destinationConnection.destinationId)
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
          destinationConnection.configuration,
          newConfiguration,
          destinationDefinitionSpecificationRead.connectionSpecification,
        )
    } returns newConfiguration
    every {
      destinationService.getStandardDestinationDefinition(standardDestinationDefinition.destinationDefinitionId)
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.workspaceId,
        destinationConnection.destinationId,
      )
    } returns destinationDefinitionVersion
    every {
      destinationService.getDestinationDefinitionFromDestination(destinationConnection.destinationId)
    } returns standardDestinationDefinition
    every {
      destinationService.getDestinationConnection(destinationConnection.destinationId)
    } returns expectedDestinationConnection
    every {
      configurationUpdate.destination(
        destinationConnection.destinationId,
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
        destinationConnection.workspaceId,
        destinationConnection.destinationId,
      )
    }
    verify {
      validator.ensure(destinationDefinitionSpecificationRead.connectionSpecification, newConfiguration)
    }
  }

  @Test
  fun testUpdateDestinationNoEntitlementThrows() {
    val updatedDestName = "my updated dest name"
    val newConfiguration = destinationConnection.configuration
    (newConfiguration as ObjectNode).put(API_KEY_FIELD, API_KEY_VALUE)
    val newResourceAllocation: ScopedResourceRequirements? = getResourceRequirementsForDestinationRequest("3", "3 GB")
    val destinationUpdate =
      DestinationUpdate()
        .name(updatedDestName)
        .destinationId(destinationConnection.destinationId)
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
          destinationConnection.configuration,
          newConfiguration,
          destinationDefinitionSpecificationRead.connectionSpecification,
        )
    } returns newConfiguration
    every {
      destinationService.getStandardDestinationDefinition(standardDestinationDefinition.destinationDefinitionId)
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.workspaceId,
        destinationConnection.destinationId,
      )
    } returns destinationDefinitionVersion
    every {
      destinationService.getDestinationDefinitionFromDestination(destinationConnection.destinationId)
    } returns standardDestinationDefinition
    every {
      destinationService.getDestinationConnection(destinationConnection.destinationId)
    } returns expectedDestinationConnection
    every {
      configurationUpdate.destination(
        destinationConnection.destinationId,
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
          standardDestinationDefinition.destinationDefinitionId,
        )
    } throws LicenseEntitlementProblem()

    Assertions.assertThrows(
      LicenseEntitlementProblem::class.java,
      { destinationHandler.updateDestination(destinationUpdate) },
    )

    verify {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.workspaceId,
        destinationConnection.destinationId,
      )
    }
    verify {
      validator.ensure(destinationDefinitionSpecificationRead.connectionSpecification, newConfiguration)
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
    val newConfiguration = destinationConnection.configuration
    (newConfiguration as ObjectNode).put(API_KEY_FIELD, API_KEY_VALUE)
    val newResourceAllocation: ScopedResourceRequirements? = getResourceRequirementsForDestinationRequest("3", "3 GB")
    val destinationUpdate =
      DestinationUpdate()
        .name(updatedDestName)
        .destinationId(destinationConnection.destinationId)
        .connectionConfiguration(newConfiguration)
        .resourceAllocation(newResourceAllocation)

    Assertions.assertThrows(
      BadRequestException::class.java,
      { cloudDestinationHandler.updateDestination(destinationUpdate) },
      "Expected updateDestination to throw BadRequestException",
    )
  }

  @Test
  fun testUpgradeDestinationVersion() {
    val requestBody = DestinationIdRequestBody().destinationId(destinationConnection.destinationId)

    every {
      destinationService.getDestinationConnection(destinationConnection.destinationId)
    } returns destinationConnection
    every {
      destinationService.getStandardDestinationDefinition(standardDestinationDefinition.destinationDefinitionId)
    } returns standardDestinationDefinition

    destinationHandler.upgradeDestinationVersion(requestBody)

    // validate that we call the actorDefinitionVersionUpdater to upgrade the version to global default
    verify {
      actorDefinitionVersionUpdater.upgradeActorVersion(destinationConnection, standardDestinationDefinition)
    }
  }

  @Test
  fun testGetDestination() {
    val expectedDestinationRead =
      DestinationRead()
        .name(destinationConnection.name)
        .destinationDefinitionId(standardDestinationDefinition.destinationDefinitionId)
        .workspaceId(destinationConnection.workspaceId)
        .destinationId(destinationConnection.destinationId)
        .connectionConfiguration(destinationConnection.configuration)
        .destinationName(standardDestinationDefinition.name)
        .icon(ICON_URL)
        .isEntitled(IS_ENTITLED)
        .isVersionOverrideApplied(IS_VERSION_OVERRIDE_APPLIED)
        .supportState(SUPPORT_STATE)
        .resourceAllocation(RESOURCE_ALLOCATION)
    val destinationIdRequestBody =
      DestinationIdRequestBody().destinationId(expectedDestinationRead.destinationId)

    every {
      secretsProcessor.prepareSecretsForOutput(
        destinationConnection.configuration,
        destinationDefinitionSpecificationRead.connectionSpecification,
      )
    } returns destinationConnection.configuration
    every {
      destinationService.getDestinationConnection(destinationConnection.destinationId)
    } returns destinationConnection
    every {
      destinationService.getStandardDestinationDefinition(standardDestinationDefinition.destinationDefinitionId)
    } returns standardDestinationDefinition
    every {
      destinationService.getDestinationDefinitionFromDestination(destinationConnection.destinationId)
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.workspaceId,
        destinationConnection.destinationId,
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
    Assertions.assertTrue(expectedDestinationRead.icon.startsWith("https://"))

    verify {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.workspaceId,
        destinationConnection.destinationId,
      )
    }
    verify {
      secretsProcessor.prepareSecretsForOutput(
        destinationConnection.configuration,
        destinationDefinitionSpecificationRead.connectionSpecification,
      )
    }
  }

  @Test
  fun testListDestinationForWorkspace() {
    val expectedDestinationRead =
      DestinationRead()
        .name(destinationConnectionWithCount.destination.name)
        .destinationDefinitionId(standardDestinationDefinition.destinationDefinitionId)
        .workspaceId(destinationConnectionWithCount.destination.workspaceId)
        .destinationId(destinationConnectionWithCount.destination.destinationId)
        .connectionConfiguration(destinationConnectionWithCount.destination.configuration)
        .destinationName(standardDestinationDefinition.name)
        .icon(ICON_URL)
        .isEntitled(IS_ENTITLED)
        .isVersionOverrideApplied(IS_VERSION_OVERRIDE_APPLIED)
        .supportState(SUPPORT_STATE)
        .status(ActorStatus.ACTIVE)
        .resourceAllocation(RESOURCE_ALLOCATION)
        .numConnections(0)
    val workspaceIdRequestBody =
      WorkspaceIdRequestBody().workspaceId(destinationConnectionWithCount.destination.workspaceId)

    every {
      destinationService.getDestinationConnection(destinationConnectionWithCount.destination.destinationId)
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
        standardDestinationDefinition.destinationDefinitionId,
      )
    } returns standardDestinationDefinition
    every {
      destinationService.getDestinationDefinitionFromDestination(
        destinationConnectionWithCount.destination.destinationId,
      )
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnectionWithCount.destination.workspaceId,
        destinationConnectionWithCount.destination.destinationId,
      )
    } returns destinationDefinitionVersion
    every {
      secretsProcessor.prepareSecretsForOutput(
        destinationConnectionWithCount.destination.configuration,
        destinationDefinitionSpecificationRead.connectionSpecification,
      )
    } returns destinationConnectionWithCount.destination.configuration
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
            .workspaceId(workspaceIdRequestBody.workspaceId)
            .pageSize(10),
        )

    Assertions.assertEquals(expectedDestinationRead, actualDestinationRead.destinations.get(0))
    Assertions.assertEquals(1, actualDestinationRead.numConnections)
    Assertions.assertEquals(10, actualDestinationRead.pageSize)

    verify {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnectionWithCount.destination.workspaceId,
        destinationConnectionWithCount.destination.destinationId,
      )
    }
    verify {
      secretsProcessor.prepareSecretsForOutput(
        destinationConnectionWithCount.destination.configuration,
        destinationDefinitionSpecificationRead.connectionSpecification,
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
  fun testListDestinationsForWorkspaceWithPagination() {
    // Create multiple destinations for pagination testing
    val destination1 = createDestinationConnectionWithCount("dest1", 2)
    val destination2 = createDestinationConnectionWithCount("dest2", 1)
    val destinations: List<DestinationConnectionWithCount> =
      listOf(destination1, destination2)

    val workspaceId = destination1.destination.workspaceId
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
    Assertions.assertEquals(2, result.destinations.size)
    Assertions.assertEquals(2, result.numConnections)
    Assertions.assertEquals(10, result.pageSize)

    // Verify the first destination
    val firstDestination = result.destinations.get(0)
    Assertions.assertEquals("dest1", firstDestination.name)
    Assertions.assertEquals(2, firstDestination.numConnections)
    Assertions.assertEquals(ActorStatus.ACTIVE, firstDestination.status) // Has connections, so should be active

    // Verify the second destination
    val secondDestination = result.destinations.get(1)
    Assertions.assertEquals("dest2", secondDestination.name)
    Assertions.assertEquals(1, secondDestination.numConnections)
    Assertions.assertEquals(ActorStatus.ACTIVE, secondDestination.status) // Has connections, so should be active

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
  fun testListDestinationsForWorkspaceWithFilters() {
    val workspaceId = destinationConnectionWithCount.destination.workspaceId
    val requestBody =
      ActorListCursorPaginatedRequestBody()
        .workspaceId(workspaceId)
        .pageSize(5)
        .filters(
          ActorListFilters()
            .searchTerm("test")
            .states(listOf(ActorStatus.ACTIVE)),
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
    } returns listOf(destinationConnectionWithCount)

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
    } returns destinationConnectionWithCount.destination.configuration
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
    Assertions.assertEquals(1, result.destinations.size)
    Assertions.assertEquals(1, result.numConnections)
    Assertions.assertEquals(5, result.pageSize)

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
        .withDestinationDefinitionId(standardDestinationDefinition.destinationDefinitionId)
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
  fun testDeleteDestinationAndDeleteSecrets() {
    val newConfiguration = destinationConnection.configuration
    (newConfiguration as ObjectNode).put(API_KEY_FIELD, API_KEY_VALUE)

    val expectedSourceConnection = clone(destinationConnection).withTombstone(true)

    val destinationIdRequestBody = DestinationIdRequestBody().destinationId(destinationConnection.destinationId)
    val standardSync = ConnectionHelpers.generateSyncWithDestinationId(destinationConnection.destinationId)
    standardSync.breakingChange = false
    val connectionRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync)
    val connectionReadList = ConnectionReadList().connections(mutableListOf(connectionRead))
    val workspaceIdRequestBody = WorkspaceIdRequestBody().workspaceId(destinationConnection.workspaceId)

    every {
      destinationService.getDestinationConnection(destinationConnection.destinationId)
    } returns destinationConnection andThen expectedSourceConnection
    every {
      oAuthConfigSupplier.maskSourceOAuthParameters(
        destinationDefinitionSpecificationRead.destinationDefinitionId,
        destinationConnection.workspaceId,
        newConfiguration,
        destinationDefinitionVersion.spec,
      )
    } returns newConfiguration
    every {
      destinationService.getStandardDestinationDefinition(destinationDefinitionSpecificationRead.destinationDefinitionId)
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.workspaceId,
        destinationConnection.destinationId,
      )
    } returns destinationDefinitionVersion
    every {
      destinationService.getDestinationDefinitionFromDestination(destinationConnection.destinationId)
    } returns standardDestinationDefinition
    every { connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody) } returns connectionReadList
    every {
      secretsProcessor.prepareSecretsForOutput(
        destinationConnection.configuration,
        destinationDefinitionSpecificationRead.connectionSpecification,
      )
    } returns destinationConnection.configuration
    every {
      actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(
        standardDestinationDefinition,
        destinationConnection.workspaceId,
        destinationConnection.destinationId,
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
    verify { connectionsHandler.deleteConnection(connectionRead.connectionId) }
    verify { secretReferenceService.deleteActorSecretReferences(ActorId(destinationConnection.destinationId)) }
  }

  @Test
  fun testSearchDestinations() {
    val expectedDestinationRead =
      DestinationRead()
        .name(destinationConnection.name)
        .destinationDefinitionId(standardDestinationDefinition.destinationDefinitionId)
        .workspaceId(destinationConnection.workspaceId)
        .destinationId(destinationConnection.destinationId)
        .connectionConfiguration(destinationConnection.configuration)
        .destinationName(standardDestinationDefinition.name)
        .icon(ICON_URL)
        .isEntitled(IS_ENTITLED)
        .isVersionOverrideApplied(IS_VERSION_OVERRIDE_APPLIED)
        .supportState(SUPPORT_STATE)
        .resourceAllocation(RESOURCE_ALLOCATION)

    every {
      destinationService.getDestinationConnection(destinationConnection.destinationId)
    } returns destinationConnection
    every {
      destinationService.listDestinationConnection()
    } returns listOf(destinationConnection)
    every {
      destinationService.getStandardDestinationDefinition(standardDestinationDefinition.destinationDefinitionId)
    } returns standardDestinationDefinition
    every {
      destinationService.getDestinationDefinitionFromDestination(destinationConnection.destinationId)
    } returns standardDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        destinationConnection.workspaceId,
        destinationConnection.destinationId,
      )
    } returns destinationDefinitionVersion
    every {
      secretsProcessor.prepareSecretsForOutput(
        destinationConnection.configuration,
        destinationDefinitionSpecificationRead.connectionSpecification,
      )
    } returns destinationConnection.configuration
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

    val validDestinationSearch = DestinationSearch().name(destinationConnection.name)
    var actualDestinationRead = destinationHandler.searchDestinations(validDestinationSearch)
    Assertions.assertEquals(1, actualDestinationRead.destinations.size)
    Assertions.assertEquals(expectedDestinationRead, actualDestinationRead.destinations.get(0))
    verify {
      secretsProcessor.prepareSecretsForOutput(
        destinationConnection.configuration,
        destinationDefinitionSpecificationRead.connectionSpecification,
      )
    }

    val invalidDestinationSearch = DestinationSearch().name("invalid")
    actualDestinationRead = destinationHandler.searchDestinations(invalidDestinationSearch)
    Assertions.assertEquals(0, actualDestinationRead.destinations.size)
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
