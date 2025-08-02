/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.BreakingChangeScope
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.SupportLevel
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl
import io.airbyte.data.services.shared.ActorServicePaginationHelper
import io.airbyte.featureflag.HeartbeatMaxSecondsBetweenMessages
import io.airbyte.featureflag.SourceDefinition
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.test.utils.BaseConfigDatabaseTest
import io.airbyte.validation.json.JsonValidationException
import org.jooq.exception.DataAccessException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito
import org.mockito.Mockito.mock
import org.mockito.Mockito.spy
import java.io.IOException
import java.sql.SQLException
import java.util.List
import java.util.Map
import java.util.UUID
import org.mockito.Mockito.`when` as whenever

/**
 * Tests for configRepository methods that write connector metadata together. Includes writing
 * global metadata (source/destination definitions and breaking changes) and versioned metadata
 * (actor definition versions).
 */
internal class ConnectorMetadataPersistenceTest : BaseConfigDatabaseTest() {
  private var actorDefinitionVersionUpdater: ActorDefinitionVersionUpdater? = null
  private var actorDefinitionService: ActorDefinitionService? = null

  private var connectionService: ConnectionService? = null
  private var sourceService: SourceService? = null
  private var destinationService: DestinationService? = null
  private var workspaceService: WorkspaceService? = null

  @BeforeEach
  @Throws(SQLException::class, JsonValidationException::class, IOException::class)
  fun setup() {
    truncateAllTables()

    val featureFlagClient = mock(TestClient::class.java)
    whenever(
      featureFlagClient.stringVariation(org.mockito.kotlin.eq(HeartbeatMaxSecondsBetweenMessages), org.mockito.kotlin.any<SourceDefinition>()),
    ).thenReturn("3600")

    val secretsRepositoryReader = mock(SecretsRepositoryReader::class.java)
    val secretsRepositoryWriter = mock(SecretsRepositoryWriter::class.java)
    val secretPersistenceConfigService = mock(SecretPersistenceConfigService::class.java)
    val connectionTimelineEventService = mock(ConnectionTimelineEventService::class.java)
    val metricClient = mock(MetricClient::class.java)
    val scopedConfigurationService = mock(ScopedConfigurationService::class.java)
    val dataplaneGroupService = mock(DataplaneGroupService::class.java)
    val actorServicePaginationHelper = mock(ActorServicePaginationHelper::class.java)

    whenever(dataplaneGroupService.getDataplaneGroupByOrganizationIdAndName(org.mockito.kotlin.any(), org.mockito.kotlin.any()))
      .thenReturn(DataplaneGroup().withId(DATAPLANE_GROUP_ID))

    connectionService = ConnectionServiceJooqImpl(database!!)
    actorDefinitionService = spy(ActorDefinitionServiceJooqImpl(database!!))
    actorDefinitionVersionUpdater =
      spy(
        ActorDefinitionVersionUpdater(
          featureFlagClient,
          connectionService!!,
          actorDefinitionService!!,
          scopedConfigurationService,
          connectionTimelineEventService,
        ),
      )

    sourceService =
      SourceServiceJooqImpl(
        database!!,
        featureFlagClient,
        secretPersistenceConfigService,
        connectionService!!,
        actorDefinitionVersionUpdater!!,
        metricClient,
        actorServicePaginationHelper,
      )

    destinationService =
      DestinationServiceJooqImpl(
        database!!,
        featureFlagClient,
        connectionService!!,
        actorDefinitionVersionUpdater!!,
        metricClient,
        actorServicePaginationHelper,
      )

    workspaceService =
      WorkspaceServiceJooqImpl(
        database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        metricClient,
      )

    val organizationService = OrganizationServiceJooqImpl(database!!)
    val defaultOrg = MockData.defaultOrganization()
    organizationService.writeOrganization(defaultOrg)

    workspaceService!!.writeStandardWorkspaceNoSecrets(
      StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withName("default")
        .withSlug("workspace-slug")
        .withInitialSetupComplete(false)
        .withTombstone(false)
        .withDataplaneGroupId(DATAPLANE_GROUP_ID)
        .withOrganizationId(defaultOrg.organizationId),
    )
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testWriteConnectorMetadataForSource() {
    // Initial insert
    val sourceDefinition: StandardSourceDefinition = createBaseSourceDef()
    val actorDefinitionVersion1: ActorDefinitionVersion = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId())

    sourceService!!.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion1, emptyList())

    var sourceDefinitionFromDB = sourceService!!.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId())
    val actorDefinitionVersionFromDB =
      actorDefinitionService!!.getActorDefinitionVersion(
        actorDefinitionVersion1.getActorDefinitionId(),
        actorDefinitionVersion1.getDockerImageTag(),
      )

    Assertions.assertTrue(actorDefinitionVersionFromDB.isPresent())
    val firstVersionId = actorDefinitionVersionFromDB.get().getVersionId()

    Assertions.assertEquals(actorDefinitionVersion1.withVersionId(firstVersionId), actorDefinitionVersionFromDB.get())
    Assertions.assertEquals(firstVersionId, sourceDefinitionFromDB.getDefaultVersionId())
    Assertions.assertEquals(sourceDefinition.withDefaultVersionId(firstVersionId), sourceDefinitionFromDB)

    // Updating an existing source definition/version
    val sourceDefinition2 = sourceDefinition.withName("updated name")
    val actorDefinitionVersion2: ActorDefinitionVersion =
      createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId()).withDockerImageTag(UPGRADE_IMAGE_TAG)
    val scopedImpact =
      List.of<BreakingChangeScope?>(
        BreakingChangeScope()
          .withScopeType(BreakingChangeScope.ScopeType.STREAM)
          .withImpactedScopes(mutableListOf<Any?>("stream_a", "stream_b")),
      )
    val breakingChanges =
      mutableListOf(
        MockData
          .actorDefinitionBreakingChange(UPGRADE_IMAGE_TAG)!!
          .withActorDefinitionId(sourceDefinition2.getSourceDefinitionId())
          .withScopedImpact(scopedImpact),
      )
    sourceService!!.writeConnectorMetadata(sourceDefinition2, actorDefinitionVersion2, breakingChanges)

    sourceDefinitionFromDB = sourceService!!.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId())
    val actorDefinitionVersion2FromDB =
      actorDefinitionService!!.getActorDefinitionVersion(
        actorDefinitionVersion2.getActorDefinitionId(),
        actorDefinitionVersion2.getDockerImageTag(),
      )
    val breakingChangesForDefFromDb =
      actorDefinitionService!!.listBreakingChangesForActorDefinition(sourceDefinition2.getSourceDefinitionId())

    Assertions.assertTrue(actorDefinitionVersion2FromDB.isPresent())
    val newADVId = actorDefinitionVersion2FromDB.get().getVersionId()

    Assertions.assertNotEquals(firstVersionId, newADVId)
    Assertions.assertEquals(newADVId, sourceDefinitionFromDB.getDefaultVersionId())
    Assertions.assertEquals(sourceDefinition2.withDefaultVersionId(newADVId), sourceDefinitionFromDB)
    org.assertj.core.api.Assertions
      .assertThat<ActorDefinitionBreakingChange?>(breakingChangesForDefFromDb)
      .containsExactlyInAnyOrderElementsOf(breakingChanges)
    Mockito
      .verify<ActorDefinitionVersionUpdater?>(actorDefinitionVersionUpdater)
      .updateSourceDefaultVersion(sourceDefinition2, actorDefinitionVersion2, breakingChanges)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testWriteConnectorMetadataForDestination() {
    // Initial insert
    val destinationDefinition: StandardDestinationDefinition = createBaseDestDef()
    val actorDefinitionVersion1: ActorDefinitionVersion = createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId())

    destinationService!!.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion1, emptyList())

    var destinationDefinitionFromDB =
      destinationService!!.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId())
    val actorDefinitionVersionFromDB =
      actorDefinitionService!!.getActorDefinitionVersion(
        actorDefinitionVersion1.getActorDefinitionId(),
        actorDefinitionVersion1.getDockerImageTag(),
      )

    Assertions.assertTrue(actorDefinitionVersionFromDB.isPresent())
    val firstVersionId = actorDefinitionVersionFromDB.get().getVersionId()

    Assertions.assertEquals(actorDefinitionVersion1.withVersionId(firstVersionId), actorDefinitionVersionFromDB.get())
    Assertions.assertEquals(firstVersionId, destinationDefinitionFromDB.getDefaultVersionId())
    Assertions.assertEquals(destinationDefinition.withDefaultVersionId(firstVersionId), destinationDefinitionFromDB)

    // Updating an existing destination definition/version
    val destinationDefinition2 = destinationDefinition.withName("updated name")
    val actorDefinitionVersion2: ActorDefinitionVersion =
      createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId()).withDockerImageTag(UPGRADE_IMAGE_TAG)

    val scopedImpact =
      List.of<BreakingChangeScope?>(
        BreakingChangeScope()
          .withScopeType(BreakingChangeScope.ScopeType.STREAM)
          .withImpactedScopes(mutableListOf<Any?>("stream_a", "stream_b")),
      )
    val breakingChanges =
      mutableListOf(
        MockData
          .actorDefinitionBreakingChange(UPGRADE_IMAGE_TAG)!!
          .withActorDefinitionId(destinationDefinition2.getDestinationDefinitionId())
          .withScopedImpact(scopedImpact),
      )
    destinationService!!.writeConnectorMetadata(destinationDefinition2, actorDefinitionVersion2, breakingChanges)

    destinationDefinitionFromDB = destinationService!!.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId())
    val actorDefinitionVersion2FromDB =
      actorDefinitionService!!.getActorDefinitionVersion(
        actorDefinitionVersion2.getActorDefinitionId(),
        actorDefinitionVersion2.getDockerImageTag(),
      )
    val breakingChangesForDefFromDb =
      actorDefinitionService!!.listBreakingChangesForActorDefinition(destinationDefinition2.getDestinationDefinitionId())

    Assertions.assertTrue(actorDefinitionVersion2FromDB.isPresent())
    val newADVId = actorDefinitionVersion2FromDB.get().getVersionId()

    Assertions.assertNotEquals(firstVersionId, newADVId)
    Assertions.assertEquals(newADVId, destinationDefinitionFromDB.getDefaultVersionId())
    Assertions.assertEquals(destinationDefinition2.withDefaultVersionId(newADVId), destinationDefinitionFromDB)
    org.assertj.core.api.Assertions
      .assertThat<ActorDefinitionBreakingChange?>(breakingChangesForDefFromDb)
      .containsExactlyInAnyOrderElementsOf(breakingChanges)
    Mockito
      .verify<ActorDefinitionVersionUpdater?>(actorDefinitionVersionUpdater)
      .updateDestinationDefaultVersion(destinationDefinition2, actorDefinitionVersion2, breakingChanges)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testUpdateConnectorMetadata() {
    val sourceDefinition: StandardSourceDefinition = createBaseSourceDef()
    val actorDefinitionId = sourceDefinition.getSourceDefinitionId()
    val actorDefinitionVersion1: ActorDefinitionVersion = createBaseActorDefVersion(actorDefinitionId)
    sourceService!!.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion1, emptyList())

    val optADVForTag = actorDefinitionService!!.getActorDefinitionVersion(actorDefinitionId, DOCKER_IMAGE_TAG)
    Assertions.assertTrue(optADVForTag.isPresent())
    val advForTag = optADVForTag.get()
    val retrievedSourceDefinition =
      sourceService!!.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId())
    Assertions.assertEquals(retrievedSourceDefinition.getDefaultVersionId(), advForTag.getVersionId())

    val updatedSpec =
      ConnectorSpecification()
        .withConnectionSpecification(jsonNode<MutableMap<String?, String?>?>(Map.of<String?, String?>("key", "value2")))
        .withProtocolVersion(
          PROTOCOL_VERSION,
        )

    // Modify spec without changing docker image tag
    val modifiedADV: ActorDefinitionVersion = createBaseActorDefVersion(actorDefinitionId).withSpec(updatedSpec)
    sourceService!!.writeConnectorMetadata(sourceDefinition, modifiedADV, emptyList())

    Assertions.assertEquals(retrievedSourceDefinition, sourceService!!.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId()))
    val optADVForTagAfterCall2 =
      actorDefinitionService!!.getActorDefinitionVersion(actorDefinitionId, DOCKER_IMAGE_TAG)
    Assertions.assertTrue(optADVForTagAfterCall2.isPresent())

    // Modifying fields without updating image tag updates existing version
    Assertions.assertEquals(modifiedADV.withVersionId(advForTag.getVersionId()), optADVForTagAfterCall2.get())

    // Modifying docker image tag creates a new version
    val newADV: ActorDefinitionVersion =
      createBaseActorDefVersion(actorDefinitionId).withDockerImageTag(UPGRADE_IMAGE_TAG).withSpec(updatedSpec)
    sourceService!!.writeConnectorMetadata(sourceDefinition, newADV, emptyList())

    val optADVForTag2 = actorDefinitionService!!.getActorDefinitionVersion(actorDefinitionId, UPGRADE_IMAGE_TAG)
    Assertions.assertTrue(optADVForTag2.isPresent())
    val advForTag2 = optADVForTag2.get()

    // Versioned data is updated as well as the version id
    Assertions.assertEquals(advForTag2, newADV.withVersionId(advForTag2.getVersionId()))
    Assertions.assertNotEquals(advForTag2.getVersionId(), advForTag.getVersionId())
    Assertions.assertNotEquals(advForTag2.getSpec(), advForTag.getSpec())
    Mockito
      .verify<ActorDefinitionVersionUpdater?>(actorDefinitionVersionUpdater)
      .updateSourceDefaultVersion(sourceDefinition, newADV, emptyList())
  }

  @ParameterizedTest
  @ValueSource(strings = ["2.0.0", "dev", "test", "1.9.1-dev.33a53e6236", "97b69a76-1f06-4680-8905-8beda74311d0"])
  @Throws(
    IOException::class,
  )
  fun testCustomImageTagsDoNotBreakCustomConnectorUpgrade(dockerImageTag: String?) {
    // Initial insert
    val customSourceDefinition: StandardSourceDefinition = createBaseSourceDef().withCustom(true)
    val customDestinationDefinition: StandardDestinationDefinition = createBaseDestDef().withCustom(true)
    val sourceActorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(customSourceDefinition.getSourceDefinitionId())
    val destinationActorDefinitionVersion: ActorDefinitionVersion =
      createBaseActorDefVersion(customDestinationDefinition.getDestinationDefinitionId())
    sourceService!!.writeConnectorMetadata(customSourceDefinition, sourceActorDefinitionVersion, emptyList())
    destinationService!!.writeConnectorMetadata(
      customDestinationDefinition,
      destinationActorDefinitionVersion,
      emptyList(),
    )

    // Update
    Assertions.assertDoesNotThrow(
      Executable {
        sourceService!!.writeConnectorMetadata(
          customSourceDefinition,
          createBaseActorDefVersion(customSourceDefinition.getSourceDefinitionId()).withDockerImageTag(dockerImageTag),
          emptyList(),
        )
      },
    )
    Assertions.assertDoesNotThrow(
      Executable {
        destinationService!!.writeConnectorMetadata(
          customDestinationDefinition,
          createBaseActorDefVersion(customDestinationDefinition.getDestinationDefinitionId()).withDockerImageTag(dockerImageTag),
          emptyList(),
        )
      },
    )
  }

  @ParameterizedTest
  @ValueSource(strings = ["2.0.0", "dev", "test", "1.9.1-dev.33a53e6236", "97b69a76-1f06-4680-8905-8beda74311d0"])
  @Throws(
    IOException::class,
  )
  fun testImageTagExpectationsNorNonCustomConnectorUpgradesWithoutBreakingChanges(dockerImageTag: String?) {
    // Initial insert
    val sourceDefinition: StandardSourceDefinition = createBaseSourceDef()
    val destinationDefinition: StandardDestinationDefinition = createBaseDestDef()
    val sourceActorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId())
    val destinationActorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId())
    sourceService!!.writeConnectorMetadata(sourceDefinition, sourceActorDefinitionVersion, emptyList())
    destinationService!!.writeConnectorMetadata(
      destinationDefinition,
      destinationActorDefinitionVersion,
      emptyList(),
    )

    // Update
    Assertions.assertDoesNotThrow(
      Executable {
        sourceService!!.writeConnectorMetadata(
          sourceDefinition,
          createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId()).withDockerImageTag(dockerImageTag),
          emptyList(),
        )
      },
    )
    Assertions.assertDoesNotThrow(
      Executable {
        destinationService!!.writeConnectorMetadata(
          destinationDefinition,
          createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId()).withDockerImageTag(dockerImageTag),
          emptyList(),
        )
      },
    )
  }

  @ParameterizedTest
  @CsvSource("0.0.1, true", "dev, true", "test, false", "1.9.1-dev.33a53e6236, true", "97b69a76-1f06-4680-8905-8beda74311d0, false")
  @Throws(
    IOException::class,
  )
  fun testImageTagExpectationsNorNonCustomConnectorUpgradesWithBreakingChanges(
    dockerImageTag: String?,
    upgradeShouldSucceed: Boolean,
  ) {
    // Initial insert
    val sourceDefinition: StandardSourceDefinition = createBaseSourceDef()
    val destinationDefinition: StandardDestinationDefinition = createBaseDestDef()
    val sourceActorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId())
    val destinationActorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId())
    sourceService!!.writeConnectorMetadata(sourceDefinition, sourceActorDefinitionVersion, emptyList())
    destinationService!!.writeConnectorMetadata(
      destinationDefinition,
      destinationActorDefinitionVersion,
      emptyList(),
    )

    val sourceBreakingChanges =
      listOf(
        MockData.actorDefinitionBreakingChange(UPGRADE_IMAGE_TAG)!!.withActorDefinitionId(sourceDefinition.getSourceDefinitionId()),
      )
    val destinationBreakingChanges =
      listOf(
        MockData.actorDefinitionBreakingChange(UPGRADE_IMAGE_TAG)!!.withActorDefinitionId(destinationDefinition.getDestinationDefinitionId()),
      )

    // Update
    if (upgradeShouldSucceed) {
      Assertions.assertDoesNotThrow(
        Executable {
          sourceService!!.writeConnectorMetadata(
            sourceDefinition,
            createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId()).withDockerImageTag(dockerImageTag),
            sourceBreakingChanges,
          )
        },
      )
      Assertions.assertDoesNotThrow(
        Executable {
          destinationService!!.writeConnectorMetadata(
            destinationDefinition,
            createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId()).withDockerImageTag(dockerImageTag),
            destinationBreakingChanges,
          )
        },
      )
    } else {
      Assertions.assertThrows<IllegalArgumentException?>(
        IllegalArgumentException::class.java,
        Executable {
          sourceService!!.writeConnectorMetadata(
            sourceDefinition,
            createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId()).withDockerImageTag(dockerImageTag),
            sourceBreakingChanges,
          )
        },
      )
      Assertions.assertThrows<IllegalArgumentException?>(
        IllegalArgumentException::class.java,
        Executable {
          destinationService!!.writeConnectorMetadata(
            destinationDefinition,
            createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId()).withDockerImageTag(dockerImageTag),
            destinationBreakingChanges,
          )
        },
      )
    }
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, ConfigNotFoundException::class)
  fun testTransactionRollbackOnFailure() {
    val initialADVId = UUID.randomUUID()
    val sourceDefinition: StandardSourceDefinition = createBaseSourceDef()
    val actorDefinitionVersion1: ActorDefinitionVersion =
      createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId()).withVersionId(initialADVId)

    sourceService!!.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion1, emptyList())

    val sourceDefId = sourceDefinition.getSourceDefinitionId()
    val sourceConnection: SourceConnection = createBaseSourceActor(sourceDefId)
    sourceService!!.writeSourceConnectionNoSecrets(sourceConnection)

    val initialSourceDefinitionDefaultVersionId =
      sourceService!!.getStandardSourceDefinition(sourceDefId).getDefaultVersionId()
    Assertions.assertNotNull(initialSourceDefinitionDefaultVersionId)

    // Introduce a breaking change between 0.0.1 and UPGRADE_IMAGE_TAG to make the upgrade breaking, but
    // with a version that will fail to write (due to null docker repo).
    // We want to check that the state is rolled back correctly.
    val invalidVersion = "1.0.0"
    val breakingChangesForDef =
      List.of<ActorDefinitionBreakingChange?>(MockData.actorDefinitionBreakingChange(invalidVersion)!!.withActorDefinitionId(sourceDefId))

    val newVersionId = UUID.randomUUID()
    val newVersion =
      MockData
        .actorDefinitionVersion()!!
        .withActorDefinitionId(sourceDefId)
        .withVersionId(newVersionId)
        .withDockerRepository(null)
        .withDockerImageTag(invalidVersion)
        .withDocumentationUrl("https://www.something.new")

    val updatedSourceDefinition = clone<StandardSourceDefinition>(sourceDefinition).withName("updated name")

    Assertions.assertThrows<DataAccessException?>(
      DataAccessException::class.java,
      Executable { sourceService!!.writeConnectorMetadata(updatedSourceDefinition, newVersion, breakingChangesForDef) },
    )

    val sourceDefinitionDefaultVersionIdAfterFailedUpgrade =
      sourceService!!.getStandardSourceDefinition(sourceDefId).getDefaultVersionId()
    val sourceDefinitionAfterFailedUpgrade =
      sourceService!!.getStandardSourceDefinition(sourceDefId)
    val newActorDefinitionVersionAfterFailedUpgrade =
      actorDefinitionService!!.getActorDefinitionVersion(sourceDefId, invalidVersion)
    val defaultActorDefinitionVersionAfterFailedUpgrade =
      actorDefinitionService!!.getActorDefinitionVersion(sourceDefinitionDefaultVersionIdAfterFailedUpgrade)

    // New actor definition version was not persisted
    Assertions.assertFalse(newActorDefinitionVersionAfterFailedUpgrade.isPresent())
    // Valid breaking change was not persisted
    Assertions.assertEquals(0, actorDefinitionService!!.listBreakingChangesForActorDefinition(sourceDefId).size)

    // The default version does not get upgraded
    Assertions.assertEquals(initialSourceDefinitionDefaultVersionId, sourceDefinitionDefaultVersionIdAfterFailedUpgrade)

    // Source definition metadata is the same as before
    Assertions.assertEquals(sourceDefinition.withDefaultVersionId(initialADVId), sourceDefinitionAfterFailedUpgrade)
    // Actor definition metadata is the same as before
    Assertions.assertEquals(actorDefinitionVersion1, defaultActorDefinitionVersionAfterFailedUpgrade)
  }

  companion object {
    private val DEFAULT_ORGANIZATION_ID: UUID = UUID.randomUUID()
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val DATAPLANE_GROUP_ID: UUID = UUID.randomUUID()

    private const val DOCKER_IMAGE_TAG = "0.0.1"

    private const val UPGRADE_IMAGE_TAG = "0.0.2"
    private const val PROTOCOL_VERSION = "1.0.0"

    private fun createBaseSourceDef(): StandardSourceDefinition {
      val id = UUID.randomUUID()

      return StandardSourceDefinition()
        .withName("source-def-" + id)
        .withSourceDefinitionId(id)
        .withTombstone(false)
        .withMaxSecondsBetweenMessages(MockData.DEFAULT_MAX_SECONDS_BETWEEN_MESSAGES)
    }

    private fun createBaseActorDefVersion(actorDefId: UUID?): ActorDefinitionVersion =
      ActorDefinitionVersion()
        .withActorDefinitionId(actorDefId)
        .withDockerRepository("source-image-" + actorDefId)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withProtocolVersion(PROTOCOL_VERSION)
        .withSupportLevel(SupportLevel.CERTIFIED)
        .withInternalSupportLevel(200L)
        .withSpec(
          ConnectorSpecification()
            .withConnectionSpecification(jsonNode<MutableMap<String?, String?>?>(Map.of<String?, String?>("key", "value1")))
            .withProtocolVersion(
              PROTOCOL_VERSION,
            ),
        )

    private fun createBaseDestDef(): StandardDestinationDefinition {
      val id = UUID.randomUUID()

      return StandardDestinationDefinition()
        .withName("source-def-" + id)
        .withDestinationDefinitionId(id)
        .withTombstone(false)
    }

    private fun createBaseSourceActor(actorDefinitionId: UUID?): SourceConnection {
      val id = UUID.randomUUID()

      return SourceConnection()
        .withSourceId(id)
        .withSourceDefinitionId(actorDefinitionId)
        .withWorkspaceId(WORKSPACE_ID)
        .withName("source-" + id)
    }
  }
}
