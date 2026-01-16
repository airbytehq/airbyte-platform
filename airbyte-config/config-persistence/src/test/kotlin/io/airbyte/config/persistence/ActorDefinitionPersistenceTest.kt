/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.DestinationConnection
import io.airbyte.config.Organization
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
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
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.data.DataplaneGroupServiceTestJooqImpl
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl
import io.airbyte.data.services.shared.ActorServicePaginationHelper
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.HeartbeatMaxSecondsBetweenMessages
import io.airbyte.featureflag.SourceDefinition
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.test.utils.BaseConfigDatabaseTest
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito
import java.util.UUID

internal class ActorDefinitionPersistenceTest : BaseConfigDatabaseTest() {
  private lateinit var actorDefinitionService: ActorDefinitionService
  private lateinit var sourceService: SourceServiceJooqImpl
  private lateinit var destinationService: DestinationServiceJooqImpl
  private lateinit var workspaceService: WorkspaceService
  private lateinit var connectionService: ConnectionService
  private lateinit var dataplaneGroupService: DataplaneGroupService

  @BeforeEach
  fun setup() {
    truncateAllTables()

    val featureFlagClient: FeatureFlagClient = Mockito.mock(TestClient::class.java)
    Mockito
      .`when`(
        featureFlagClient.stringVariation(
          org.mockito.kotlin.eq(HeartbeatMaxSecondsBetweenMessages),
          org.mockito.kotlin.any<SourceDefinition>(),
        ),
      ).thenReturn(TEST_DEFAULT_MAX_SECONDS)

    val secretsRepositoryReader: SecretsRepositoryReader = Mockito.mock(SecretsRepositoryReader::class.java)
    val secretsRepositoryWriter: SecretsRepositoryWriter = Mockito.mock(SecretsRepositoryWriter::class.java)
    val secretPersistenceConfigService: SecretPersistenceConfigService = Mockito.mock(SecretPersistenceConfigService::class.java)
    val scopedConfigurationService: ScopedConfigurationService = Mockito.mock(ScopedConfigurationService::class.java)
    val connectionTimelineEventService: ConnectionTimelineEventService = Mockito.mock(ConnectionTimelineEventService::class.java)
    val metricClient: MetricClient = Mockito.mock(MetricClient::class.java)
    val actorPaginationServiceHelper: ActorServicePaginationHelper = Mockito.mock(ActorServicePaginationHelper::class.java)

    actorDefinitionService = ActorDefinitionServiceJooqImpl(database!!)
    val organizationService: OrganizationService = OrganizationServiceJooqImpl(database!!)
    organizationService.writeOrganization(
      Organization()
        .withOrganizationId(DEFAULT_ORGANIZATION_ID)
        .withName("Test Organization")
        .withEmail("test@test.com"),
    )

    dataplaneGroupService = Mockito.spy(DataplaneGroupServiceTestJooqImpl(database!!))
    dataplaneGroupService.writeDataplaneGroup(
      DataplaneGroup()
        .withId(dataplaneGroupId)
        .withOrganizationId(DEFAULT_ORGANIZATION_ID)
        .withName("test")
        .withEnabled(true)
        .withTombstone(false),
    )

    connectionService = Mockito.spy(ConnectionServiceJooqImpl(database!!))

    sourceService =
      Mockito.spy(
        SourceServiceJooqImpl(
          database!!,
          featureFlagClient,
          secretPersistenceConfigService,
          connectionService,
          ActorDefinitionVersionUpdater(
            featureFlagClient,
            connectionService,
            actorDefinitionService,
            scopedConfigurationService,
            connectionTimelineEventService,
          ),
          metricClient,
          actorPaginationServiceHelper,
        ),
      )
    destinationService =
      Mockito.spy(
        DestinationServiceJooqImpl(
          database!!,
          featureFlagClient,
          connectionService,
          ActorDefinitionVersionUpdater(
            featureFlagClient,
            connectionService,
            actorDefinitionService,
            scopedConfigurationService,
            connectionTimelineEventService,
          ),
          metricClient,
          actorPaginationServiceHelper,
        ),
      )

    organizationService.writeOrganization(MockData.defaultOrganization())

    workspaceService =
      Mockito.spy(
        WorkspaceServiceJooqImpl(
          database!!,
          featureFlagClient,
          secretsRepositoryReader,
          secretsRepositoryWriter,
          secretPersistenceConfigService,
          metricClient,
        ),
      )
  }

  @Test
  fun testSourceDefinitionWithNullTombstone() {
    assertReturnsSrcDef(createBaseSourceDef())
  }

  @Test
  fun testSourceDefinitionWithTrueTombstone() {
    assertReturnsSrcDef(createBaseSourceDef().withTombstone(true))
  }

  @Test
  fun testSourceDefinitionWithFalseTombstone() {
    assertReturnsSrcDef(createBaseSourceDef().withTombstone(false))
  }

  @Test
  fun testSourceDefinitionDefaultMaxSeconds() {
    assertReturnsSrcDefDefaultMaxSecondsBetweenMessages(createBaseSourceDefWithoutMaxSecondsBetweenMessages())
  }

  @Test
  fun testSourceDefinitionMaxSecondsGreaterThenDefaultShouldReturnConfigured() {
    assertReturnsSrcDef(
      createBaseSourceDefWithoutMaxSecondsBetweenMessages().withMaxSecondsBetweenMessages(TEST_DEFAULT_MAX_SECONDS.toLong() + 1),
    )
  }

  @Test
  fun testSourceDefinitionMaxSecondsLessThenDefaultShouldReturnDefault() {
    val def: StandardSourceDefinition = createBaseSourceDefWithoutMaxSecondsBetweenMessages().withMaxSecondsBetweenMessages(1L)
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(def.sourceDefinitionId)
    sourceService.writeConnectorMetadata(def, actorDefinitionVersion, mutableListOf())
    val exp =
      def.withDefaultVersionId(actorDefinitionVersion.versionId).withMaxSecondsBetweenMessages(TEST_DEFAULT_MAX_SECONDS.toLong())
    Assertions.assertEquals(exp, sourceService.getStandardSourceDefinition(def.sourceDefinitionId))
  }

  private fun assertReturnsSrcDef(srcDef: StandardSourceDefinition) {
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(srcDef.sourceDefinitionId)
    sourceService.writeConnectorMetadata(srcDef, actorDefinitionVersion, mutableListOf())
    Assertions.assertEquals(
      srcDef.withDefaultVersionId(actorDefinitionVersion.versionId),
      sourceService.getStandardSourceDefinition(srcDef.sourceDefinitionId),
    )
  }

  private fun assertReturnsSrcDefDefaultMaxSecondsBetweenMessages(srcDef: StandardSourceDefinition) {
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(srcDef.sourceDefinitionId)
    sourceService.writeConnectorMetadata(srcDef, actorDefinitionVersion, mutableListOf())
    Assertions.assertEquals(
      srcDef
        .withDefaultVersionId(actorDefinitionVersion.versionId)
        .withMaxSecondsBetweenMessages(MockData.DEFAULT_MAX_SECONDS_BETWEEN_MESSAGES),
      sourceService.getStandardSourceDefinition(srcDef.sourceDefinitionId),
    )
  }

  @Test
  fun testGetSourceDefinitionFromSource() {
    val workspace: StandardWorkspace = createBaseStandardWorkspace()
    val srcDef: StandardSourceDefinition = createBaseSourceDef().withTombstone(false)
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(srcDef.sourceDefinitionId)
    val source: SourceConnection = createSource(srcDef.sourceDefinitionId, workspace.workspaceId)
    workspaceService.writeStandardWorkspaceNoSecrets(workspace)
    sourceService.writeConnectorMetadata(srcDef, actorDefinitionVersion, mutableListOf())
    sourceService.writeSourceConnectionNoSecrets(source)

    Assertions.assertEquals(
      srcDef.withDefaultVersionId(actorDefinitionVersion.versionId),
      sourceService.getSourceDefinitionFromSource(source.sourceId),
    )
  }

  @Test
  fun testGetSourceDefinitionsFromConnection() {
    val workspace: StandardWorkspace = createBaseStandardWorkspace()
    val destDef: StandardDestinationDefinition = createBaseDestDef().withTombstone(false)
    val destActorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(destDef.destinationDefinitionId)
    val dest: DestinationConnection = createDest(destDef.destinationDefinitionId, workspace.workspaceId)
    val srcDef: StandardSourceDefinition = createBaseSourceDef().withTombstone(false)
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(srcDef.sourceDefinitionId)
    val source: SourceConnection = createSource(srcDef.sourceDefinitionId, workspace.workspaceId)
    workspaceService.writeStandardWorkspaceNoSecrets(workspace)
    sourceService.writeConnectorMetadata(srcDef, actorDefinitionVersion, mutableListOf())
    sourceService.writeSourceConnectionNoSecrets(source)
    destinationService.writeConnectorMetadata(destDef, destActorDefinitionVersion, mutableListOf())
    destinationService.writeDestinationConnectionNoSecrets(dest)

    val connectionId = UUID.randomUUID()
    val connection =
      StandardSync()
        .withName("Test Sync")
        .withDestinationId(dest.destinationId)
        .withConnectionId(connectionId)
        .withSourceId(source.sourceId)
        .withCatalog(ConfiguredAirbyteCatalog())
        .withBreakingChange(false)

    connectionService.writeStandardSync(connection)

    Assertions.assertEquals(
      srcDef.withDefaultVersionId(actorDefinitionVersion.versionId),
      sourceService.getSourceDefinitionFromConnection(connectionId),
    )
  }

  @ParameterizedTest
  @ValueSource(ints = [0, 1, 2, 10])
  fun testListStandardSourceDefsHandlesTombstoneSourceDefs(numSrcDefs: Int) {
    val allSourceDefinitions: MutableList<StandardSourceDefinition?> = ArrayList()
    val notTombstoneSourceDefinitions: MutableList<StandardSourceDefinition?> = ArrayList()
    for (i in 0..<numSrcDefs) {
      val isTombstone = i % 2 == 0 // every other is tombstone
      val sourceDefinition: StandardSourceDefinition = createBaseSourceDef().withTombstone(isTombstone)
      val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(sourceDefinition.sourceDefinitionId)
      allSourceDefinitions.add(sourceDefinition)
      if (!isTombstone) {
        notTombstoneSourceDefinitions.add(sourceDefinition)
      }
      sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, mutableListOf())
      sourceDefinition.defaultVersionId = actorDefinitionVersion.versionId
    }

    val returnedSrcDefsWithoutTombstone: MutableList<StandardSourceDefinition> = sourceService.listStandardSourceDefinitions(false)
    Assertions.assertEquals(notTombstoneSourceDefinitions, returnedSrcDefsWithoutTombstone)

    val returnedSrcDefsWithTombstone: MutableList<StandardSourceDefinition> = sourceService.listStandardSourceDefinitions(true)
    Assertions.assertEquals(allSourceDefinitions, returnedSrcDefsWithTombstone)
  }

  @Test
  fun testDestinationDefinitionWithNullTombstone() {
    assertReturnsDestDef(createBaseDestDef())
  }

  @Test
  fun testDestinationDefinitionWithTrueTombstone() {
    assertReturnsDestDef(createBaseDestDef().withTombstone(true))
  }

  @Test
  fun testDestinationDefinitionWithFalseTombstone() {
    assertReturnsDestDef(createBaseDestDef().withTombstone(false))
  }

  fun assertReturnsDestDef(destDef: StandardDestinationDefinition) {
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(destDef.destinationDefinitionId)
    destinationService.writeConnectorMetadata(destDef, actorDefinitionVersion, mutableListOf())
    Assertions.assertEquals(
      destDef.withDefaultVersionId(actorDefinitionVersion.versionId),
      destinationService.getStandardDestinationDefinition(destDef.destinationDefinitionId),
    )
  }

  @Test
  fun testGetDestinationDefinitionFromDestination() {
    val workspace: StandardWorkspace = createBaseStandardWorkspace()
    val destDef: StandardDestinationDefinition = createBaseDestDef().withTombstone(false)
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(destDef.destinationDefinitionId)
    val dest: DestinationConnection = createDest(destDef.destinationDefinitionId, workspace.workspaceId)
    workspaceService.writeStandardWorkspaceNoSecrets(workspace)
    destinationService.writeConnectorMetadata(destDef, actorDefinitionVersion, mutableListOf())
    destinationService.writeDestinationConnectionNoSecrets(dest)

    Assertions.assertEquals(
      destDef.withDefaultVersionId(actorDefinitionVersion.versionId),
      destinationService.getDestinationDefinitionFromDestination(dest.destinationId),
    )
  }

  @Test
  fun testGetDestinationDefinitionsFromConnection() {
    val workspace: StandardWorkspace = createBaseStandardWorkspace()
    val destDef: StandardDestinationDefinition = createBaseDestDef().withTombstone(false)
    val sourceDefinition: StandardSourceDefinition = createBaseSourceDef().withTombstone(false)
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(destDef.destinationDefinitionId)
    val sourceActorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(sourceDefinition.sourceDefinitionId)
    val dest: DestinationConnection = createDest(destDef.destinationDefinitionId, workspace.workspaceId)
    val source: SourceConnection = createSource(sourceDefinition.sourceDefinitionId, workspace.workspaceId)
    workspaceService.writeStandardWorkspaceNoSecrets(workspace)
    destinationService.writeConnectorMetadata(destDef, actorDefinitionVersion, mutableListOf())
    sourceService.writeConnectorMetadata(sourceDefinition, sourceActorDefinitionVersion, mutableListOf())
    destinationService.writeDestinationConnectionNoSecrets(dest)
    sourceService.writeSourceConnectionNoSecrets(source)

    val connectionId = UUID.randomUUID()
    val connection =
      StandardSync()
        .withName("Test Sync")
        .withDestinationId(dest.destinationId)
        .withConnectionId(connectionId)
        .withSourceId(source.sourceId)
        .withCatalog(ConfiguredAirbyteCatalog())
        .withBreakingChange(false)

    connectionService.writeStandardSync(connection)

    Assertions.assertEquals(
      destDef.withDefaultVersionId(actorDefinitionVersion.versionId),
      destinationService.getDestinationDefinitionFromConnection(connectionId),
    )
  }

  @ParameterizedTest
  @ValueSource(ints = [0, 1, 2, 10])
  fun testListStandardDestDefsHandlesTombstoneDestDefs(numDestinationDefinitions: Int) {
    val allDestinationDefinitions: MutableList<StandardDestinationDefinition?> = ArrayList()
    val notTombstoneDestinationDefinitions: MutableList<StandardDestinationDefinition?> = ArrayList()
    for (i in 0..<numDestinationDefinitions) {
      val isTombstone = i % 2 == 0 // every other is tombstone
      val destinationDefinition: StandardDestinationDefinition = createBaseDestDef().withTombstone(isTombstone)
      val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(destinationDefinition.destinationDefinitionId)
      allDestinationDefinitions.add(destinationDefinition)
      if (!isTombstone) {
        notTombstoneDestinationDefinitions.add(destinationDefinition)
      }
      destinationService.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion, mutableListOf())
      destinationDefinition.defaultVersionId = actorDefinitionVersion.versionId
    }

    val returnedDestDefsWithoutTombstone: MutableList<StandardDestinationDefinition> =
      destinationService.listStandardDestinationDefinitions(false)
    Assertions.assertEquals(notTombstoneDestinationDefinitions, returnedDestDefsWithoutTombstone)

    val returnedDestDefsWithTombstone: MutableList<StandardDestinationDefinition> = destinationService.listStandardDestinationDefinitions(true)
    Assertions.assertEquals(allDestinationDefinitions, returnedDestDefsWithTombstone)
  }

  @Test
  fun testUpdateDeclarativeActorDefinitionVersions() {
    val declarativeDockerRepository = "airbyte/source-declarative-manifest"
    val previousTag = "0.1.0"
    val newTag = "0.2.0"
    val differentMajorTag = "1.0.0"

    // Write multiple definitions to be updated and one to not be updated
    val sourceDef: StandardSourceDefinition = createBaseSourceDef()
    val adv: ActorDefinitionVersion =
      createBaseActorDefVersion(sourceDef.sourceDefinitionId)
        .withDockerRepository(declarativeDockerRepository)
        .withDockerImageTag(previousTag)
    sourceService.writeConnectorMetadata(sourceDef, adv, mutableListOf())

    val sourceDef2: StandardSourceDefinition = createBaseSourceDef()
    val adv2: ActorDefinitionVersion =
      createBaseActorDefVersion(sourceDef2.sourceDefinitionId)
        .withDockerRepository(declarativeDockerRepository)
        .withDockerImageTag(previousTag)
    sourceService.writeConnectorMetadata(sourceDef2, adv2, mutableListOf())

    val sourceDef3: StandardSourceDefinition = createBaseSourceDef()
    val adv3: ActorDefinitionVersion =
      createBaseActorDefVersion(sourceDef3.sourceDefinitionId)
        .withDockerRepository(declarativeDockerRepository)
        .withDockerImageTag(differentMajorTag)
    sourceService.writeConnectorMetadata(sourceDef3, adv3, mutableListOf())

    val numUpdated = actorDefinitionService.updateDeclarativeActorDefinitionVersions(previousTag, newTag)
    Assertions.assertEquals(2, numUpdated)

    val updatedSourceDef = sourceService.getStandardSourceDefinition(sourceDef.sourceDefinitionId)
    val updatedSourceDef2 = sourceService.getStandardSourceDefinition(sourceDef2.sourceDefinitionId)
    val persistedSourceDef3 = sourceService.getStandardSourceDefinition(sourceDef3.sourceDefinitionId)

    // Definitions on the previous tag should be updated to the new tag
    Assertions.assertEquals(
      newTag,
      actorDefinitionService.getActorDefinitionVersion(updatedSourceDef.defaultVersionId).dockerImageTag,
    )
    Assertions.assertEquals(
      newTag,
      actorDefinitionService.getActorDefinitionVersion(updatedSourceDef2.defaultVersionId).dockerImageTag,
    )
    // Definitions on a different version don't get updated
    Assertions.assertEquals(
      differentMajorTag,
      actorDefinitionService.getActorDefinitionVersion(persistedSourceDef3.defaultVersionId).dockerImageTag,
    )
  }

  @Test
  fun getActorDefinitionIdsInUse() {
    val workspace: StandardWorkspace = createBaseStandardWorkspace()
    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    val sourceDefInUse: StandardSourceDefinition = createBaseSourceDef()
    val actorDefinitionVersion3: ActorDefinitionVersion = createBaseActorDefVersion(sourceDefInUse.sourceDefinitionId)
    sourceService.writeConnectorMetadata(sourceDefInUse, actorDefinitionVersion3, mutableListOf())
    val sourceConnection: SourceConnection = createSource(sourceDefInUse.sourceDefinitionId, workspace.workspaceId)
    sourceService.writeSourceConnectionNoSecrets(sourceConnection)

    val sourceDefNotInUse: StandardSourceDefinition = createBaseSourceDef()
    val actorDefinitionVersion4: ActorDefinitionVersion = createBaseActorDefVersion(sourceDefNotInUse.sourceDefinitionId)
    sourceService.writeConnectorMetadata(sourceDefNotInUse, actorDefinitionVersion4, mutableListOf())

    val destDefInUse: StandardDestinationDefinition = createBaseDestDef()
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(destDefInUse.destinationDefinitionId)
    destinationService.writeConnectorMetadata(destDefInUse, actorDefinitionVersion, mutableListOf())
    val destinationConnection: DestinationConnection = createDest(destDefInUse.destinationDefinitionId, workspace.workspaceId)
    destinationService.writeDestinationConnectionNoSecrets(destinationConnection)

    val destDefNotInUse: StandardDestinationDefinition = createBaseDestDef()
    val actorDefinitionVersion2: ActorDefinitionVersion = createBaseActorDefVersion(destDefNotInUse.destinationDefinitionId)
    destinationService.writeConnectorMetadata(destDefNotInUse, actorDefinitionVersion2, mutableListOf())

    Assertions.assertTrue(actorDefinitionService.getActorDefinitionIdsInUse().contains(sourceDefInUse.sourceDefinitionId))
    Assertions.assertTrue(actorDefinitionService.getActorDefinitionIdsInUse().contains(destDefInUse.destinationDefinitionId))
    Assertions.assertFalse(actorDefinitionService.getActorDefinitionIdsInUse().contains(sourceDefNotInUse.sourceDefinitionId))
    Assertions.assertFalse(actorDefinitionService.getActorDefinitionIdsInUse().contains(destDefNotInUse.destinationDefinitionId))
  }

  @Test
  fun testGetActorDefinitionIdsToDefaultVersionsMap() {
    val sourceDef: StandardSourceDefinition = createBaseSourceDef()
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(sourceDef.sourceDefinitionId)
    sourceService.writeConnectorMetadata(sourceDef, actorDefinitionVersion, mutableListOf())

    val destDef: StandardDestinationDefinition = createBaseDestDef()
    val actorDefinitionVersion2: ActorDefinitionVersion = createBaseActorDefVersion(destDef.destinationDefinitionId)
    destinationService.writeConnectorMetadata(destDef, actorDefinitionVersion2, mutableListOf())

    val actorDefIdToDefaultVersionId: Map<UUID, ActorDefinitionVersion> =
      actorDefinitionService.getActorDefinitionIdsToDefaultVersionsMap()
    Assertions.assertEquals(actorDefIdToDefaultVersionId.size, 2)
    Assertions.assertEquals(actorDefIdToDefaultVersionId[sourceDef.sourceDefinitionId], actorDefinitionVersion)
    Assertions.assertEquals(actorDefIdToDefaultVersionId[destDef.destinationDefinitionId], actorDefinitionVersion2)
  }

  @Test
  fun testUpdateStandardSourceDefinition() {
    val sourceDefinition: StandardSourceDefinition = createBaseSourceDef()
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(sourceDefinition.sourceDefinitionId)

    sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, mutableListOf())

    val sourceDefinitionFromDB =
      sourceService.getStandardSourceDefinition(sourceDefinition.sourceDefinitionId)
    Assertions.assertEquals(sourceDefinition.withDefaultVersionId(actorDefinitionVersion.versionId), sourceDefinitionFromDB)

    val sourceDefinition2 =
      sourceDefinition
        .withName("new name")
        .withIcon("updated icon")
        .withTombstone(true)
    sourceService.updateStandardSourceDefinition(sourceDefinition2)

    val sourceDefinition2FromDB =
      sourceService.getStandardSourceDefinition(sourceDefinition.sourceDefinitionId)

    // The default version has not changed
    Assertions.assertEquals(sourceDefinition2FromDB.defaultVersionId, sourceDefinitionFromDB.defaultVersionId)

    // Source definition has been updated
    Assertions.assertEquals(sourceDefinition2.withDefaultVersionId(actorDefinitionVersion.versionId), sourceDefinition2FromDB)
  }

  @Test
  fun testUpdateNonexistentStandardSourceDefinitionThrows() {
    val sourceDefinition: StandardSourceDefinition = createBaseSourceDef()
    Assertions.assertThrows(
      ConfigNotFoundException::class.java,
    ) { sourceService.updateStandardSourceDefinition(sourceDefinition) }
  }

  @Test
  fun testUpdateStandardDestinationDefinition() {
    val destinationDefinition: StandardDestinationDefinition = createBaseDestDef()
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(destinationDefinition.destinationDefinitionId)

    destinationService.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion, mutableListOf())

    val destinationDefinitionFromDB =
      destinationService.getStandardDestinationDefinition(destinationDefinition.destinationDefinitionId)
    Assertions.assertEquals(destinationDefinition.withDefaultVersionId(actorDefinitionVersion.versionId), destinationDefinitionFromDB)

    val destinationDefinition2 =
      destinationDefinition
        .withName("new name")
        .withIcon("updated icon")
        .withTombstone(true)
    destinationService.updateStandardDestinationDefinition(destinationDefinition2)

    val destinationDefinition2FromDB =
      destinationService.getStandardDestinationDefinition(destinationDefinition.destinationDefinitionId)

    // The default version has not changed
    Assertions.assertEquals(destinationDefinition2FromDB.defaultVersionId, destinationDefinitionFromDB.defaultVersionId)

    // Destination definition has been updated
    Assertions.assertEquals(destinationDefinition2.withDefaultVersionId(actorDefinitionVersion.versionId), destinationDefinition2FromDB)
  }

  @Test
  fun testUpdateNonexistentStandardDestinationDefinitionThrows() {
    val destinationDefinition: StandardDestinationDefinition = createBaseDestDef()
    Assertions.assertThrows(
      ConfigNotFoundException::class.java,
    ) { destinationService.updateStandardDestinationDefinition(destinationDefinition) }
  }

  companion object {
    private const val TEST_DEFAULT_MAX_SECONDS = "3600"
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val DEFAULT_ORGANIZATION_ID: UUID = UUID.randomUUID()
    private const val DOCKER_IMAGE_TAG = "0.0.1"
    private val dataplaneGroupId: UUID = UUID.randomUUID()

    private fun createSource(
      sourceDefId: UUID?,
      workspaceId: UUID?,
    ): SourceConnection =
      SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withSourceDefinitionId(sourceDefId)
        .withWorkspaceId(workspaceId)
        .withName("source")

    private fun createDest(
      destDefId: UUID?,
      workspaceId: UUID?,
    ): DestinationConnection =
      DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withDestinationDefinitionId(destDefId)
        .withWorkspaceId(workspaceId)
        .withName("dest")

    private fun createBaseSourceDef(): StandardSourceDefinition {
      val id = UUID.randomUUID()

      return StandardSourceDefinition()
        .withName("source-def-$id")
        .withSourceDefinitionId(id)
        .withTombstone(false)
        .withMaxSecondsBetweenMessages(MockData.DEFAULT_MAX_SECONDS_BETWEEN_MESSAGES)
    }

    private fun createBaseActorDefVersion(actorDefId: UUID?): ActorDefinitionVersion =
      ActorDefinitionVersion()
        .withVersionId(UUID.randomUUID())
        .withActorDefinitionId(actorDefId)
        .withDockerRepository("source-image-$actorDefId")
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSupportLevel(SupportLevel.COMMUNITY)
        .withInternalSupportLevel(100L)
        .withProtocolVersion("0.2.0")

    private fun createBaseSourceDefWithoutMaxSecondsBetweenMessages(): StandardSourceDefinition {
      val id = UUID.randomUUID()

      return StandardSourceDefinition()
        .withName("source-def-$id")
        .withSourceDefinitionId(id)
        .withTombstone(false)
    }

    private fun createBaseDestDef(): StandardDestinationDefinition {
      val id = UUID.randomUUID()

      return StandardDestinationDefinition()
        .withName("source-def-$id")
        .withDestinationDefinitionId(id)
        .withTombstone(false)
    }

    private fun createBaseStandardWorkspace(): StandardWorkspace =
      StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withName("workspace-a")
        .withSlug("workspace-a-slug")
        .withInitialSetupComplete(false)
        .withTombstone(false)
        .withDataplaneGroupId(dataplaneGroupId)
        .withOrganizationId(DEFAULT_ORGANIZATION_ID)
  }
}
