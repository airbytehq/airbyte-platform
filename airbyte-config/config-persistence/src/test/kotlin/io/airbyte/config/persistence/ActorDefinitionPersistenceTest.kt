/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito
import java.io.IOException
import java.sql.SQLException
import java.util.UUID

internal class ActorDefinitionPersistenceTest : BaseConfigDatabaseTest() {
  private lateinit var actorDefinitionService: ActorDefinitionService
  private lateinit var sourceService: SourceServiceJooqImpl
  private lateinit var destinationService: DestinationServiceJooqImpl
  private lateinit var workspaceService: WorkspaceService
  private lateinit var connectionService: ConnectionService
  private lateinit var dataplaneGroupService: DataplaneGroupService

  @BeforeEach
  @Throws(SQLException::class, IOException::class)
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
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testSourceDefinitionWithNullTombstone() {
    assertReturnsSrcDef(createBaseSourceDef())
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testSourceDefinitionWithTrueTombstone() {
    assertReturnsSrcDef(createBaseSourceDef().withTombstone(true))
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testSourceDefinitionWithFalseTombstone() {
    assertReturnsSrcDef(createBaseSourceDef().withTombstone(false))
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testSourceDefinitionDefaultMaxSeconds() {
    assertReturnsSrcDefDefaultMaxSecondsBetweenMessages(createBaseSourceDefWithoutMaxSecondsBetweenMessages())
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testSourceDefinitionMaxSecondsGreaterThenDefaultShouldReturnConfigured() {
    assertReturnsSrcDef(
      createBaseSourceDefWithoutMaxSecondsBetweenMessages().withMaxSecondsBetweenMessages(TEST_DEFAULT_MAX_SECONDS.toLong() + 1),
    )
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testSourceDefinitionMaxSecondsLessThenDefaultShouldReturnDefault() {
    val def: StandardSourceDefinition = createBaseSourceDefWithoutMaxSecondsBetweenMessages().withMaxSecondsBetweenMessages(1L)
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(def.getSourceDefinitionId())
    sourceService.writeConnectorMetadata(def, actorDefinitionVersion, mutableListOf())
    val exp =
      def.withDefaultVersionId(actorDefinitionVersion.getVersionId()).withMaxSecondsBetweenMessages(TEST_DEFAULT_MAX_SECONDS.toLong())
    Assertions.assertEquals(exp, sourceService.getStandardSourceDefinition(def.getSourceDefinitionId()))
  }

  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
  private fun assertReturnsSrcDef(srcDef: StandardSourceDefinition) {
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(srcDef.getSourceDefinitionId())
    sourceService.writeConnectorMetadata(srcDef, actorDefinitionVersion, mutableListOf())
    Assertions.assertEquals(
      srcDef.withDefaultVersionId(actorDefinitionVersion.getVersionId()),
      sourceService.getStandardSourceDefinition(srcDef.getSourceDefinitionId()),
    )
  }

  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
  private fun assertReturnsSrcDefDefaultMaxSecondsBetweenMessages(srcDef: StandardSourceDefinition) {
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(srcDef.getSourceDefinitionId())
    sourceService.writeConnectorMetadata(srcDef, actorDefinitionVersion, mutableListOf())
    Assertions.assertEquals(
      srcDef
        .withDefaultVersionId(actorDefinitionVersion.getVersionId())
        .withMaxSecondsBetweenMessages(MockData.DEFAULT_MAX_SECONDS_BETWEEN_MESSAGES),
      sourceService.getStandardSourceDefinition(srcDef.getSourceDefinitionId()),
    )
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class)
  fun testGetSourceDefinitionFromSource() {
    val workspace: StandardWorkspace = createBaseStandardWorkspace()
    val srcDef: StandardSourceDefinition = createBaseSourceDef().withTombstone(false)
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(srcDef.getSourceDefinitionId())
    val source: SourceConnection = createSource(srcDef.getSourceDefinitionId(), workspace.getWorkspaceId())
    workspaceService.writeStandardWorkspaceNoSecrets(workspace)
    sourceService.writeConnectorMetadata(srcDef, actorDefinitionVersion, mutableListOf())
    sourceService.writeSourceConnectionNoSecrets(source)

    Assertions.assertEquals(
      srcDef.withDefaultVersionId(actorDefinitionVersion.getVersionId()),
      sourceService.getSourceDefinitionFromSource(source.getSourceId()),
    )
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testGetSourceDefinitionsFromConnection() {
    val workspace: StandardWorkspace = createBaseStandardWorkspace()
    val destDef: StandardDestinationDefinition = createBaseDestDef().withTombstone(false)
    val destActorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(destDef.getDestinationDefinitionId())
    val dest: DestinationConnection = createDest(destDef.getDestinationDefinitionId(), workspace.getWorkspaceId())
    val srcDef: StandardSourceDefinition = createBaseSourceDef().withTombstone(false)
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(srcDef.getSourceDefinitionId())
    val source: SourceConnection = createSource(srcDef.getSourceDefinitionId(), workspace.getWorkspaceId())
    workspaceService.writeStandardWorkspaceNoSecrets(workspace)
    sourceService.writeConnectorMetadata(srcDef, actorDefinitionVersion, mutableListOf())
    sourceService.writeSourceConnectionNoSecrets(source)
    destinationService.writeConnectorMetadata(destDef, destActorDefinitionVersion, mutableListOf())
    destinationService.writeDestinationConnectionNoSecrets(dest)

    val connectionId = UUID.randomUUID()
    val connection =
      StandardSync()
        .withName("Test Sync")
        .withDestinationId(dest.getDestinationId())
        .withConnectionId(connectionId)
        .withSourceId(source.getSourceId())
        .withCatalog(ConfiguredAirbyteCatalog())
        .withBreakingChange(false)

    connectionService.writeStandardSync(connection)

    Assertions.assertEquals(
      srcDef.withDefaultVersionId(actorDefinitionVersion.getVersionId()),
      sourceService.getSourceDefinitionFromConnection(connectionId),
    )
  }

  @ParameterizedTest
  @ValueSource(ints = [0, 1, 2, 10])
  @Throws(IOException::class)
  fun testListStandardSourceDefsHandlesTombstoneSourceDefs(numSrcDefs: Int) {
    val allSourceDefinitions: MutableList<StandardSourceDefinition?> = ArrayList<StandardSourceDefinition?>()
    val notTombstoneSourceDefinitions: MutableList<StandardSourceDefinition?> = ArrayList<StandardSourceDefinition?>()
    for (i in 0..<numSrcDefs) {
      val isTombstone = i % 2 == 0 // every other is tombstone
      val sourceDefinition: StandardSourceDefinition = createBaseSourceDef().withTombstone(isTombstone)
      val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId())
      allSourceDefinitions.add(sourceDefinition)
      if (!isTombstone) {
        notTombstoneSourceDefinitions.add(sourceDefinition)
      }
      sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, mutableListOf())
      sourceDefinition.setDefaultVersionId(actorDefinitionVersion.getVersionId())
    }

    val returnedSrcDefsWithoutTombstone: MutableList<StandardSourceDefinition> = sourceService.listStandardSourceDefinitions(false)
    Assertions.assertEquals(notTombstoneSourceDefinitions, returnedSrcDefsWithoutTombstone)

    val returnedSrcDefsWithTombstone: MutableList<StandardSourceDefinition> = sourceService.listStandardSourceDefinitions(true)
    Assertions.assertEquals(allSourceDefinitions, returnedSrcDefsWithTombstone)
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testDestinationDefinitionWithNullTombstone() {
    assertReturnsDestDef(createBaseDestDef())
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testDestinationDefinitionWithTrueTombstone() {
    assertReturnsDestDef(createBaseDestDef().withTombstone(true))
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testDestinationDefinitionWithFalseTombstone() {
    assertReturnsDestDef(createBaseDestDef().withTombstone(false))
  }

  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
  fun assertReturnsDestDef(destDef: StandardDestinationDefinition) {
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(destDef.getDestinationDefinitionId())
    destinationService.writeConnectorMetadata(destDef, actorDefinitionVersion, mutableListOf())
    Assertions.assertEquals(
      destDef.withDefaultVersionId(actorDefinitionVersion.getVersionId()),
      destinationService.getStandardDestinationDefinition(destDef.getDestinationDefinitionId()),
    )
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class)
  fun testGetDestinationDefinitionFromDestination() {
    val workspace: StandardWorkspace = createBaseStandardWorkspace()
    val destDef: StandardDestinationDefinition = createBaseDestDef().withTombstone(false)
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(destDef.getDestinationDefinitionId())
    val dest: DestinationConnection = createDest(destDef.getDestinationDefinitionId(), workspace.getWorkspaceId())
    workspaceService.writeStandardWorkspaceNoSecrets(workspace)
    destinationService.writeConnectorMetadata(destDef, actorDefinitionVersion, mutableListOf())
    destinationService.writeDestinationConnectionNoSecrets(dest)

    Assertions.assertEquals(
      destDef.withDefaultVersionId(actorDefinitionVersion.getVersionId()),
      destinationService.getDestinationDefinitionFromDestination(dest.getDestinationId()),
    )
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testGetDestinationDefinitionsFromConnection() {
    val workspace: StandardWorkspace = createBaseStandardWorkspace()
    val destDef: StandardDestinationDefinition = createBaseDestDef().withTombstone(false)
    val sourceDefinition: StandardSourceDefinition = createBaseSourceDef().withTombstone(false)
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(destDef.getDestinationDefinitionId())
    val sourceActorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId())
    val dest: DestinationConnection = createDest(destDef.getDestinationDefinitionId(), workspace.getWorkspaceId())
    val source: SourceConnection = createSource(sourceDefinition.getSourceDefinitionId(), workspace.getWorkspaceId())
    workspaceService.writeStandardWorkspaceNoSecrets(workspace)
    destinationService.writeConnectorMetadata(destDef, actorDefinitionVersion, mutableListOf())
    sourceService.writeConnectorMetadata(sourceDefinition, sourceActorDefinitionVersion, mutableListOf())
    destinationService.writeDestinationConnectionNoSecrets(dest)
    sourceService.writeSourceConnectionNoSecrets(source)

    val connectionId = UUID.randomUUID()
    val connection =
      StandardSync()
        .withName("Test Sync")
        .withDestinationId(dest.getDestinationId())
        .withConnectionId(connectionId)
        .withSourceId(source.getSourceId())
        .withCatalog(ConfiguredAirbyteCatalog())
        .withBreakingChange(false)

    connectionService.writeStandardSync(connection)

    Assertions.assertEquals(
      destDef.withDefaultVersionId(actorDefinitionVersion.getVersionId()),
      destinationService.getDestinationDefinitionFromConnection(connectionId),
    )
  }

  @ParameterizedTest
  @ValueSource(ints = [0, 1, 2, 10])
  @Throws(IOException::class)
  fun testListStandardDestDefsHandlesTombstoneDestDefs(numDestinationDefinitions: Int) {
    val allDestinationDefinitions: MutableList<StandardDestinationDefinition?> = ArrayList<StandardDestinationDefinition?>()
    val notTombstoneDestinationDefinitions: MutableList<StandardDestinationDefinition?> = ArrayList<StandardDestinationDefinition?>()
    for (i in 0..<numDestinationDefinitions) {
      val isTombstone = i % 2 == 0 // every other is tombstone
      val destinationDefinition: StandardDestinationDefinition = createBaseDestDef().withTombstone(isTombstone)
      val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId())
      allDestinationDefinitions.add(destinationDefinition)
      if (!isTombstone) {
        notTombstoneDestinationDefinitions.add(destinationDefinition)
      }
      destinationService.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion, mutableListOf())
      destinationDefinition.setDefaultVersionId(actorDefinitionVersion.getVersionId())
    }

    val returnedDestDefsWithoutTombstone: MutableList<StandardDestinationDefinition> =
      destinationService.listStandardDestinationDefinitions(false)
    Assertions.assertEquals(notTombstoneDestinationDefinitions, returnedDestDefsWithoutTombstone)

    val returnedDestDefsWithTombstone: MutableList<StandardDestinationDefinition> = destinationService.listStandardDestinationDefinitions(true)
    Assertions.assertEquals(allDestinationDefinitions, returnedDestDefsWithTombstone)
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testUpdateDeclarativeActorDefinitionVersions() {
    val declarativeDockerRepository = "airbyte/source-declarative-manifest"
    val previousTag = "0.1.0"
    val newTag = "0.2.0"
    val differentMajorTag = "1.0.0"

    // Write multiple definitions to be updated and one to not be updated
    val sourceDef: StandardSourceDefinition = createBaseSourceDef()
    val adv: ActorDefinitionVersion =
      createBaseActorDefVersion(sourceDef.getSourceDefinitionId())
        .withDockerRepository(declarativeDockerRepository)
        .withDockerImageTag(previousTag)
    sourceService.writeConnectorMetadata(sourceDef, adv, mutableListOf())

    val sourceDef2: StandardSourceDefinition = createBaseSourceDef()
    val adv2: ActorDefinitionVersion =
      createBaseActorDefVersion(sourceDef2.getSourceDefinitionId())
        .withDockerRepository(declarativeDockerRepository)
        .withDockerImageTag(previousTag)
    sourceService.writeConnectorMetadata(sourceDef2, adv2, mutableListOf())

    val sourceDef3: StandardSourceDefinition = createBaseSourceDef()
    val adv3: ActorDefinitionVersion =
      createBaseActorDefVersion(sourceDef3.getSourceDefinitionId())
        .withDockerRepository(declarativeDockerRepository)
        .withDockerImageTag(differentMajorTag)
    sourceService.writeConnectorMetadata(sourceDef3, adv3, mutableListOf())

    val numUpdated = actorDefinitionService.updateDeclarativeActorDefinitionVersions(previousTag, newTag)
    Assertions.assertEquals(2, numUpdated)

    val updatedSourceDef = sourceService.getStandardSourceDefinition(sourceDef.getSourceDefinitionId())
    val updatedSourceDef2 = sourceService.getStandardSourceDefinition(sourceDef2.getSourceDefinitionId())
    val persistedSourceDef3 = sourceService.getStandardSourceDefinition(sourceDef3.getSourceDefinitionId())

    // Definitions that were on the previous tag should be updated to the new tag
    Assertions.assertEquals(
      newTag,
      actorDefinitionService.getActorDefinitionVersion(updatedSourceDef.getDefaultVersionId()).getDockerImageTag(),
    )
    Assertions.assertEquals(
      newTag,
      actorDefinitionService.getActorDefinitionVersion(updatedSourceDef2.getDefaultVersionId()).getDockerImageTag(),
    )
    // Definitions on a different version don't get updated
    Assertions.assertEquals(
      differentMajorTag,
      actorDefinitionService.getActorDefinitionVersion(persistedSourceDef3.getDefaultVersionId()).getDockerImageTag(),
    )
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class)
  fun getActorDefinitionIdsInUse() {
    val workspace: StandardWorkspace = createBaseStandardWorkspace()
    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    val sourceDefInUse: StandardSourceDefinition = createBaseSourceDef()
    val actorDefinitionVersion3: ActorDefinitionVersion = createBaseActorDefVersion(sourceDefInUse.getSourceDefinitionId())
    sourceService.writeConnectorMetadata(sourceDefInUse, actorDefinitionVersion3, mutableListOf())
    val sourceConnection: SourceConnection = createSource(sourceDefInUse.getSourceDefinitionId(), workspace.getWorkspaceId())
    sourceService.writeSourceConnectionNoSecrets(sourceConnection)

    val sourceDefNotInUse: StandardSourceDefinition = createBaseSourceDef()
    val actorDefinitionVersion4: ActorDefinitionVersion = createBaseActorDefVersion(sourceDefNotInUse.getSourceDefinitionId())
    sourceService.writeConnectorMetadata(sourceDefNotInUse, actorDefinitionVersion4, mutableListOf())

    val destDefInUse: StandardDestinationDefinition = createBaseDestDef()
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(destDefInUse.getDestinationDefinitionId())
    destinationService.writeConnectorMetadata(destDefInUse, actorDefinitionVersion, mutableListOf())
    val destinationConnection: DestinationConnection = createDest(destDefInUse.getDestinationDefinitionId(), workspace.getWorkspaceId())
    destinationService.writeDestinationConnectionNoSecrets(destinationConnection)

    val destDefNotInUse: StandardDestinationDefinition = createBaseDestDef()
    val actorDefinitionVersion2: ActorDefinitionVersion = createBaseActorDefVersion(destDefNotInUse.getDestinationDefinitionId())
    destinationService.writeConnectorMetadata(destDefNotInUse, actorDefinitionVersion2, mutableListOf())

    Assertions.assertTrue(actorDefinitionService.getActorDefinitionIdsInUse().contains(sourceDefInUse.getSourceDefinitionId()))
    Assertions.assertTrue(actorDefinitionService.getActorDefinitionIdsInUse().contains(destDefInUse.getDestinationDefinitionId()))
    Assertions.assertFalse(actorDefinitionService.getActorDefinitionIdsInUse().contains(sourceDefNotInUse.getSourceDefinitionId()))
    Assertions.assertFalse(actorDefinitionService.getActorDefinitionIdsInUse().contains(destDefNotInUse.getDestinationDefinitionId()))
  }

  @Test
  @Throws(IOException::class)
  fun testGetActorDefinitionIdsToDefaultVersionsMap() {
    val sourceDef: StandardSourceDefinition = createBaseSourceDef()
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(sourceDef.getSourceDefinitionId())
    sourceService.writeConnectorMetadata(sourceDef, actorDefinitionVersion, mutableListOf())

    val destDef: StandardDestinationDefinition = createBaseDestDef()
    val actorDefinitionVersion2: ActorDefinitionVersion = createBaseActorDefVersion(destDef.getDestinationDefinitionId())
    destinationService.writeConnectorMetadata(destDef, actorDefinitionVersion2, mutableListOf())

    val actorDefIdToDefaultVersionId: Map<UUID, ActorDefinitionVersion> =
      actorDefinitionService.getActorDefinitionIdsToDefaultVersionsMap()
    Assertions.assertEquals(actorDefIdToDefaultVersionId.size, 2)
    Assertions.assertEquals(actorDefIdToDefaultVersionId.get(sourceDef.getSourceDefinitionId()), actorDefinitionVersion)
    Assertions.assertEquals(actorDefIdToDefaultVersionId.get(destDef.getDestinationDefinitionId()), actorDefinitionVersion2)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testUpdateStandardSourceDefinition() {
    val sourceDefinition: StandardSourceDefinition = createBaseSourceDef()
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId())

    sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, mutableListOf())

    val sourceDefinitionFromDB =
      sourceService.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId())
    Assertions.assertEquals(sourceDefinition.withDefaultVersionId(actorDefinitionVersion.getVersionId()), sourceDefinitionFromDB)

    val sourceDefinition2 =
      sourceDefinition
        .withName("new name")
        .withIcon("updated icon")
        .withTombstone(true)
    sourceService.updateStandardSourceDefinition(sourceDefinition2)

    val sourceDefinition2FromDB =
      sourceService.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId())

    // Default version has not changed
    Assertions.assertEquals(sourceDefinition2FromDB.getDefaultVersionId(), sourceDefinitionFromDB.getDefaultVersionId())

    // Source definition has been updated
    Assertions.assertEquals(sourceDefinition2.withDefaultVersionId(actorDefinitionVersion.getVersionId()), sourceDefinition2FromDB)
  }

  @Test
  fun testUpdateNonexistentStandardSourceDefinitionThrows() {
    val sourceDefinition: StandardSourceDefinition = createBaseSourceDef()
    Assertions.assertThrows<ConfigNotFoundException?>(
      ConfigNotFoundException::class.java,
      Executable { sourceService.updateStandardSourceDefinition(sourceDefinition) },
    )
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testUpdateStandardDestinationDefinition() {
    val destinationDefinition: StandardDestinationDefinition = createBaseDestDef()
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId())

    destinationService.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion, mutableListOf())

    val destinationDefinitionFromDB =
      destinationService.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId())
    Assertions.assertEquals(destinationDefinition.withDefaultVersionId(actorDefinitionVersion.getVersionId()), destinationDefinitionFromDB)

    val destinationDefinition2 =
      destinationDefinition
        .withName("new name")
        .withIcon("updated icon")
        .withTombstone(true)
    destinationService.updateStandardDestinationDefinition(destinationDefinition2)

    val destinationDefinition2FromDB =
      destinationService.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId())

    // Default version has not changed
    Assertions.assertEquals(destinationDefinition2FromDB.getDefaultVersionId(), destinationDefinitionFromDB.getDefaultVersionId())

    // Destination definition has been updated
    Assertions.assertEquals(destinationDefinition2.withDefaultVersionId(actorDefinitionVersion.getVersionId()), destinationDefinition2FromDB)
  }

  @Test
  fun testUpdateNonexistentStandardDestinationDefinitionThrows() {
    val destinationDefinition: StandardDestinationDefinition = createBaseDestDef()
    Assertions.assertThrows<ConfigNotFoundException?>(
      ConfigNotFoundException::class.java,
      Executable { destinationService.updateStandardDestinationDefinition(destinationDefinition) },
    )
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
        .withName("source-def-" + id)
        .withSourceDefinitionId(id)
        .withTombstone(false)
        .withMaxSecondsBetweenMessages(MockData.DEFAULT_MAX_SECONDS_BETWEEN_MESSAGES)
    }

    private fun createBaseActorDefVersion(actorDefId: UUID?): ActorDefinitionVersion =
      ActorDefinitionVersion()
        .withVersionId(UUID.randomUUID())
        .withActorDefinitionId(actorDefId)
        .withDockerRepository("source-image-" + actorDefId)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSupportLevel(SupportLevel.COMMUNITY)
        .withInternalSupportLevel(100L)
        .withProtocolVersion("0.2.0")

    private fun createBaseSourceDefWithoutMaxSecondsBetweenMessages(): StandardSourceDefinition {
      val id = UUID.randomUUID()

      return StandardSourceDefinition()
        .withName("source-def-" + id)
        .withSourceDefinitionId(id)
        .withTombstone(false)
    }

    private fun createBaseDestDef(): StandardDestinationDefinition {
      val id = UUID.randomUUID()

      return StandardDestinationDefinition()
        .withName("source-def-" + id)
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
