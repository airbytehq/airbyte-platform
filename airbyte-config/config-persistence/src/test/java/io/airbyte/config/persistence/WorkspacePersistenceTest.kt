/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.AuthProvider
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.DestinationConnection
import io.airbyte.config.Permission
import io.airbyte.config.ReleaseStage
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
import io.airbyte.data.services.impls.data.DataplaneGroupServiceTestJooqImpl
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl
import io.airbyte.data.services.shared.ActorServicePaginationHelper
import io.airbyte.data.services.shared.ResourcesByOrganizationQueryPaginated
import io.airbyte.data.services.shared.ResourcesByUserQueryPaginated
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.test.utils.BaseConfigDatabaseTest
import io.airbyte.validation.json.JsonValidationException
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.mockito.Mockito
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap
import java.io.IOException
import java.util.Optional
import java.util.Set
import java.util.UUID

internal class WorkspacePersistenceTest : BaseConfigDatabaseTest() {
  private lateinit var workspacePersistence: WorkspacePersistence
  private lateinit var userPersistence: UserPersistence
  private lateinit var featureFlagClient: FeatureFlagClient
  private lateinit var secretsRepositoryReader: SecretsRepositoryReader
  private lateinit var secretsRepositoryWriter: SecretsRepositoryWriter
  private lateinit var secretPersistenceConfigService: SecretPersistenceConfigService
  private lateinit var sourceService: SourceService
  private lateinit var destinationService: DestinationService
  private lateinit var connectionService: ConnectionService
  private lateinit var workspaceService: WorkspaceService
  private lateinit var dataplaneGroupService: DataplaneGroupService

  @BeforeEach
  @Throws(Exception::class)
  fun setup() {
    featureFlagClient = Mockito.mock<TestClient>(TestClient::class.java)
    secretsRepositoryReader = Mockito.mock<SecretsRepositoryReader>(SecretsRepositoryReader::class.java)
    secretsRepositoryWriter = Mockito.mock<SecretsRepositoryWriter>(SecretsRepositoryWriter::class.java)
    secretPersistenceConfigService = Mockito.mock<SecretPersistenceConfigService>(SecretPersistenceConfigService::class.java)
    dataplaneGroupService = DataplaneGroupServiceTestJooqImpl(database!!)
    connectionService = Mockito.spy<ConnectionServiceJooqImpl>(ConnectionServiceJooqImpl(database!!))

    val scopedConfigurationService = Mockito.mock<ScopedConfigurationService>(ScopedConfigurationService::class.java)
    val connectionTimelineEventService = Mockito.mock<ConnectionTimelineEventService>(ConnectionTimelineEventService::class.java)
    val actorDefinitionService: ActorDefinitionService = ActorDefinitionServiceJooqImpl(database!!)
    val actorDefinitionVersionUpdater =
      ActorDefinitionVersionUpdater(
        featureFlagClient,
        connectionService,
        actorDefinitionService,
        scopedConfigurationService,
        connectionTimelineEventService,
      )
    val metricClient = Mockito.mock<MetricClient>(MetricClient::class.java)
    val actorPaginationServiceHelper = Mockito.mock<ActorServicePaginationHelper>(ActorServicePaginationHelper::class.java)

    sourceService =
      Mockito.spy<SourceServiceJooqImpl>(
        SourceServiceJooqImpl(
          database!!,
          featureFlagClient,
          secretPersistenceConfigService,
          connectionService,
          actorDefinitionVersionUpdater,
          metricClient,
          actorPaginationServiceHelper,
        ),
      )
    destinationService =
      Mockito.spy<DestinationServiceJooqImpl>(
        DestinationServiceJooqImpl(
          database!!,
          featureFlagClient,
          connectionService,
          actorDefinitionVersionUpdater,
          metricClient,
          actorPaginationServiceHelper,
        ),
      )
    workspacePersistence = WorkspacePersistence(database!!)
    userPersistence = UserPersistence(database!!)
    val organizationPersistence = OrganizationPersistence(database!!)

    truncateAllTables()
    // Create all organizations, but avoid duplicates based on organization ID
    val createdOrgIds = mutableSetOf<UUID>()

    // Add default organization if not already created
    val defaultOrg = MockData.defaultOrganization()
    if (createdOrgIds.add(defaultOrg.organizationId)) {
      organizationPersistence.createOrganization(defaultOrg)
    }

    // Add other organizations if not already created
    MockData.organizations().filterNotNull().forEach { org ->
      if (createdOrgIds.add(org.organizationId)) {
        organizationPersistence.createOrganization(org)
      }
    }

    // Create dataplane groups, avoiding duplicates by using unique names
    dataplaneGroupService.writeDataplaneGroup(
      DataplaneGroup()
        .withId(MockData.DATAPLANE_GROUP_ID_DEFAULT)
        .withOrganizationId(MockData.DEFAULT_ORGANIZATION_ID)
        .withName("test-default")
        .withEnabled(true)
        .withTombstone(false),
    )
    dataplaneGroupService.writeDataplaneGroup(
      DataplaneGroup()
        .withId(MockData.DATAPLANE_GROUP_ID_ORG_1)
        .withOrganizationId(MockData.ORGANIZATION_ID_1)
        .withName("test-org1")
        .withEnabled(true)
        .withTombstone(false),
    )
    dataplaneGroupService.writeDataplaneGroup(
      DataplaneGroup()
        .withId(MockData.DATAPLANE_GROUP_ID_ORG_2)
        .withOrganizationId(MockData.ORGANIZATION_ID_2)
        .withName("test-org2")
        .withEnabled(true)
        .withTombstone(false),
    )
    dataplaneGroupService.writeDataplaneGroup(
      DataplaneGroup()
        .withId(MockData.DATAPLANE_GROUP_ID_ORG_3)
        .withOrganizationId(MockData.ORGANIZATION_ID_3)
        .withName("test-org3")
        .withEnabled(true)
        .withTombstone(false),
    )

    workspaceService =
      Mockito.spy<WorkspaceServiceJooqImpl>(
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
  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
  fun testGetWorkspace() {
    workspaceService.writeStandardWorkspaceNoSecrets(
      createBaseStandardWorkspace().withWorkspaceId(UUID.randomUUID()).withOrganizationId(MockData.DEFAULT_ORGANIZATION_ID),
    )
    assertReturnsWorkspace(createBaseStandardWorkspace().withDataplaneGroupId(MockData.DATAPLANE_GROUP_ID_DEFAULT))
  }

  @Test
  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
  fun testWorkspaceWithNullTombstone() {
    assertReturnsWorkspace(createBaseStandardWorkspace())
  }

  @Test
  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
  fun testWorkspaceWithFalseTombstone() {
    assertReturnsWorkspace(createBaseStandardWorkspace().withTombstone(false))
  }

  @Test
  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
  fun testWorkspaceWithTrueTombstone() {
    assertReturnsWorkspace(createBaseStandardWorkspace().withTombstone(true))
  }

  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
  fun assertReturnsWorkspace(workspace: StandardWorkspace) {
    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    val expectedWorkspace = clone<StandardWorkspace>(workspace)
        /*
         * tombstone defaults to false in the db, so if the passed in workspace does not have it set, we
         * expected the workspace returned from the db to have it set to false.
         */
    if (workspace.getTombstone() == null) {
      expectedWorkspace.withTombstone(false)
    }

    assertWorkspaceEquals(expectedWorkspace, workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true))
  }

  private fun assertWorkspaceEquals(
    expectedWorkspace: StandardWorkspace?,
    actualWorkspace: StandardWorkspace?,
  ) {
    Assertions
      .assertThat<StandardWorkspace?>(actualWorkspace)
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(expectedWorkspace)
  }

  private fun assertWorkspacesEqual(
    expectedWorkspaces: MutableSet<StandardWorkspace?>?,
    actualWorkspaces: MutableSet<StandardWorkspace?>?,
  ) {
    Assertions
      .assertThat<StandardWorkspace?>(actualWorkspaces)
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(expectedWorkspaces)
  }

  // @ParameterizedTest
  // @ValueSource(booleans = {true, false})
  // void testWorkspaceByConnectionId(final boolean isTombstone)
  // throws ConfigNotFoundException, IOException, JsonValidationException,
  // io.airbyte.data.ConfigNotFoundException {
  // final UUID connectionId = UUID.randomUUID();
  // final UUID sourceId = UUID.randomUUID();
  // final StandardSync mSync = new StandardSync()
  // .withSourceId(sourceId)
  // .withConnectionId(connectionId);
  // final SourceConnection mSourceConnection = new SourceConnection()
  // .withWorkspaceId(WORKSPACE_ID);
  // final StandardWorkspace mWorkflow = new StandardWorkspace()
  // .withWorkspaceId(WORKSPACE_ID);
  //
  // doReturn(mSync)
  // .when((WorkspaceServiceJooqImpl)workspaceService)
  // .getStandardSync(connectionId);
  // doReturn(mSourceConnection)
  // .when(configRepository)
  // .getSourceConnection(sourceId);
  // doReturn(mWorkflow)
  // .when(workspaceService)
  // .getStandardWorkspaceNoSecrets(WORKSPACE_ID, isTombstone);
  // doReturn(mWorkflow)
  // .when(workspaceService)
  // .getStandardWorkspaceFromConnection(connectionId, isTombstone);
  //
  // configRepository.getStandardWorkspaceFromConnection(connectionId, isTombstone);
  //
  // verify(workspaceService).getStandardWorkspaceNoSecrets(WORKSPACE_ID, isTombstone);
  // }
  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testUpdateFeedback() {
    val workspace: StandardWorkspace = createBaseStandardWorkspace()

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    org.junit.jupiter.api.Assertions.assertNull(
      workspaceService.getStandardWorkspaceNoSecrets(workspace.getWorkspaceId(), false).getFeedbackDone(),
    )
    workspaceService.setFeedback(workspace.getWorkspaceId())
    org.junit.jupiter.api.Assertions.assertTrue(
      workspaceService.getStandardWorkspaceNoSecrets(workspace.getWorkspaceId(), false).getFeedbackDone(),
    )
  }

  @ParameterizedTest
  @CsvSource(
    "GENERALLY_AVAILABLE, GENERALLY_AVAILABLE, false",
    "ALPHA, GENERALLY_AVAILABLE, true",
    "GENERALLY_AVAILABLE, BETA, true",
    "CUSTOM, CUSTOM, false",
  )
  @Throws(JsonValidationException::class, IOException::class)
  fun testWorkspaceHasAlphaOrBetaConnector(
    sourceReleaseStage: ReleaseStage?,
    destinationReleaseStage: ReleaseStage?,
    expectation: Boolean,
  ) {
    val workspace: StandardWorkspace = createBaseStandardWorkspace()
    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    sourceService.writeConnectorMetadata(
      createSourceDefinition()!!,
      createActorDefinitionVersion(SOURCE_DEFINITION_ID, sourceReleaseStage)!!,
      mutableListOf(),
    )
    destinationService.writeConnectorMetadata(
      createDestinationDefinition()!!,
      WorkspacePersistenceTest.Companion.createActorDefinitionVersion(
        WorkspacePersistenceTest.Companion.DESTINATION_DEFINITION_ID,
        destinationReleaseStage,
      )!!,
      mutableListOf(),
    )

    sourceService.writeSourceConnectionNoSecrets(createBaseSource()!!)
    destinationService.writeDestinationConnectionNoSecrets(createBaseDestination()!!)

    org.junit.jupiter.api.Assertions
      .assertEquals(expectation, workspaceService.getWorkspaceHasAlphaOrBetaConnector(WORKSPACE_ID))
  }

  @Test
  @Throws(Exception::class)
  fun testListWorkspacesInOrgNoKeyword() {
    val workspace: StandardWorkspace =
      createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_1)
        .withDataplaneGroupId(MockData.DATAPLANE_GROUP_ID_ORG_1)
    val otherWorkspace: StandardWorkspace =
      createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_2)
        .withDataplaneGroupId(MockData.DATAPLANE_GROUP_ID_ORG_2)

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)
    workspaceService.writeStandardWorkspaceNoSecrets(otherWorkspace)

    val workspaces =
      workspacePersistence.listWorkspacesByOrganizationIdPaginated(
        ResourcesByOrganizationQueryPaginated(MockData.ORGANIZATION_ID_1, false, 10, 0),
        Optional.empty<String>(),
      )
    assertReturnsWorkspace(createBaseStandardWorkspace().withTombstone(false))

    org.junit.jupiter.api.Assertions
      .assertEquals(1, workspaces.size)
    assertWorkspaceEquals(workspace, workspaces.get(0))
  }

  @Test
  @Throws(Exception::class)
  fun testListWorkspacesInOrgWithPagination() {
    val workspace: StandardWorkspace =
      createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_1)
        .withName("A workspace")
        .withDataplaneGroupId(MockData.DATAPLANE_GROUP_ID_ORG_1)
    val otherWorkspace: StandardWorkspace =
      createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_1)
        .withName("B workspace")
        .withDataplaneGroupId(MockData.DATAPLANE_GROUP_ID_ORG_1)

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)
    workspaceService.writeStandardWorkspaceNoSecrets(otherWorkspace)

    val workspaces =
      workspacePersistence.listWorkspacesByOrganizationIdPaginated(
        ResourcesByOrganizationQueryPaginated(MockData.ORGANIZATION_ID_1, false, 1, 0),
        Optional.empty<String>(),
      )

    org.junit.jupiter.api.Assertions
      .assertEquals(1, workspaces.size)
    assertWorkspaceEquals(workspace, workspaces.get(0))
  }

  @Test
  @Throws(Exception::class)
  fun testListWorkspacesByInstanceAdminUserPaginated() {
    val workspace1: StandardWorkspace =
      createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withName("A Workspace")
        .withTombstone(false)
    val workspace2: StandardWorkspace =
      createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withName("B Workspace")
        .withTombstone(false)
    val deletedWorkspace: StandardWorkspace =
      createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withName("Deleted Workspace")
        .withTombstone(true)

    workspaceService.writeStandardWorkspaceNoSecrets(workspace1)
    workspaceService.writeStandardWorkspaceNoSecrets(workspace2)
    workspaceService.writeStandardWorkspaceNoSecrets(deletedWorkspace)

    var workspaces =
      workspacePersistence.listWorkspacesByInstanceAdminUserPaginated(
        false,
        2,
        0,
        Optional.empty<String>(),
      )

    org.junit.jupiter.api.Assertions
      .assertEquals(2, workspaces.size)
    assertWorkspaceEquals(workspace1, workspaces.get(0))
    assertWorkspaceEquals(workspace2, workspaces.get(1))

    workspaces =
      workspacePersistence.listWorkspacesByInstanceAdminUserPaginated(
        false,
        1,
        1,
        Optional.empty<String>(),
      )
    org.junit.jupiter.api.Assertions
      .assertEquals(1, workspaces.size)
    assertWorkspaceEquals(workspace2, workspaces.get(0))

    workspaces =
      workspacePersistence.listWorkspacesByInstanceAdminUserPaginated(
        true,
        10,
        0,
        Optional.empty<String>(),
      )
    org.junit.jupiter.api.Assertions
      .assertEquals(3, workspaces.size)
    assertWorkspaceEquals(deletedWorkspace, workspaces.get(2))

    workspaces =
      workspacePersistence.listWorkspacesByInstanceAdminUserPaginated(
        false,
        10,
        0,
        Optional.of<String>("A"),
      )
    org.junit.jupiter.api.Assertions
      .assertEquals(2, workspaces.size)
  }

  @Test
  @Throws(Exception::class)
  fun testListWorkspacesInOrgWithKeyword() {
    val workspace: StandardWorkspace =
      createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_1)
        .withName("workspaceWithKeyword")
        .withDataplaneGroupId(MockData.DATAPLANE_GROUP_ID_ORG_1)
    val otherWorkspace: StandardWorkspace =
      createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_1)
        .withName("workspace")
        .withDataplaneGroupId(MockData.DATAPLANE_GROUP_ID_ORG_1)

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)
    workspaceService.writeStandardWorkspaceNoSecrets(otherWorkspace)

    val workspaces =
      workspacePersistence.listWorkspacesByOrganizationIdPaginated(
        ResourcesByOrganizationQueryPaginated(MockData.ORGANIZATION_ID_1, false, 10, 0),
        Optional.of<String>("keyword"),
      )

    org.junit.jupiter.api.Assertions
      .assertEquals(1, workspaces.size)
    assertWorkspaceEquals(workspace, workspaces.get(0))
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class)
  fun testGetDefaultWorkspaceForOrganization() {
    val expectedWorkspace: StandardWorkspace =
      createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_1)
        .withName("workspaceInOrganization1")
        .withDataplaneGroupId(MockData.DATAPLANE_GROUP_ID_ORG_1)

    workspaceService.writeStandardWorkspaceNoSecrets(expectedWorkspace)

    val tombstonedWorkspace: StandardWorkspace =
      createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_1)
        .withName("tombstonedWorkspace")
        .withTombstone(true)

    workspaceService.writeStandardWorkspaceNoSecrets(tombstonedWorkspace)

    val laterWorkspace: StandardWorkspace =
      createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_1)
        .withName("laterWorkspace")

    workspaceService.writeStandardWorkspaceNoSecrets(laterWorkspace)

    val actualWorkspace = workspacePersistence.getDefaultWorkspaceForOrganization(MockData.ORGANIZATION_ID_1)

    assertWorkspaceEquals(expectedWorkspace, actualWorkspace)
  }

  @ParameterizedTest
  @CsvSource(
    "true",
    "false",
  )
  @Throws(JsonValidationException::class, IOException::class)
  fun getInitialSetupComplete(initialSetupComplete: Boolean) {
    val workspace: StandardWorkspace =
      createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_1)
        .withName("workspaceInOrganization1")
        .withInitialSetupComplete(initialSetupComplete)

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    val actualInitialSetupComplete = workspacePersistence.getInitialSetupComplete()

    org.junit.jupiter.api.Assertions
      .assertEquals(initialSetupComplete, actualInitialSetupComplete)
  }

  @Test
  @Throws(Exception::class)
  fun testListWorkspacesByUserIdWithKeywordWithPagination() {
    val workspaceId1 = UUID.randomUUID()
    val workspaceId2 = UUID.randomUUID()
    val workspaceId3 = UUID.randomUUID()
    val workspaceId4 = UUID.randomUUID()

    // create a user
    val userId = UUID.randomUUID()
    userPersistence.writeAuthenticatedUser(
      AuthenticatedUser()
        .withUserId(userId)
        .withName("user")
        .withAuthUserId("auth_id")
        .withEmail("email")
        .withAuthProvider(AuthProvider.AIRBYTE),
    )

    // create a workspace in org_1, name contains search "keyword"
    val workspace1: StandardWorkspace =
      createBaseStandardWorkspace()
        .withWorkspaceId(workspaceId1)
        .withOrganizationId(MockData.ORGANIZATION_ID_1)
        .withName("workspace_with_keyword_1")
        .withDataplaneGroupId(MockData.DATAPLANE_GROUP_ID_ORG_1)
    workspaceService.writeStandardWorkspaceNoSecrets(workspace1)

    // create a workspace in org_2, name contains search "Keyword"
    val workspace2: StandardWorkspace =
      createBaseStandardWorkspace()
        .withWorkspaceId(workspaceId2)
        .withOrganizationId(MockData.ORGANIZATION_ID_2)
        .withName("workspace_with_Keyword_2")
        .withDataplaneGroupId(MockData.DATAPLANE_GROUP_ID_ORG_2)
    workspaceService.writeStandardWorkspaceNoSecrets(workspace2)

    // create another workspace in org_2, name does NOT contain search "keyword"
    val workspace3: StandardWorkspace =
      createBaseStandardWorkspace()
        .withWorkspaceId(workspaceId3)
        .withOrganizationId(MockData.ORGANIZATION_ID_2)
        .withName("workspace_3")
        .withDataplaneGroupId(MockData.DATAPLANE_GROUP_ID_ORG_2)
    workspaceService.writeStandardWorkspaceNoSecrets(workspace3)

    // create a workspace in org_3, name contains search "keyword"
    val workspace4: StandardWorkspace =
      createBaseStandardWorkspace()
        .withWorkspaceId(workspaceId4)
        .withOrganizationId(MockData.ORGANIZATION_ID_3)
        .withName("workspace_4_with_keyword")
        .withDataplaneGroupId(MockData.DATAPLANE_GROUP_ID_ORG_3)
    workspaceService.writeStandardWorkspaceNoSecrets(workspace4)

    // create a workspace permission for workspace 1
    writePermission(
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withWorkspaceId(workspaceId1)
        .withUserId(userId)
        .withPermissionType(Permission.PermissionType.WORKSPACE_OWNER),
    )

    // create an org permission that should grant access to workspace 2 and 3
    writePermission(
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_2)
        .withUserId(userId)
        .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN),
    )

    // workspace 4 does not have any permissions associated with it
    val workspaces =
      workspacePersistence.listWorkspacesByUserIdPaginated(
        ResourcesByUserQueryPaginated(userId, false, 10, 0),
        Optional.of<String>("keyWord"),
      )

    // workspace 3 excluded because of lacking keyword, and workspace 4 excluded because no permission
    // despite keyword
    val expectedWorkspaces = Set.of<StandardWorkspace?>(workspace1, workspace2)
    val actualWorkspaces: MutableSet<StandardWorkspace?> = HashSet<StandardWorkspace?>(workspaces)

    assertWorkspacesEqual(expectedWorkspaces, actualWorkspaces)
  }

  @Test
  @Throws(Exception::class)
  fun testListWorkspacesByUserIdWithoutKeywordWithoutPagination() {
    val workspace1Id = UUID.randomUUID()
    val workspace2Id = UUID.randomUUID()
    val workspace3Id = UUID.randomUUID()

    // create a user
    val userId = UUID.randomUUID()
    userPersistence.writeAuthenticatedUser(
      AuthenticatedUser()
        .withUserId(userId)
        .withName("user")
        .withAuthUserId("auth_id")
        .withEmail("email")
        .withAuthProvider(AuthProvider.AIRBYTE),
    )

    // create a workspace in org_1
    val workspace1: StandardWorkspace =
      createBaseStandardWorkspace()
        .withWorkspaceId(workspace1Id)
        .withOrganizationId(MockData.ORGANIZATION_ID_1)
        .withName("workspace1")
        .withDataplaneGroupId(MockData.DATAPLANE_GROUP_ID_ORG_1)
    workspaceService.writeStandardWorkspaceNoSecrets(workspace1)

    // create a workspace in org_2
    val workspace2: StandardWorkspace =
      createBaseStandardWorkspace()
        .withWorkspaceId(workspace2Id)
        .withOrganizationId(MockData.ORGANIZATION_ID_2)
        .withName("workspace2")
        .withDataplaneGroupId(MockData.DATAPLANE_GROUP_ID_ORG_2)
    workspaceService.writeStandardWorkspaceNoSecrets(workspace2)

    // create a workspace in org_3
    val workspace3: StandardWorkspace =
      createBaseStandardWorkspace()
        .withWorkspaceId(workspace3Id)
        .withOrganizationId(MockData.ORGANIZATION_ID_3)
        .withName("workspace3")
        .withDataplaneGroupId(MockData.DATAPLANE_GROUP_ID_ORG_3)
    workspaceService.writeStandardWorkspaceNoSecrets(workspace3)

    // create a workspace-level permission for workspace 1
    writePermission(
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withWorkspaceId(workspace1Id)
        .withUserId(userId)
        .withPermissionType(Permission.PermissionType.WORKSPACE_READER),
    )

    // create an org-level permission that should grant access to workspace 2
    writePermission(
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_2)
        .withUserId(userId)
        .withPermissionType(Permission.PermissionType.ORGANIZATION_READER),
    )

    // create an org-member permission that should NOT grant access to workspace 3, because
    // org-member is too low of a permission to grant read-access to workspaces in the org.
    writePermission(
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_3)
        .withUserId(userId)
        .withPermissionType(Permission.PermissionType.ORGANIZATION_MEMBER),
    )

    val expectedWorkspaces = Set.of<StandardWorkspace?>(workspace1, workspace2)

    val actualWorkspaces: MutableSet<StandardWorkspace?> =
      HashSet<StandardWorkspace?>(workspacePersistence.listActiveWorkspacesByUserId(userId, Optional.empty<String>()))

    assertWorkspacesEqual(expectedWorkspaces, actualWorkspaces)
  }

  companion object {
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val SOURCE_DEFINITION_ID: UUID = UUID.randomUUID()
    private val SOURCE_ID: UUID = UUID.randomUUID()
    private val DESTINATION_DEFINITION_ID: UUID = UUID.randomUUID()
    private val DESTINATION_ID: UUID = UUID.randomUUID()
    private val CONFIG = jsonNode<ImmutableMap<String?, String?>?>(ImmutableMap.of<String?, String?>("key-a", "value-a"))

    private fun createBaseStandardWorkspace(): StandardWorkspace =
      StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withName("workspace-a")
        .withSlug("workspace-a-slug")
        .withInitialSetupComplete(false)
        .withTombstone(false)
        .withOrganizationId(MockData.DEFAULT_ORGANIZATION_ID)
        .withDataplaneGroupId(MockData.DATAPLANE_GROUP_ID_DEFAULT)

    private fun createBaseSource(): SourceConnection? =
      SourceConnection()
        .withSourceId(SOURCE_ID)
        .withSourceDefinitionId(SOURCE_DEFINITION_ID)
        .withName("source-a")
        .withTombstone(false)
        .withConfiguration(CONFIG)
        .withWorkspaceId(WORKSPACE_ID)

    private fun createBaseDestination(): DestinationConnection? =
      DestinationConnection()
        .withDestinationId(DESTINATION_ID)
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID)
        .withName("destination-a")
        .withTombstone(false)
        .withConfiguration(CONFIG)
        .withWorkspaceId(WORKSPACE_ID)

    private fun createSourceDefinition(): StandardSourceDefinition? =
      StandardSourceDefinition()
        .withSourceDefinitionId(SOURCE_DEFINITION_ID)
        .withTombstone(false)
        .withName("source-definition-a")

    private fun createActorDefinitionVersion(
      actorDefinitionId: UUID?,
      releaseStage: ReleaseStage?,
    ): ActorDefinitionVersion? =
      ActorDefinitionVersion()
        .withActorDefinitionId(actorDefinitionId)
        .withDockerRepository("dockerhub")
        .withSupportLevel(SupportLevel.COMMUNITY)
        .withInternalSupportLevel(100L)
        .withDockerImageTag("0.0.1")
        .withReleaseStage(releaseStage)

    private fun createDestinationDefinition(): StandardDestinationDefinition? =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID)
        .withTombstone(false)
        .withName("destination-definition-a")
  }
}
