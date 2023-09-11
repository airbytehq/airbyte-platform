/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.lang.MoreBooleans;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.Geography;
import io.airbyte.config.Organization;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.SupportLevel;
import io.airbyte.config.User;
import io.airbyte.config.User.AuthProvider;
import io.airbyte.config.persistence.ConfigRepository.ResourcesByOrganizationQueryPaginated;
import io.airbyte.config.persistence.ConfigRepository.ResourcesByUserQueryPaginated;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@SuppressWarnings({"PMD.LongVariable", "PMD.AvoidInstantiatingObjectsInLoops"})
class WorkspacePersistenceTest extends BaseConfigDatabaseTest {

  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID SOURCE_DEFINITION_ID = UUID.randomUUID();
  private static final UUID SOURCE_ID = UUID.randomUUID();
  private static final UUID DESTINATION_DEFINITION_ID = UUID.randomUUID();
  private static final UUID DESTINATION_ID = UUID.randomUUID();
  private static final JsonNode CONFIG = Jsons.jsonNode(ImmutableMap.of("key-a", "value-a"));

  private ConfigRepository configRepository;
  private WorkspacePersistence workspacePersistence;
  private PermissionPersistence permissionPersistence;
  private UserPersistence userPersistence;

  @BeforeEach
  void setup() throws Exception {
    configRepository = spy(new ConfigRepository(database, null, MockData.MAX_SECONDS_BETWEEN_MESSAGE_SUPPLIER));
    workspacePersistence = new WorkspacePersistence(database);
    permissionPersistence = new PermissionPersistence(database);
    userPersistence = new UserPersistence(database);
    final OrganizationPersistence organizationPersistence = new OrganizationPersistence(database);

    truncateAllTables();
    for (final Organization organization : MockData.organizations()) {
      organizationPersistence.createOrganization(organization);
    }
  }

  @Test
  void testGetWorkspace() throws ConfigNotFoundException, IOException, JsonValidationException {
    configRepository.writeStandardWorkspaceNoSecrets(createBaseStandardWorkspace().withWorkspaceId(UUID.randomUUID()));
    assertReturnsWorkspace(createBaseStandardWorkspace());
  }

  @Test
  void testWorkspaceWithNullTombstone() throws ConfigNotFoundException, IOException, JsonValidationException {
    assertReturnsWorkspace(createBaseStandardWorkspace());
  }

  @Test
  void testWorkspaceWithFalseTombstone() throws ConfigNotFoundException, IOException, JsonValidationException {
    assertReturnsWorkspace(createBaseStandardWorkspace().withTombstone(false));
  }

  @Test
  void testWorkspaceWithTrueTombstone() throws ConfigNotFoundException, IOException, JsonValidationException {
    assertReturnsWorkspace(createBaseStandardWorkspace().withTombstone(true));
  }

  private static StandardWorkspace createBaseStandardWorkspace() {
    return new StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withName("workspace-a")
        .withSlug("workspace-a-slug")
        .withInitialSetupComplete(false)
        .withTombstone(false)
        .withDefaultGeography(Geography.AUTO);
  }

  private static SourceConnection createBaseSource() {
    return new SourceConnection()
        .withSourceId(SOURCE_ID)
        .withSourceDefinitionId(SOURCE_DEFINITION_ID)
        .withName("source-a")
        .withTombstone(false)
        .withConfiguration(CONFIG)
        .withWorkspaceId(WORKSPACE_ID);
  }

  private static DestinationConnection createBaseDestination() {
    return new DestinationConnection()
        .withDestinationId(DESTINATION_ID)
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID)
        .withName("destination-a")
        .withTombstone(false)
        .withConfiguration(CONFIG)
        .withWorkspaceId(WORKSPACE_ID);
  }

  private static StandardSourceDefinition createSourceDefinition() {
    return new StandardSourceDefinition()
        .withSourceDefinitionId(SOURCE_DEFINITION_ID)
        .withTombstone(false)
        .withName("source-definition-a");
  }

  private static ActorDefinitionVersion createActorDefinitionVersion(final UUID actorDefinitionId,
                                                                     final io.airbyte.config.ReleaseStage releaseStage) {
    return new ActorDefinitionVersion()
        .withActorDefinitionId(actorDefinitionId)
        .withDockerRepository("dockerhub")
        .withSupportLevel(SupportLevel.COMMUNITY)
        .withDockerImageTag("0.0.1")
        .withReleaseStage(releaseStage);
  }

  private static StandardDestinationDefinition createDestinationDefinition() {
    return new StandardDestinationDefinition()
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID)
        .withTombstone(false)
        .withName("destination-definition-a");
  }

  void assertReturnsWorkspace(final StandardWorkspace workspace) throws ConfigNotFoundException, IOException, JsonValidationException {
    configRepository.writeStandardWorkspaceNoSecrets(workspace);

    final StandardWorkspace expectedWorkspace = Jsons.clone(workspace);
    /*
     * tombstone defaults to false in the db, so if the passed in workspace does not have it set, we
     * expected the workspace returned from the db to have it set to false.
     */
    if (workspace.getTombstone() == null) {
      expectedWorkspace.withTombstone(false);
    }

    assertEquals(workspace, configRepository.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testWorkspaceByConnectionId(final boolean isTombstone) throws ConfigNotFoundException, IOException, JsonValidationException {
    final UUID connectionId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final StandardSync mSync = new StandardSync()
        .withSourceId(sourceId);
    final SourceConnection mSourceConnection = new SourceConnection()
        .withWorkspaceId(WORKSPACE_ID);
    final StandardWorkspace mWorkflow = new StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID);

    doReturn(mSync)
        .when(configRepository)
        .getStandardSync(connectionId);
    doReturn(mSourceConnection)
        .when(configRepository)
        .getSourceConnection(sourceId);
    doReturn(mWorkflow)
        .when(configRepository)
        .getStandardWorkspaceNoSecrets(WORKSPACE_ID, isTombstone);

    configRepository.getStandardWorkspaceFromConnection(connectionId, isTombstone);

    verify(configRepository).getStandardWorkspaceNoSecrets(WORKSPACE_ID, isTombstone);
  }

  @Test
  void testUpdateFeedback() throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardWorkspace workspace = createBaseStandardWorkspace();

    configRepository.writeStandardWorkspaceNoSecrets(workspace);

    assertFalse(MoreBooleans.isTruthy(configRepository.getStandardWorkspaceNoSecrets(workspace.getWorkspaceId(), false).getFeedbackDone()));
    configRepository.setFeedback(workspace.getWorkspaceId());
    assertTrue(configRepository.getStandardWorkspaceNoSecrets(workspace.getWorkspaceId(), false).getFeedbackDone());
  }

  @ParameterizedTest
  @CsvSource({
    "GENERALLY_AVAILABLE, GENERALLY_AVAILABLE, false",
    "ALPHA, GENERALLY_AVAILABLE, true",
    "GENERALLY_AVAILABLE, BETA, true",
    "CUSTOM, CUSTOM, false",
  })
  void testWorkspaceHasAlphaOrBetaConnector(final ReleaseStage sourceReleaseStage,
                                            final ReleaseStage destinationReleaseStage,
                                            final boolean expectation)
      throws JsonValidationException, IOException {
    final StandardWorkspace workspace = createBaseStandardWorkspace();
    configRepository.writeStandardWorkspaceNoSecrets(workspace);

    configRepository.writeConnectorMetadata(
        createSourceDefinition(),
        createActorDefinitionVersion(SOURCE_DEFINITION_ID, sourceReleaseStage));
    configRepository.writeConnectorMetadata(
        createDestinationDefinition(),
        createActorDefinitionVersion(DESTINATION_DEFINITION_ID, destinationReleaseStage));

    configRepository.writeSourceConnectionNoSecrets(createBaseSource());
    configRepository.writeDestinationConnectionNoSecrets(createBaseDestination());

    assertEquals(expectation, configRepository.getWorkspaceHasAlphaOrBetaConnector(WORKSPACE_ID));
  }

  @Test
  void testListWorkspacesInOrgNoKeyword() throws Exception {

    final StandardWorkspace workspace = createBaseStandardWorkspace().withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_1);
    final StandardWorkspace otherWorkspace = createBaseStandardWorkspace().withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_2);

    configRepository.writeStandardWorkspaceNoSecrets(workspace);
    configRepository.writeStandardWorkspaceNoSecrets(otherWorkspace);

    final List<StandardWorkspace> workspaces = workspacePersistence.listWorkspacesByOrganizationIdPaginated(
        new ResourcesByOrganizationQueryPaginated(MockData.ORGANIZATION_ID_1, false, 10, 0), Optional.empty());
    assertReturnsWorkspace(createBaseStandardWorkspace().withTombstone(false));

    assertEquals(1, workspaces.size());
    assertEquals(workspace, workspaces.get(0));
  }

  @Test
  void testListWorkspacesInOrgWithPagination() throws Exception {
    final StandardWorkspace workspace = createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_1)
        .withName("A workspace");
    final StandardWorkspace otherWorkspace = createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID()).withOrganizationId(MockData.ORGANIZATION_ID_1).withName("B workspace");

    configRepository.writeStandardWorkspaceNoSecrets(workspace);
    configRepository.writeStandardWorkspaceNoSecrets(otherWorkspace);

    final List<StandardWorkspace> workspaces = workspacePersistence.listWorkspacesByOrganizationIdPaginated(
        new ResourcesByOrganizationQueryPaginated(MockData.ORGANIZATION_ID_1, false, 1, 0), Optional.empty());

    assertEquals(1, workspaces.size());
    assertEquals(workspace, workspaces.get(0));
  }

  @Test
  void testListWorkspacesInOrgWithKeyword() throws Exception {
    final StandardWorkspace workspace = createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_1)
        .withName("workspaceWithKeyword");
    final StandardWorkspace otherWorkspace = createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID()).withOrganizationId(MockData.ORGANIZATION_ID_1).withName("workspace");

    configRepository.writeStandardWorkspaceNoSecrets(workspace);
    configRepository.writeStandardWorkspaceNoSecrets(otherWorkspace);

    final List<StandardWorkspace> workspaces = workspacePersistence.listWorkspacesByOrganizationIdPaginated(
        new ResourcesByOrganizationQueryPaginated(MockData.ORGANIZATION_ID_1, false, 10, 0), Optional.of("keyword"));

    assertEquals(1, workspaces.size());
    assertEquals(workspace, workspaces.get(0));
  }

  @Test
  void testGetDefaultWorkspaceForOrganization() throws JsonValidationException, IOException {
    final StandardWorkspace expectedWorkspace = createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_1)
        .withName("workspaceInOrganization1");

    configRepository.writeStandardWorkspaceNoSecrets(expectedWorkspace);

    final StandardWorkspace tombstonedWorkspace = createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_1)
        .withName("tombstonedWorkspace")
        .withTombstone(true);

    configRepository.writeStandardWorkspaceNoSecrets(tombstonedWorkspace);

    final StandardWorkspace laterWorkspace = createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_1)
        .withName("laterWorkspace");

    configRepository.writeStandardWorkspaceNoSecrets(laterWorkspace);

    final StandardWorkspace actualWorkspace = workspacePersistence.getDefaultWorkspaceForOrganization(MockData.ORGANIZATION_ID_1);

    assertEquals(expectedWorkspace, actualWorkspace);
  }

  @Test
  void testListWorkspacesByUserIdWithKeywordWithPagination() throws Exception {
    final UUID workspaceId = UUID.randomUUID();
    // create a user
    final UUID userId = UUID.randomUUID();
    userPersistence.writeUser(new User()
        .withUserId(userId)
        .withName("user")
        .withAuthUserId("auth_id")
        .withEmail("email")
        .withAuthProvider(AuthProvider.AIRBYTE));
    // create a workspace in org_1, name contains search "keyword"
    final StandardWorkspace orgWorkspace = createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_1)
        .withName("workspace_with_keyword_1");
    configRepository.writeStandardWorkspaceNoSecrets(orgWorkspace);
    // create a workspace in org_2, name contains search "Keyword"
    final StandardWorkspace userWorkspace = createBaseStandardWorkspace()
        .withWorkspaceId(workspaceId).withOrganizationId(MockData.ORGANIZATION_ID_2)
        .withName("workspace_with_Keyword_2");
    configRepository.writeStandardWorkspaceNoSecrets(userWorkspace);
    // create a workspace permission
    permissionPersistence.writePermission(new Permission()
        .withPermissionId(UUID.randomUUID())
        .withWorkspaceId(workspaceId)
        .withUserId(userId)
        .withPermissionType(PermissionType.WORKSPACE_READER));
    // create an org permission
    permissionPersistence.writePermission(new Permission()
        .withPermissionId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_1)
        .withUserId(userId)
        .withPermissionType(PermissionType.ORGANIZATION_ADMIN));

    final List<StandardWorkspace> workspaces = workspacePersistence.listWorkspacesByUserIdPaginated(
        new ResourcesByUserQueryPaginated(userId, false, 10, 0), Optional.of("keyWord"));

    assertEquals(2, workspaces.size());
  }

  @Test
  void testListWorkspacesByUserIdWithoutKeywordWithoutPagination() throws Exception {
    final UUID workspaceId = UUID.randomUUID();
    // create a user
    final UUID userId = UUID.randomUUID();
    userPersistence.writeUser(new User()
        .withUserId(userId)
        .withName("user")
        .withAuthUserId("auth_id")
        .withEmail("email")
        .withAuthProvider(AuthProvider.AIRBYTE));
    // create a workspace in org_1
    final StandardWorkspace orgWorkspace = createBaseStandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_1)
        .withName("workspace1");
    configRepository.writeStandardWorkspaceNoSecrets(orgWorkspace);
    // create a workspace in org_2
    final StandardWorkspace userWorkspace = createBaseStandardWorkspace()
        .withWorkspaceId(workspaceId).withOrganizationId(MockData.ORGANIZATION_ID_2)
        .withName("workspace2");
    configRepository.writeStandardWorkspaceNoSecrets(userWorkspace);
    // create a workspace permission
    permissionPersistence.writePermission(new Permission()
        .withPermissionId(UUID.randomUUID())
        .withWorkspaceId(workspaceId)
        .withUserId(userId)
        .withPermissionType(PermissionType.WORKSPACE_READER));
    // create an org permission
    permissionPersistence.writePermission(new Permission()
        .withPermissionId(UUID.randomUUID())
        .withOrganizationId(MockData.ORGANIZATION_ID_1)
        .withUserId(userId)
        .withPermissionType(PermissionType.ORGANIZATION_ADMIN));

    final List<StandardWorkspace> workspaces = workspacePersistence.listWorkspacesByUserId(
        userId, false, Optional.empty());

    assertEquals(2, workspaces.size());
  }

}
