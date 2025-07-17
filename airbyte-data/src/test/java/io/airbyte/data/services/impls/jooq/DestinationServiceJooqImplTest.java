/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.JobStatus;
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSync.Status;
import io.airbyte.config.SupportLevel;
import io.airbyte.config.helpers.CatalogHelpers;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.data.ConfigNotFoundException;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.shared.ActorServicePaginationHelper;
import io.airbyte.data.services.shared.DestinationConnectionWithCount;
import io.airbyte.data.services.shared.SortKey;
import io.airbyte.data.services.shared.WorkspaceResourceCursorPagination;
import io.airbyte.featureflag.TestClient;
import io.airbyte.metrics.MetricClient;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import io.airbyte.protocol.models.v0.Field;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class DestinationServiceJooqImplTest extends BaseConfigDatabaseTest {

  private static final CatalogHelpers catalogHelpers = new CatalogHelpers(new FieldGenerator());

  private final DestinationServiceJooqImpl destinationServiceJooqImpl;
  private final SourceServiceJooqImpl sourceServiceJooqImpl;
  private final ConnectionServiceJooqImpl connectionServiceJooqImpl;

  public DestinationServiceJooqImplTest() {
    final TestClient featureFlagClient = mock(TestClient.class);
    final MetricClient metricClient = mock(MetricClient.class);
    final ConnectionService connectionService = mock(ConnectionService.class);
    final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater = mock(ActorDefinitionVersionUpdater.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);
    final ActorServicePaginationHelper actorPaginationServiceHelper = new ActorServicePaginationHelper(database);

    this.destinationServiceJooqImpl =
        new DestinationServiceJooqImpl(database, featureFlagClient, connectionService, actorDefinitionVersionUpdater, metricClient,
            actorPaginationServiceHelper);
    this.sourceServiceJooqImpl =
        new SourceServiceJooqImpl(database, featureFlagClient, secretPersistenceConfigService, connectionService, actorDefinitionVersionUpdater,
            metricClient, actorPaginationServiceHelper);
    this.connectionServiceJooqImpl = new ConnectionServiceJooqImpl(database);
  }

  @Test
  void testListWorkspaceDestinationConnectionsWithCounts_NoConnections()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final UUID workspaceId = helper.getWorkspace().getWorkspaceId();

    // Test with no connections - should return empty list
    final List<DestinationConnectionWithCount> result =
        destinationServiceJooqImpl.listWorkspaceDestinationConnectionsWithCounts(workspaceId, WorkspaceResourceCursorPagination.fromValues(
            SortKey.DESTINATION_NAME, null, null, null, null, null, null, null, null, null, null));

    assertNotNull(result);
    assertEquals(1, result.size()); // Should have the destination from setup, but with 0 connections
    assertEquals(0, result.get(0).connectionCount);
    assertEquals(helper.getDestination().getDestinationId(), result.get(0).destination.getDestinationId());
    assertNull(result.get(0).lastSync, "Should have no last sync when no connections exist");
  }

  @Test
  void testListWorkspaceDestinationConnectionsWithCounts_WithActiveConnections()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final DestinationConnection destination = helper.getDestination();
    final SourceConnection source = helper.getSource();
    final UUID workspaceId = helper.getWorkspace().getWorkspaceId();

    final StandardSync connection1 = createConnectionWithName(source, destination, Status.ACTIVE, "connection-1");
    final StandardSync connection2 = createConnectionWithName(source, destination, Status.ACTIVE, "connection-2");
    final StandardSync connection3 = createConnectionWithName(source, destination, Status.ACTIVE, "connection-3");

    final OffsetDateTime oldestJobTime = OffsetDateTime.now().minusHours(3);
    final OffsetDateTime middleJobTime = OffsetDateTime.now().minusHours(2);
    final OffsetDateTime newestJobTime = OffsetDateTime.now().minusHours(1);
    createJobRecord(connection1.getConnectionId(), newestJobTime, JobStatus.SUCCEEDED);
    createJobRecord(connection1.getConnectionId(), middleJobTime, JobStatus.RUNNING);
    createJobRecord(connection1.getConnectionId(), oldestJobTime, JobStatus.CANCELLED);
    createJobRecord(connection2.getConnectionId(), newestJobTime, JobStatus.SUCCEEDED);
    createJobRecord(connection3.getConnectionId(), newestJobTime, JobStatus.SUCCEEDED);

    final List<DestinationConnectionWithCount> result =
        destinationServiceJooqImpl.listWorkspaceDestinationConnectionsWithCounts(workspaceId, WorkspaceResourceCursorPagination.fromValues(
            SortKey.DESTINATION_NAME, null, null, null, null, null, null, null, null, null, null));

    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals(3, result.get(0).connectionCount);
    assertEquals(destination.getDestinationId(), result.get(0).destination.getDestinationId());
    assertTrue(Math.abs(result.get(0).lastSync.toEpochSecond() - newestJobTime.toEpochSecond()) < 2,
        "Last sync should be the most recent job time");

    // All 3 connections have SUCCEEDED as their most recent job status
    assertEquals(3, result.get(0).connectionJobStatuses.get(JobStatus.SUCCEEDED), "Should have 3 SUCCEEDED jobs");
    assertEquals(0, result.get(0).connectionJobStatuses.get(JobStatus.FAILED), "Should have 0 FAILED jobs");
    assertEquals(0, result.get(0).connectionJobStatuses.get(JobStatus.PENDING), "Should have 0 PENDING jobs");
    assertEquals(0, result.get(0).connectionJobStatuses.get(JobStatus.INCOMPLETE), "Should have 0 INCOMPLETE jobs");
    assertEquals(0, result.get(0).connectionJobStatuses.get(JobStatus.CANCELLED), "Should have 0 CANCELLED jobs");
    assertEquals(0, result.get(0).connectionJobStatuses.get(JobStatus.RUNNING), "Should have 0 RUNNING jobs");
  }

  @Test
  void testListWorkspaceDestinationConnectionsWithCounts_ExcludesDeprecatedConnections()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final DestinationConnection destination = helper.getDestination();
    final SourceConnection source = helper.getSource();
    final UUID workspaceId = helper.getWorkspace().getWorkspaceId();

    // Create 2 active connections and 1 deprecated connection
    createConnection(source, destination, Status.ACTIVE);
    createConnection(source, destination, Status.ACTIVE);
    createConnection(source, destination, Status.DEPRECATED);

    final List<DestinationConnectionWithCount> result =
        destinationServiceJooqImpl.listWorkspaceDestinationConnectionsWithCounts(workspaceId, WorkspaceResourceCursorPagination.fromValues(
            SortKey.DESTINATION_NAME, null, null, null, null, null, null, null, null, null, null));

    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals(2, result.get(0).connectionCount); // Should only count active connections, not deprecated
    assertEquals(destination.getDestinationId(), result.get(0).destination.getDestinationId());
  }

  @Test
  void testListWorkspaceDestinationConnectionsWithCounts_MultipleDestinations()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final SourceConnection source = helper.getSource();
    final DestinationConnection destination1 = helper.getDestination();
    final UUID workspaceId = helper.getWorkspace().getWorkspaceId();

    // Create a second destination in the same workspace
    final DestinationConnection destination2 = createAdditionalDestination(workspaceId, helper);

    // Create connections: 2 to destination1, 1 to destination2
    createConnection(source, destination1, Status.ACTIVE);
    createConnection(source, destination1, Status.ACTIVE);
    createConnection(source, destination2, Status.ACTIVE);

    final List<DestinationConnectionWithCount> result =
        destinationServiceJooqImpl.listWorkspaceDestinationConnectionsWithCounts(workspaceId, WorkspaceResourceCursorPagination.fromValues(
            SortKey.DESTINATION_NAME, null, null, null, null, null, null, null, null, null, null));

    assertNotNull(result);
    assertEquals(2, result.size());

    boolean found1 = false;
    boolean found2 = false;
    for (final DestinationConnectionWithCount destWithCount : result) {
      if (destWithCount.destination.getDestinationId().equals(destination1.getDestinationId())) {
        assertEquals(2, destWithCount.connectionCount);
        found1 = true;
      } else if (destWithCount.destination.getDestinationId().equals(destination2.getDestinationId())) {
        assertEquals(1, destWithCount.connectionCount);
        found2 = true;
      }
    }
    assertTrue(found1, "Destination 1 should be in results");
    assertTrue(found2, "Destination 2 should be in results");
  }

  @Test
  void testListWorkspaceDestinationConnectionsWithCounts_DifferentWorkspaces()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final DestinationConnection destination = helper.getDestination();
    final SourceConnection source = helper.getSource();

    // Create connections in the workspace
    createConnection(source, destination, Status.ACTIVE);

    // Query for a different workspace (should return empty)
    final UUID differentWorkspaceId = UUID.randomUUID();
    final List<DestinationConnectionWithCount> result =
        destinationServiceJooqImpl.listWorkspaceDestinationConnectionsWithCounts(differentWorkspaceId, WorkspaceResourceCursorPagination.fromValues(
            SortKey.DESTINATION_NAME, null, null, null, null, null, null, null, null, null, null));

    assertNotNull(result);
    assertEquals(0, result.size()); // No destinations in the different workspace
  }

  @Test
  void testListWorkspaceDestinationConnectionsWithCounts_WithMixedConnectionStatuses()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final DestinationConnection destination1 = helper.getDestination();
    final DestinationConnection destination2 = createAdditionalDestination(helper.getWorkspace().getWorkspaceId(), helper);
    final SourceConnection source = helper.getSource();
    final UUID workspaceId = helper.getWorkspace().getWorkspaceId();

    // Create connections with different statuses and names
    final StandardSync activeConnection1 = createConnectionWithName(source, destination1, Status.ACTIVE, "active-dest1-connection");
    final StandardSync inactiveConnection = createConnectionWithName(source, destination1, Status.INACTIVE, "inactive-dest-connection");
    final StandardSync deprecatedConnection = createConnectionWithName(source, destination1, Status.DEPRECATED, "deprecated-connection");
    final StandardSync activeConnection2 = createConnectionWithName(source, destination2, Status.ACTIVE, "active-dest2-connection");

    // Create job records with more status variety including FAILED
    final OffsetDateTime jobTimeActive1 = OffsetDateTime.now().minusHours(3);
    final OffsetDateTime jobTimeActive2 = OffsetDateTime.now().minusHours(4);
    final OffsetDateTime jobTimeInactive1 = OffsetDateTime.now().minusHours(1);
    final OffsetDateTime jobTimeDeprecated1 = OffsetDateTime.now().minusHours(0);

    createJobRecord(activeConnection1.getConnectionId(), jobTimeActive1, JobStatus.SUCCEEDED);
    createJobRecord(activeConnection1.getConnectionId(), jobTimeActive2, JobStatus.SUCCEEDED);
    createJobRecord(inactiveConnection.getConnectionId(), jobTimeInactive1, JobStatus.FAILED);
    createJobRecord(deprecatedConnection.getConnectionId(), jobTimeDeprecated1, JobStatus.SUCCEEDED);
    createJobRecord(activeConnection2.getConnectionId(), jobTimeActive1, JobStatus.RUNNING);

    final List<DestinationConnectionWithCount> result =
        destinationServiceJooqImpl.listWorkspaceDestinationConnectionsWithCounts(workspaceId, WorkspaceResourceCursorPagination.fromValues(
            SortKey.DESTINATION_NAME, null, null, null, null, null, null, null, null, null, null));

    assertNotNull(result);
    assertEquals(2, result.size());

    // Find and verify each destination in the results
    boolean found1 = false;
    boolean found2 = false;
    for (final DestinationConnectionWithCount destinationWithCount : result) {
      if (destinationWithCount.destination.getDestinationId().equals(destination1.getDestinationId())) {
        // destination1 has 2 non-deprecated connections (1 active, 1 inactive)
        assertEquals(2, destinationWithCount.connectionCount);
        assertNotNull(destinationWithCount.lastSync, "Should have last sync when connections with jobs exist");
        // Should be the newer job time (from inactive connection)
        assertEquals(jobTimeInactive1.toEpochSecond(), destinationWithCount.lastSync.toEpochSecond(),
            "Last sync should be the most recent job time");
        // destination1 has 1 SUCCEEDED job (from active connection) and 1 FAILED job (from inactive
        // connection)
        assertEquals(1, destinationWithCount.connectionJobStatuses.get(JobStatus.SUCCEEDED),
            "Should have 1 succeeded job (from active connection)");
        assertEquals(1, destinationWithCount.connectionJobStatuses.get(JobStatus.FAILED),
            "Should have 1 failed job (from inactive connection)");
        found1 = true;
      } else if (destinationWithCount.destination.getDestinationId().equals(destination2.getDestinationId())) {
        // destination2 has 1 non-deprecated connection (1 active)
        assertEquals(1, destinationWithCount.connectionCount);
        assertNotNull(destinationWithCount.lastSync, "Should have last sync when connections with jobs exist");
        // Should have 1 RUNNING job (most recent for the connection)
        assertEquals(1, destinationWithCount.connectionJobStatuses.get(JobStatus.RUNNING),
            "Should have 1 running job");
        found2 = true;
      }
    }
    assertTrue(found1, "Destination 1 should be in results");
    assertTrue(found2, "Destination 2 should be in results");
  }

  @Test
  void testListWorkspaceDestinationConnectionsWithCounts_WithConnectionsButNoJobs()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final DestinationConnection destination = helper.getDestination();
    final SourceConnection source = helper.getSource();
    final UUID workspaceId = helper.getWorkspace().getWorkspaceId();

    // Create connections but no job records
    createConnectionWithName(source, destination, Status.ACTIVE, "connection-without-jobs");
    createConnectionWithName(source, destination, Status.INACTIVE, "another-connection-without-jobs");

    final List<DestinationConnectionWithCount> result =
        destinationServiceJooqImpl.listWorkspaceDestinationConnectionsWithCounts(workspaceId, WorkspaceResourceCursorPagination.fromValues(
            SortKey.DESTINATION_NAME, null, null, null, null, null, null, null, null, null, null));

    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals(2, result.get(0).connectionCount);
    assertEquals(destination.getDestinationId(), result.get(0).destination.getDestinationId());
    assertNull(result.get(0).lastSync, "Should have no last sync when no jobs exist");

  }

  @Test
  void testListWorkspaceDestinationConnectionsWithCounts_MixedConnectionStates()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final SourceConnection source = helper.getSource();
    final DestinationConnection destination = helper.getDestination();
    final UUID workspaceId = helper.getWorkspace().getWorkspaceId();

    createConnection(source, destination, Status.ACTIVE);
    createConnection(source, destination, Status.INACTIVE);
    createConnection(source, destination, Status.DEPRECATED);
    createConnection(source, destination, Status.ACTIVE);

    final List<DestinationConnectionWithCount> result =
        destinationServiceJooqImpl.listWorkspaceDestinationConnectionsWithCounts(workspaceId, WorkspaceResourceCursorPagination.fromValues(
            SortKey.DESTINATION_NAME, null, null, null, null, null, null, null, null, null, null));

    assertNotNull(result);
    assertEquals(1, result.size());
    // Should count all non-deprecated connections (2 active + 1 inactive)
    assertEquals(3, result.get(0).connectionCount);
    assertEquals(destination.getDestinationId(), result.get(0).destination.getDestinationId());
  }

  @Test
  void testListWorkspaceDestinationConnectionsWithCounts_AllJobStatuses()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final DestinationConnection destination = helper.getDestination();
    final SourceConnection source = helper.getSource();
    final UUID workspaceId = helper.getWorkspace().getWorkspaceId();

    // Create 6 connections, each with a different most recent job status
    final StandardSync succeededConnection = createConnectionWithName(source, destination, Status.ACTIVE, "succeeded-connection");
    final StandardSync failedConnection = createConnectionWithName(source, destination, Status.ACTIVE, "failed-connection");
    final StandardSync runningConnection = createConnectionWithName(source, destination, Status.ACTIVE, "running-connection");
    final StandardSync pendingConnection = createConnectionWithName(source, destination, Status.ACTIVE, "pending-connection");
    final StandardSync incompleteConnection = createConnectionWithName(source, destination, Status.ACTIVE, "incomplete-connection");
    final StandardSync cancelledConnection = createConnectionWithName(source, destination, Status.ACTIVE, "cancelled-connection");

    // Create jobs with different statuses - most recent job determines the status counted
    final OffsetDateTime baseTime = OffsetDateTime.now().minusHours(1);

    // Each connection gets multiple jobs, but only the most recent status matters
    createJobRecord(succeededConnection.getConnectionId(), baseTime.minusHours(2), JobStatus.PENDING);
    createJobRecord(succeededConnection.getConnectionId(), baseTime, JobStatus.SUCCEEDED);

    createJobRecord(failedConnection.getConnectionId(), baseTime.minusHours(1), JobStatus.RUNNING);
    createJobRecord(failedConnection.getConnectionId(), baseTime, JobStatus.FAILED);

    createJobRecord(runningConnection.getConnectionId(), baseTime.minusHours(1), JobStatus.PENDING);
    createJobRecord(runningConnection.getConnectionId(), baseTime, JobStatus.RUNNING);

    createJobRecord(pendingConnection.getConnectionId(), baseTime.minusHours(1), JobStatus.FAILED);
    createJobRecord(pendingConnection.getConnectionId(), baseTime, JobStatus.PENDING);

    createJobRecord(incompleteConnection.getConnectionId(), baseTime.minusHours(1), JobStatus.RUNNING);
    createJobRecord(incompleteConnection.getConnectionId(), baseTime, JobStatus.INCOMPLETE);

    createJobRecord(cancelledConnection.getConnectionId(), baseTime.minusHours(1), JobStatus.SUCCEEDED);
    createJobRecord(cancelledConnection.getConnectionId(), baseTime, JobStatus.CANCELLED);

    final List<DestinationConnectionWithCount> result =
        destinationServiceJooqImpl.listWorkspaceDestinationConnectionsWithCounts(workspaceId, WorkspaceResourceCursorPagination.fromValues(
            SortKey.DESTINATION_NAME, null, null, null, null, null, null, null, null, null, null));

    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals(6, result.get(0).connectionCount);
    assertEquals(destination.getDestinationId(), result.get(0).destination.getDestinationId());

    // Verify all job statuses are correctly counted (1 connection per status)
    assertEquals(1, result.get(0).connectionJobStatuses.get(JobStatus.SUCCEEDED), "Should have 1 SUCCEEDED job");
    assertEquals(1, result.get(0).connectionJobStatuses.get(JobStatus.FAILED), "Should have 1 FAILED job");
    assertEquals(1, result.get(0).connectionJobStatuses.get(JobStatus.RUNNING), "Should have 1 RUNNING job");
    assertEquals(1, result.get(0).connectionJobStatuses.get(JobStatus.PENDING), "Should have 1 PENDING job");
    assertEquals(1, result.get(0).connectionJobStatuses.get(JobStatus.INCOMPLETE), "Should have 1 INCOMPLETE job");
    assertEquals(1, result.get(0).connectionJobStatuses.get(JobStatus.CANCELLED), "Should have 1 CANCELLED job");

    // Verify last sync time is the most recent across all connections
    assertNotNull(result.get(0).lastSync, "Should have last sync when connections with jobs exist");
    assertTrue(Math.abs(result.get(0).lastSync.toEpochSecond() - baseTime.toEpochSecond()) < 2,
        "Last sync should be the most recent job time across all connections");
  }

  DestinationConnection createDestination(UUID workspaceId, StandardDestinationDefinition destinationDefinition, Boolean withTombstone)
      throws IOException {
    DestinationConnection destination = new DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withWorkspaceId(workspaceId)
        .withName("source")
        .withDestinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .withTombstone(withTombstone);
    destinationServiceJooqImpl.writeDestinationConnectionNoSecrets(destination);
    return destination;
  }

  @Test
  void testListDestinationDefinitionsForWorkspace_ReturnsUsedDestinations()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final List<StandardDestinationDefinition> result =
        destinationServiceJooqImpl.listDestinationDefinitionsForWorkspace(helper.getWorkspace().getWorkspaceId(), false);

    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals(helper.getDestinationDefinition().getDestinationDefinitionId(),
        result.get(0).getDestinationDefinitionId());
    assertEquals("Test destination def", result.get(0).getName());
  }

  @Test
  void testListDestinationDefinitionsForWorkspace_ExcludesTombstonedActors()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final UUID workspaceId = helper.getWorkspace().getWorkspaceId();

    // Create an additional destination and tombstone it
    final DestinationConnection additionalDestination = createAdditionalDestination(workspaceId, helper);
    final DestinationConnection tombstonedDestination = additionalDestination.withTombstone(true);
    destinationServiceJooqImpl.writeDestinationConnectionNoSecrets(tombstonedDestination);

    // Should only return the non-tombstoned destination
    final List<StandardDestinationDefinition> result =
        destinationServiceJooqImpl.listDestinationDefinitionsForWorkspace(workspaceId, false);

    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals(helper.getDestinationDefinition().getDestinationDefinitionId(),
        result.get(0).getDestinationDefinitionId());
  }

  @Test
  void testListDestinationDefinitionsForWorkspace_IncludesTombstonedActorsWhenRequested()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final UUID workspaceId = helper.getWorkspace().getWorkspaceId();

    // Create an additional destination and tombstone it
    final DestinationConnection additionalDestination = createAdditionalDestination(workspaceId, helper);

    // Tombstone the additional destination
    final DestinationConnection tombstonedDestination = additionalDestination.withTombstone(true);
    destinationServiceJooqImpl.writeDestinationConnectionNoSecrets(tombstonedDestination);

    // Should return the definition when including tombstones
    final List<StandardDestinationDefinition> result =
        destinationServiceJooqImpl.listDestinationDefinitionsForWorkspace(workspaceId, true);

    assertNotNull(result);
    assertEquals(1, result.size());

    // Should be the same definition used by both active and tombstoned destinations
    assertEquals(helper.getDestinationDefinition().getDestinationDefinitionId(),
        result.get(0).getDestinationDefinitionId());
  }

  @Test
  void testListDestinationDefinitionsForWorkspace_EmptyWhenNoDestinationsInWorkspace()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    // Use a different workspace that has no destinations
    final UUID emptyWorkspaceId = UUID.randomUUID();

    final List<StandardDestinationDefinition> result =
        destinationServiceJooqImpl.listDestinationDefinitionsForWorkspace(emptyWorkspaceId, false);

    assertNotNull(result);
    assertEquals(0, result.size());
  }

  @Test
  void testListDestinationDefinitionsForWorkspace_MultipleDestinationsWithSameDefinition()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final UUID workspaceId = helper.getWorkspace().getWorkspaceId();

    // Create additional destinations using the same definition
    createAdditionalDestination(workspaceId, helper);
    createAdditionalDestination(workspaceId, helper);

    // Should return the definition only once, even though multiple destinations use it
    final List<StandardDestinationDefinition> result =
        destinationServiceJooqImpl.listDestinationDefinitionsForWorkspace(workspaceId, false);

    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals(helper.getDestinationDefinition().getDestinationDefinitionId(),
        result.get(0).getDestinationDefinitionId());
  }

  @Test
  void testListDestinationDefinitionsForWorkspace_MultipleDestinationsWithDifferentDefinitions()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final UUID workspaceId = helper.getWorkspace().getWorkspaceId();

    // Create a second destination definition
    final UUID secondDestinationDefinitionId = UUID.randomUUID();
    final StandardDestinationDefinition secondDestinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(secondDestinationDefinitionId)
        .withName("Second destination def")
        .withTombstone(false);
    final ActorDefinitionVersion secondDestinationDefinitionVersion =
        createBaseActorDefVersion(secondDestinationDefinitionId, "0.0.2");
    helper.createActorDefinition(secondDestinationDefinition, secondDestinationDefinitionVersion);

    // Create a destination using the second definition
    final UUID secondDestinationId = UUID.randomUUID();
    final DestinationConnection secondDestination = new DestinationConnection()
        .withDestinationId(secondDestinationId)
        .withDestinationDefinitionId(secondDestinationDefinitionId)
        .withWorkspaceId(workspaceId)
        .withName("test-destination-" + secondDestinationId)
        .withConfiguration(io.airbyte.commons.json.Jsons.emptyObject())
        .withTombstone(false);
    destinationServiceJooqImpl.writeDestinationConnectionNoSecrets(secondDestination);

    // Should return both definitions
    final List<StandardDestinationDefinition> result =
        destinationServiceJooqImpl.listDestinationDefinitionsForWorkspace(workspaceId, false);

    assertNotNull(result);
    assertEquals(2, result.size());

    final List<UUID> definitionIds = result.stream()
        .map(StandardDestinationDefinition::getDestinationDefinitionId)
        .toList();

    assertTrue(definitionIds.contains(helper.getDestinationDefinition().getDestinationDefinitionId()));
    assertTrue(definitionIds.contains(secondDestinationDefinitionId));
  }

  private StandardSync createConnection(final SourceConnection source, final DestinationConnection destination, final Status status)
      throws IOException, JsonValidationException {
    final UUID connectionId = UUID.randomUUID();
    final List<ConfiguredAirbyteStream> streams = List.of(
        catalogHelpers.createConfiguredAirbyteStream("test_stream", "test_namespace", Field.of("field", JsonSchemaType.STRING)));

    final StandardSync connection = new StandardSync()
        .withConnectionId(connectionId)
        .withSourceId(source.getSourceId())
        .withDestinationId(destination.getDestinationId())
        .withName("test-connection-" + connectionId)
        .withCatalog(new ConfiguredAirbyteCatalog().withStreams(streams))
        .withManual(true)
        .withNamespaceDefinition(NamespaceDefinitionType.SOURCE)
        .withBreakingChange(false)
        .withStatus(status)
        .withTags(Collections.emptyList());

    connectionServiceJooqImpl.writeStandardSync(connection);
    return connection;
  }

  private DestinationConnection createAdditionalDestination(final UUID workspaceId, final JooqTestDbSetupHelper helper) throws IOException {
    final UUID destinationId = UUID.randomUUID();
    final DestinationConnection destination = new DestinationConnection()
        .withDestinationId(destinationId)
        .withDestinationDefinitionId(helper.getDestinationDefinition().getDestinationDefinitionId())
        .withWorkspaceId(workspaceId)
        .withName("test-destination-" + destinationId)
        .withConfiguration(io.airbyte.commons.json.Jsons.emptyObject())
        .withTombstone(false);

    destinationServiceJooqImpl.writeDestinationConnectionNoSecrets(destination);
    return destination;
  }

  private StandardSync createConnectionWithName(final SourceConnection source,
                                                final DestinationConnection destination,
                                                final Status status,
                                                final String connectionName)
      throws IOException, JsonValidationException {
    final UUID connectionId = UUID.randomUUID();
    final List<ConfiguredAirbyteStream> streams = List.of(
        catalogHelpers.createConfiguredAirbyteStream("test_stream", "test_namespace", Field.of("field", JsonSchemaType.STRING)));

    final StandardSync connection = new StandardSync()
        .withConnectionId(connectionId)
        .withSourceId(source.getSourceId())
        .withDestinationId(destination.getDestinationId())
        .withName(connectionName)
        .withCatalog(new ConfiguredAirbyteCatalog().withStreams(streams))
        .withManual(true)
        .withNamespaceDefinition(NamespaceDefinitionType.SOURCE)
        .withBreakingChange(false)
        .withStatus(status)
        .withTags(Collections.emptyList());

    connectionServiceJooqImpl.writeStandardSync(connection);
    return connection;
  }

  private void createJobRecord(final UUID connectionId, final OffsetDateTime createdAt, final JobStatus jobStatus) {
    // Insert a job record directly into the database
    // This simulates the job table structure used in the actual query
    database.query(ctx -> {
      ctx.insertInto(table("jobs"))
          .columns(
              field("config_type"),
              field("scope"),
              field("created_at"),
              field("updated_at"),
              field("status"),
              field("config"))
          .values(
              field("cast(? as job_config_type)", "sync"),
              connectionId.toString(),
              createdAt,
              createdAt,
              field("cast(? as job_status)", jobStatus.name().toLowerCase()),
              field("cast(? as jsonb)", "{}"))
          .execute();
      return null;
    });
  }

  private static ActorDefinitionVersion createBaseActorDefVersion(final UUID actorDefId, final String dockerImageTag) {
    final ConnectorSpecification spec = new ConnectorSpecification()
        .withConnectionSpecification(io.airbyte.commons.json.Jsons.jsonNode(Map.of("type", "object")));

    return new ActorDefinitionVersion()
        .withActorDefinitionId(actorDefId)
        .withDockerRepository("airbyte/test")
        .withDockerImageTag(dockerImageTag)
        .withSpec(spec)
        .withSupportLevel(SupportLevel.COMMUNITY)
        .withInternalSupportLevel(200L);
  }

}
