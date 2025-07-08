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

import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.JobStatus;
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSync.Status;
import io.airbyte.config.helpers.CatalogHelpers;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.data.ConfigNotFoundException;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.shared.SourceConnectionWithCount;
import io.airbyte.featureflag.TestClient;
import io.airbyte.metrics.MetricClient;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.v0.Field;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class SourceServiceJooqImplTest extends BaseConfigDatabaseTest {

  private static final CatalogHelpers catalogHelpers = new CatalogHelpers(new FieldGenerator());

  private final SourceServiceJooqImpl sourceServiceJooqImpl;
  private final ConnectionServiceJooqImpl connectionServiceJooqImpl;

  public SourceServiceJooqImplTest() {
    final TestClient featureFlagClient = mock(TestClient.class);
    final MetricClient metricClient = mock(MetricClient.class);
    final ConnectionService connectionService = mock(ConnectionService.class);
    final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater = mock(ActorDefinitionVersionUpdater.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);

    this.sourceServiceJooqImpl = new SourceServiceJooqImpl(database, featureFlagClient, secretPersistenceConfigService, connectionService,
        actorDefinitionVersionUpdater, metricClient);
    this.connectionServiceJooqImpl = new ConnectionServiceJooqImpl(database);
  }

  @Test
  void testListWorkspaceSourceConnectionsWithCounts_NoConnections()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final UUID workspaceId = helper.getWorkspace().getWorkspaceId();

    final List<SourceConnectionWithCount> result = sourceServiceJooqImpl.listWorkspaceSourceConnectionsWithCounts(workspaceId);

    assertNotNull(result);
    assertEquals(1, result.size()); // Should have the source from setup, but with 0 connections
    assertEquals(0, result.get(0).connectionCount());
    assertEquals(helper.getSource().getSourceId(), result.get(0).source().getSourceId());
    assertNull(result.get(0).lastSync(), "Should have no last sync when no connections exist");
  }

  @Test
  void testListWorkspaceSourceConnectionsWithCounts_WithActiveConnections()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final SourceConnection source = helper.getSource();
    final DestinationConnection destination = helper.getDestination();
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

    final List<SourceConnectionWithCount> result = sourceServiceJooqImpl.listWorkspaceSourceConnectionsWithCounts(workspaceId);

    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals(3, result.get(0).connectionCount());
    assertEquals(source.getSourceId(), result.get(0).source().getSourceId());
    assertTrue(Math.abs(result.get(0).lastSync().toEpochSecond() - newestJobTime.toEpochSecond()) < 2,
        "Last sync should be the most recent job time");

    // All 3 connections have SUCCEEDED as their most recent job status
    assertEquals(3, result.get(0).connectionJobStatuses().get(JobStatus.SUCCEEDED), "Should have 3 SUCCEEDED jobs");
    assertEquals(0, result.get(0).connectionJobStatuses().get(JobStatus.FAILED), "Should have 0 FAILED jobs");
    assertEquals(0, result.get(0).connectionJobStatuses().get(JobStatus.PENDING), "Should have 0 PENDING jobs");
    assertEquals(0, result.get(0).connectionJobStatuses().get(JobStatus.INCOMPLETE), "Should have 0 INCOMPLETE jobs");
    assertEquals(0, result.get(0).connectionJobStatuses().get(JobStatus.CANCELLED), "Should have 0 CANCELLED jobs");
    assertEquals(0, result.get(0).connectionJobStatuses().get(JobStatus.RUNNING), "Should have 0 RUNNING jobs");

  }

  @Test
  void testListWorkspaceSourceConnectionsWithCounts_ExcludesDeprecatedConnections()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final SourceConnection source = helper.getSource();
    final DestinationConnection destination = helper.getDestination();
    final UUID workspaceId = helper.getWorkspace().getWorkspaceId();

    // Create 2 active connections and 2 deprecated connections
    createConnection(source, destination, Status.ACTIVE);
    createConnection(source, destination, Status.INACTIVE);
    createConnection(source, destination, Status.DEPRECATED);
    createConnection(source, destination, Status.DEPRECATED);

    final List<SourceConnectionWithCount> result = sourceServiceJooqImpl.listWorkspaceSourceConnectionsWithCounts(workspaceId);

    assertNotNull(result);
    assertEquals(1, result.size());
    // Should only count active & inactive connections, not deprecated
    assertEquals(2, result.get(0).connectionCount());
    assertEquals(source.getSourceId(), result.get(0).source().getSourceId());
  }

  @Test
  void testListWorkspaceSourceConnectionsWithCounts_MultipleSources()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final SourceConnection source1 = helper.getSource();
    final DestinationConnection destination = helper.getDestination();
    final UUID workspaceId = helper.getWorkspace().getWorkspaceId();

    final SourceConnection source2 = createAdditionalSource(workspaceId, helper);

    // Create connections: 3 from source1, 1 from source2
    createConnection(source1, destination, Status.ACTIVE);
    createConnection(source1, destination, Status.ACTIVE);
    createConnection(source1, destination, Status.ACTIVE);
    createConnection(source2, destination, Status.ACTIVE);

    final List<SourceConnectionWithCount> result = sourceServiceJooqImpl.listWorkspaceSourceConnectionsWithCounts(workspaceId);

    assertNotNull(result);
    assertEquals(2, result.size());

    boolean found1 = false;
    boolean found2 = false;
    for (final SourceConnectionWithCount sourceWithCount : result) {
      if (sourceWithCount.source().getSourceId().equals(source1.getSourceId())) {
        assertEquals(3, sourceWithCount.connectionCount());
        found1 = true;
      } else if (sourceWithCount.source().getSourceId().equals(source2.getSourceId())) {
        assertEquals(1, sourceWithCount.connectionCount());
        found2 = true;
      }
    }
    assertTrue(found1, "Source 1 should be in results");
    assertTrue(found2, "Source 2 should be in results");
  }

  @Test
  void testListWorkspaceSourceConnectionsWithCounts_DifferentWorkspaces()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final SourceConnection source = helper.getSource();
    final DestinationConnection destination = helper.getDestination();

    createConnection(source, destination, Status.ACTIVE);

    final UUID differentWorkspaceId = UUID.randomUUID();
    final List<SourceConnectionWithCount> result = sourceServiceJooqImpl.listWorkspaceSourceConnectionsWithCounts(differentWorkspaceId);

    assertNotNull(result);
    assertEquals(0, result.size());
  }

  @Test
  void testListWorkspaceSourceConnectionsWithCounts_WithMixedConnectionStatuses()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final SourceConnection source1 = helper.getSource();
    final SourceConnection source2 = createAdditionalSource(helper.getWorkspace().getWorkspaceId(), helper);
    final DestinationConnection destination = helper.getDestination();
    final UUID workspaceId = helper.getWorkspace().getWorkspaceId();

    // Create connections with different statuses and names
    final StandardSync activeConnection1 = createConnectionWithName(source1, destination, Status.ACTIVE, "active-source1-connection");
    final StandardSync inactiveConnection = createConnectionWithName(source1, destination, Status.INACTIVE, "inactive-source-connection");
    final StandardSync deprecatedConnection = createConnectionWithName(source1, destination, Status.DEPRECATED, "deprecated-connection");
    final StandardSync activeConnection2 = createConnectionWithName(source2, destination, Status.ACTIVE, "active-source2-connection");

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

    final List<SourceConnectionWithCount> result = sourceServiceJooqImpl.listWorkspaceSourceConnectionsWithCounts(workspaceId);

    assertNotNull(result);
    assertEquals(2, result.size());

    // Find and verify each source in the results
    boolean found1 = false;
    boolean found2 = false;
    for (final SourceConnectionWithCount sourceWithCount : result) {
      if (sourceWithCount.source().getSourceId().equals(source1.getSourceId())) {
        // source1 has 2 non-deprecated connections (1 active, 1 inactive)
        assertEquals(2, sourceWithCount.connectionCount());
        assertNotNull(sourceWithCount.lastSync(), "Should have last sync when connections with jobs exist");
        // Should be the newer job time (from inactive connection)
        assertEquals(jobTimeInactive1.toEpochSecond(), sourceWithCount.lastSync().toEpochSecond(),
            "Last sync should be the most recent job time");
        // source1 has 1 SUCCEEDED job (from active connection) and 1 FAILED job (from inactive connection)
        assertEquals(1, sourceWithCount.connectionJobStatuses().get(JobStatus.SUCCEEDED),
            "Should have 1 succeeded job (from active connection)");
        assertEquals(1, sourceWithCount.connectionJobStatuses().get(JobStatus.FAILED),
            "Should have 1 failed job (from inactive connection)");
        found1 = true;
      } else if (sourceWithCount.source().getSourceId().equals(source2.getSourceId())) {
        // source2 has 1 non-deprecated connection (1 active)
        assertEquals(1, sourceWithCount.connectionCount());
        assertNotNull(sourceWithCount.lastSync(), "Should have last sync when connections with jobs exist");
        // Should have 1 RUNNING job (most recent for the connection)
        assertEquals(1, sourceWithCount.connectionJobStatuses().get(JobStatus.RUNNING),
            "Should have 1 running job");
        found2 = true;
      }
    }
    assertTrue(found1, "Source 1 should be in results");
    assertTrue(found2, "Source 2 should be in results");
  }

  @Test
  void testListWorkspaceSourceConnectionsWithCounts_WithConnectionsButNoJobs()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final SourceConnection source = helper.getSource();
    final DestinationConnection destination = helper.getDestination();
    final UUID workspaceId = helper.getWorkspace().getWorkspaceId();

    // Create connections but no job records
    createConnectionWithName(source, destination, Status.ACTIVE, "source-connection-without-jobs");
    createConnectionWithName(source, destination, Status.INACTIVE, "another-source-connection-without-jobs");

    final List<SourceConnectionWithCount> result = sourceServiceJooqImpl.listWorkspaceSourceConnectionsWithCounts(workspaceId);

    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals(2, result.get(0).connectionCount());
    assertEquals(source.getSourceId(), result.get(0).source().getSourceId());
    assertNull(result.get(0).lastSync(), "Should have no last sync when no jobs exist");
  }

  @Test
  void testListWorkspaceSourceConnectionsWithCounts_MixedConnectionStates()
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

    final List<SourceConnectionWithCount> result = sourceServiceJooqImpl.listWorkspaceSourceConnectionsWithCounts(workspaceId);

    assertNotNull(result);
    assertEquals(1, result.size());
    // Should count all non-deprecated connections (2 active + 1 inactive)
    assertEquals(3, result.get(0).connectionCount());
    assertEquals(source.getSourceId(), result.get(0).source().getSourceId());
  }

  @Test
  void testListWorkspaceSourceConnectionsWithCounts_AllJobStatuses()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final SourceConnection source = helper.getSource();
    final DestinationConnection destination = helper.getDestination();
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

    final List<SourceConnectionWithCount> result = sourceServiceJooqImpl.listWorkspaceSourceConnectionsWithCounts(workspaceId);

    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals(6, result.get(0).connectionCount());
    assertEquals(source.getSourceId(), result.get(0).source().getSourceId());

    // Verify all job statuses are correctly counted (1 connection per status)
    assertEquals(1, result.get(0).connectionJobStatuses().get(JobStatus.SUCCEEDED), "Should have 1 SUCCEEDED job");
    assertEquals(1, result.get(0).connectionJobStatuses().get(JobStatus.FAILED), "Should have 1 FAILED job");
    assertEquals(1, result.get(0).connectionJobStatuses().get(JobStatus.RUNNING), "Should have 1 RUNNING job");
    assertEquals(1, result.get(0).connectionJobStatuses().get(JobStatus.PENDING), "Should have 1 PENDING job");
    assertEquals(1, result.get(0).connectionJobStatuses().get(JobStatus.INCOMPLETE), "Should have 1 INCOMPLETE job");
    assertEquals(1, result.get(0).connectionJobStatuses().get(JobStatus.CANCELLED), "Should have 1 CANCELLED job");

    // Verify last sync time is the most recent across all connections
    assertNotNull(result.get(0).lastSync(), "Should have last sync when connections with jobs exist");
    assertTrue(Math.abs(result.get(0).lastSync().toEpochSecond() - baseTime.toEpochSecond()) < 2,
        "Last sync should be the most recent job time across all connections");
  }

  private StandardSync createConnection(final SourceConnection source, final DestinationConnection destination, final Status status)
      throws IOException {
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

  private SourceConnection createAdditionalSource(final UUID workspaceId, final JooqTestDbSetupHelper helper) throws IOException {
    final UUID sourceId = UUID.randomUUID();
    final SourceConnection source = new SourceConnection()
        .withSourceId(sourceId)
        .withSourceDefinitionId(helper.getSourceDefinition().getSourceDefinitionId())
        .withWorkspaceId(workspaceId)
        .withName("test-source-" + sourceId)
        .withConfiguration(io.airbyte.commons.json.Jsons.emptyObject())
        .withTombstone(false);

    sourceServiceJooqImpl.writeSourceConnectionNoSecrets(source);
    return source;
  }

  private StandardSync createConnectionWithName(final SourceConnection source,
                                                final DestinationConnection destination,
                                                final Status status,
                                                final String connectionName)
      throws IOException {
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

}
