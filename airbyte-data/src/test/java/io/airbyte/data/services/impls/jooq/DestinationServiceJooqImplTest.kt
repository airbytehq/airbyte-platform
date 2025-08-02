/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationConnection
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.SupportLevel
import io.airbyte.config.Tag
import io.airbyte.config.helpers.CatalogHelpers
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.shared.ActorServicePaginationHelper
import io.airbyte.data.services.shared.SortKey
import io.airbyte.data.services.shared.WorkspaceResourceCursorPagination.Companion.fromValues
import io.airbyte.db.ContextQueryFunction
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.protocol.models.v0.Field
import io.airbyte.test.utils.BaseConfigDatabaseTest
import io.airbyte.validation.json.JsonValidationException
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.io.IOException
import java.sql.SQLException
import java.time.OffsetDateTime
import java.util.Locale
import java.util.UUID
import kotlin.math.abs

internal class DestinationServiceJooqImplTest : BaseConfigDatabaseTest() {
  private val destinationServiceJooqImpl: DestinationServiceJooqImpl
  private val sourceServiceJooqImpl: SourceServiceJooqImpl
  private val connectionServiceJooqImpl: ConnectionServiceJooqImpl

  init {
    val featureFlagClient = Mockito.mock<TestClient>(TestClient::class.java)
    val metricClient = Mockito.mock<MetricClient>(MetricClient::class.java)
    val connectionService = Mockito.mock<ConnectionService>(ConnectionService::class.java)
    val actorDefinitionVersionUpdater = Mockito.mock<ActorDefinitionVersionUpdater>(ActorDefinitionVersionUpdater::class.java)
    val secretPersistenceConfigService = Mockito.mock<SecretPersistenceConfigService>(SecretPersistenceConfigService::class.java)
    val actorPaginationServiceHelper = ActorServicePaginationHelper(database!!)

    this.destinationServiceJooqImpl =
      DestinationServiceJooqImpl(
        database!!,
        featureFlagClient,
        connectionService,
        actorDefinitionVersionUpdater,
        metricClient,
        actorPaginationServiceHelper,
      )
    this.sourceServiceJooqImpl =
      SourceServiceJooqImpl(
        database!!,
        featureFlagClient,
        secretPersistenceConfigService,
        connectionService,
        actorDefinitionVersionUpdater,
        metricClient,
        actorPaginationServiceHelper,
      )
    this.connectionServiceJooqImpl = ConnectionServiceJooqImpl(database!!)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun testListWorkspaceDestinationConnectionsWithCounts_NoConnections() {
    val helper = JooqTestDbSetupHelper()
    helper.setUpDependencies()

    val workspaceId = helper.workspace!!.workspaceId

    // Test with no connections - should return empty list
    val result =
      destinationServiceJooqImpl.listWorkspaceDestinationConnectionsWithCounts(
        workspaceId,
        fromValues(
          SortKey.DESTINATION_NAME,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
        ),
      )

    Assertions.assertNotNull(result)
    Assertions.assertEquals(1, result.size) // Should have the destination from setup, but with 0 connections
    Assertions.assertEquals(0, result.get(0).connectionCount)
    Assertions.assertEquals(helper.destination!!.destinationId, result.get(0).destination.destinationId)
    Assertions.assertNull(result.get(0).lastSync, "Should have no last sync when no connections exist")
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun testListWorkspaceDestinationConnectionsWithCounts_WithActiveConnections() {
    val helper = JooqTestDbSetupHelper()
    helper.setUpDependencies()

    val destination = helper.destination!!
    val source = helper.source!!
    val workspaceId = helper.workspace!!.workspaceId

    val connection1 = createConnectionWithName(source, destination, StandardSync.Status.ACTIVE, "connection-1")
    val connection2 = createConnectionWithName(source, destination, StandardSync.Status.ACTIVE, "connection-2")
    val connection3 = createConnectionWithName(source, destination, StandardSync.Status.ACTIVE, "connection-3")

    val oldestJobTime = OffsetDateTime.now().minusHours(3)
    val middleJobTime = OffsetDateTime.now().minusHours(2)
    val newestJobTime = OffsetDateTime.now().minusHours(1)
    createJobRecord(connection1.getConnectionId(), newestJobTime, JobStatus.SUCCEEDED)
    createJobRecord(connection1.getConnectionId(), middleJobTime, JobStatus.RUNNING)
    createJobRecord(connection1.getConnectionId(), oldestJobTime, JobStatus.CANCELLED)
    createJobRecord(connection2.getConnectionId(), newestJobTime, JobStatus.SUCCEEDED)
    createJobRecord(connection3.getConnectionId(), newestJobTime, JobStatus.SUCCEEDED)

    val result =
      destinationServiceJooqImpl.listWorkspaceDestinationConnectionsWithCounts(
        workspaceId,
        fromValues(
          SortKey.DESTINATION_NAME,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
        ),
      )

    Assertions.assertNotNull(result)
    Assertions.assertEquals(1, result.size)
    Assertions.assertEquals(3, result.get(0).connectionCount)
    Assertions.assertEquals(destination.destinationId, result.get(0).destination.destinationId)
    Assertions.assertTrue(
      abs(result.get(0).lastSync!!.toEpochSecond() - newestJobTime.toEpochSecond()) < 2,
      "Last sync should be the most recent job time",
    )

    // All 3 connections have SUCCEEDED as their most recent job status
    Assertions.assertEquals(3, result.get(0).connectionJobStatuses.get(JobStatus.SUCCEEDED), "Should have 3 SUCCEEDED jobs")
    Assertions.assertEquals(0, result.get(0).connectionJobStatuses.get(JobStatus.FAILED), "Should have 0 FAILED jobs")
    Assertions.assertEquals(0, result.get(0).connectionJobStatuses.get(JobStatus.PENDING), "Should have 0 PENDING jobs")
    Assertions.assertEquals(0, result.get(0).connectionJobStatuses.get(JobStatus.INCOMPLETE), "Should have 0 INCOMPLETE jobs")
    Assertions.assertEquals(0, result.get(0).connectionJobStatuses.get(JobStatus.CANCELLED), "Should have 0 CANCELLED jobs")
    Assertions.assertEquals(0, result.get(0).connectionJobStatuses.get(JobStatus.RUNNING), "Should have 0 RUNNING jobs")
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun testListWorkspaceDestinationConnectionsWithCounts_ExcludesDeprecatedConnections() {
    val helper = JooqTestDbSetupHelper()
    helper.setUpDependencies()

    val destination = helper.destination!!
    val source = helper.source!!
    val workspaceId = helper.workspace!!.workspaceId

    // Create 2 active connections and 1 deprecated connection
    createConnection(source, destination, StandardSync.Status.ACTIVE)
    createConnection(source, destination, StandardSync.Status.ACTIVE)
    createConnection(source, destination, StandardSync.Status.DEPRECATED)

    val result =
      destinationServiceJooqImpl.listWorkspaceDestinationConnectionsWithCounts(
        workspaceId,
        fromValues(
          SortKey.DESTINATION_NAME,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
        ),
      )

    Assertions.assertNotNull(result)
    Assertions.assertEquals(1, result.size)
    Assertions.assertEquals(2, result.get(0).connectionCount) // Should only count active connections, not deprecated
    Assertions.assertEquals(destination.destinationId, result.get(0).destination.destinationId)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun testListWorkspaceDestinationConnectionsWithCounts_MultipleDestinations() {
    val helper = JooqTestDbSetupHelper()
    helper.setUpDependencies()

    val source = helper.source!!
    val destination1 = helper.destination!!
    val workspaceId = helper.workspace!!.workspaceId

    // Create a second destination in the same workspace
    val destination2 = createAdditionalDestination(workspaceId, helper)

    // Create connections: 2 to destination1, 1 to destination2
    createConnection(source, destination1, StandardSync.Status.ACTIVE)
    createConnection(source, destination1, StandardSync.Status.ACTIVE)
    createConnection(source, destination2, StandardSync.Status.ACTIVE)

    val result =
      destinationServiceJooqImpl.listWorkspaceDestinationConnectionsWithCounts(
        workspaceId,
        fromValues(
          SortKey.DESTINATION_NAME,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
        ),
      )

    Assertions.assertNotNull(result)
    Assertions.assertEquals(2, result.size)

    var found1 = false
    var found2 = false
    for (destWithCount in result) {
      if (destWithCount.destination.destinationId == destination1.destinationId) {
        Assertions.assertEquals(2, destWithCount.connectionCount)
        found1 = true
      } else if (destWithCount.destination.destinationId == destination2.destinationId) {
        Assertions.assertEquals(1, destWithCount.connectionCount)
        found2 = true
      }
    }
    Assertions.assertTrue(found1, "Destination 1 should be in results")
    Assertions.assertTrue(found2, "Destination 2 should be in results")
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun testListWorkspaceDestinationConnectionsWithCounts_DifferentWorkspaces() {
    val helper = JooqTestDbSetupHelper()
    helper.setUpDependencies()

    val destination = helper.destination!!
    val source = helper.source!!

    // Create connections in the workspace
    createConnection(source, destination, StandardSync.Status.ACTIVE)

    // Query for a different workspace (should return empty)
    val differentWorkspaceId = UUID.randomUUID()
    val result =
      destinationServiceJooqImpl.listWorkspaceDestinationConnectionsWithCounts(
        differentWorkspaceId,
        fromValues(
          SortKey.DESTINATION_NAME,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
        ),
      )

    Assertions.assertNotNull(result)
    Assertions.assertEquals(0, result.size) // No destinations in the different workspace
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun testListWorkspaceDestinationConnectionsWithCounts_WithMixedConnectionStatuses() {
    val helper = JooqTestDbSetupHelper()
    helper.setUpDependencies()

    val destination1 = helper.destination!!
    val destination2 = createAdditionalDestination(helper.workspace!!.workspaceId, helper)
    val source = helper.source!!
    val workspaceId = helper.workspace!!.workspaceId

    // Create connections with different statuses and names
    val activeConnection1 = createConnectionWithName(source, destination1, StandardSync.Status.ACTIVE, "active-dest1-connection")
    val inactiveConnection = createConnectionWithName(source, destination1, StandardSync.Status.INACTIVE, "inactive-dest-connection")
    val deprecatedConnection = createConnectionWithName(source, destination1, StandardSync.Status.DEPRECATED, "deprecated-connection")
    val activeConnection2 = createConnectionWithName(source, destination2, StandardSync.Status.ACTIVE, "active-dest2-connection")

    // Create job records with more status variety including FAILED
    val jobTimeActive1 = OffsetDateTime.now().minusHours(3)
    val jobTimeActive2 = OffsetDateTime.now().minusHours(4)
    val jobTimeInactive1 = OffsetDateTime.now().minusHours(1)
    val jobTimeDeprecated1 = OffsetDateTime.now().minusHours(0)

    createJobRecord(activeConnection1.getConnectionId(), jobTimeActive1, JobStatus.SUCCEEDED)
    createJobRecord(activeConnection1.getConnectionId(), jobTimeActive2, JobStatus.SUCCEEDED)
    createJobRecord(inactiveConnection.getConnectionId(), jobTimeInactive1, JobStatus.FAILED)
    createJobRecord(deprecatedConnection.getConnectionId(), jobTimeDeprecated1, JobStatus.SUCCEEDED)
    createJobRecord(activeConnection2.getConnectionId(), jobTimeActive1, JobStatus.RUNNING)

    val result =
      destinationServiceJooqImpl.listWorkspaceDestinationConnectionsWithCounts(
        workspaceId,
        fromValues(
          SortKey.DESTINATION_NAME,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
        ),
      )

    Assertions.assertNotNull(result)
    Assertions.assertEquals(2, result.size)

    // Find and verify each destination in the results
    var found1 = false
    var found2 = false
    for (destinationWithCount in result) {
      if (destinationWithCount.destination.destinationId == destination1.destinationId) {
        // destination1 has 2 non-deprecated connections (1 active, 1 inactive)
        Assertions.assertEquals(2, destinationWithCount.connectionCount)
        Assertions.assertNotNull(destinationWithCount.lastSync, "Should have last sync when connections with jobs exist")
        // Should be the newer job time (from inactive connection)
        Assertions.assertEquals(
          jobTimeInactive1.toEpochSecond(),
          destinationWithCount.lastSync!!.toEpochSecond(),
          "Last sync should be the most recent job time",
        )
        // destination1 has 1 SUCCEEDED job (from active connection) and 1 FAILED job (from inactive
        // connection)
        Assertions.assertEquals(
          1,
          destinationWithCount.connectionJobStatuses.get(JobStatus.SUCCEEDED),
          "Should have 1 succeeded job (from active connection)",
        )
        Assertions.assertEquals(
          1,
          destinationWithCount.connectionJobStatuses.get(JobStatus.FAILED),
          "Should have 1 failed job (from inactive connection)",
        )
        found1 = true
      } else if (destinationWithCount.destination.destinationId == destination2.destinationId) {
        // destination2 has 1 non-deprecated connection (1 active)
        Assertions.assertEquals(1, destinationWithCount.connectionCount)
        Assertions.assertNotNull(destinationWithCount.lastSync, "Should have last sync when connections with jobs exist")
        // Should have 1 RUNNING job (most recent for the connection)
        Assertions.assertEquals(
          1,
          destinationWithCount.connectionJobStatuses.get(JobStatus.RUNNING),
          "Should have 1 running job",
        )
        found2 = true
      }
    }
    Assertions.assertTrue(found1, "Destination 1 should be in results")
    Assertions.assertTrue(found2, "Destination 2 should be in results")
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun testListWorkspaceDestinationConnectionsWithCounts_WithConnectionsButNoJobs() {
    val helper = JooqTestDbSetupHelper()
    helper.setUpDependencies()

    val destination = helper.destination!!
    val source = helper.source!!
    val workspaceId = helper.workspace!!.workspaceId

    // Create connections but no job records
    createConnectionWithName(source, destination, StandardSync.Status.ACTIVE, "connection-without-jobs")
    createConnectionWithName(source, destination, StandardSync.Status.INACTIVE, "another-connection-without-jobs")

    val result =
      destinationServiceJooqImpl.listWorkspaceDestinationConnectionsWithCounts(
        workspaceId,
        fromValues(
          SortKey.DESTINATION_NAME,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
        ),
      )

    Assertions.assertNotNull(result)
    Assertions.assertEquals(1, result.size)
    Assertions.assertEquals(2, result.get(0).connectionCount)
    Assertions.assertEquals(destination.destinationId, result.get(0).destination.destinationId)
    Assertions.assertNull(result.get(0).lastSync, "Should have no last sync when no jobs exist")
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun testListWorkspaceDestinationConnectionsWithCounts_MixedConnectionStates() {
    val helper = JooqTestDbSetupHelper()
    helper.setUpDependencies()

    val source = helper.source!!
    val destination = helper.destination!!
    val workspaceId = helper.workspace!!.workspaceId

    createConnection(source, destination, StandardSync.Status.ACTIVE)
    createConnection(source, destination, StandardSync.Status.INACTIVE)
    createConnection(source, destination, StandardSync.Status.DEPRECATED)
    createConnection(source, destination, StandardSync.Status.ACTIVE)

    val result =
      destinationServiceJooqImpl.listWorkspaceDestinationConnectionsWithCounts(
        workspaceId,
        fromValues(
          SortKey.DESTINATION_NAME,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
        ),
      )

    Assertions.assertNotNull(result)
    Assertions.assertEquals(1, result.size)
    // Should count all non-deprecated connections (2 active + 1 inactive)
    Assertions.assertEquals(3, result.get(0).connectionCount)
    Assertions.assertEquals(destination.destinationId, result.get(0).destination.destinationId)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun testListWorkspaceDestinationConnectionsWithCounts_AllJobStatuses() {
    val helper = JooqTestDbSetupHelper()
    helper.setUpDependencies()

    val destination = helper.destination!!
    val source = helper.source!!
    val workspaceId = helper.workspace!!.workspaceId

    // Create 6 connections, each with a different most recent job status
    val succeededConnection = createConnectionWithName(source, destination, StandardSync.Status.ACTIVE, "succeeded-connection")
    val failedConnection = createConnectionWithName(source, destination, StandardSync.Status.ACTIVE, "failed-connection")
    val runningConnection = createConnectionWithName(source, destination, StandardSync.Status.ACTIVE, "running-connection")
    val pendingConnection = createConnectionWithName(source, destination, StandardSync.Status.ACTIVE, "pending-connection")
    val incompleteConnection = createConnectionWithName(source, destination, StandardSync.Status.ACTIVE, "incomplete-connection")
    val cancelledConnection = createConnectionWithName(source, destination, StandardSync.Status.ACTIVE, "cancelled-connection")

    // Create jobs with different statuses - most recent job determines the status counted
    val baseTime = OffsetDateTime.now().minusHours(1)

    // Each connection gets multiple jobs, but only the most recent status matters
    createJobRecord(succeededConnection.getConnectionId(), baseTime.minusHours(2), JobStatus.PENDING)
    createJobRecord(succeededConnection.getConnectionId(), baseTime, JobStatus.SUCCEEDED)

    createJobRecord(failedConnection.getConnectionId(), baseTime.minusHours(1), JobStatus.RUNNING)
    createJobRecord(failedConnection.getConnectionId(), baseTime, JobStatus.FAILED)

    createJobRecord(runningConnection.getConnectionId(), baseTime.minusHours(1), JobStatus.PENDING)
    createJobRecord(runningConnection.getConnectionId(), baseTime, JobStatus.RUNNING)

    createJobRecord(pendingConnection.getConnectionId(), baseTime.minusHours(1), JobStatus.FAILED)
    createJobRecord(pendingConnection.getConnectionId(), baseTime, JobStatus.PENDING)

    createJobRecord(incompleteConnection.getConnectionId(), baseTime.minusHours(1), JobStatus.RUNNING)
    createJobRecord(incompleteConnection.getConnectionId(), baseTime, JobStatus.INCOMPLETE)

    createJobRecord(cancelledConnection.getConnectionId(), baseTime.minusHours(1), JobStatus.SUCCEEDED)
    createJobRecord(cancelledConnection.getConnectionId(), baseTime, JobStatus.CANCELLED)

    val result =
      destinationServiceJooqImpl.listWorkspaceDestinationConnectionsWithCounts(
        workspaceId,
        fromValues(
          SortKey.DESTINATION_NAME,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
        ),
      )

    Assertions.assertNotNull(result)
    Assertions.assertEquals(1, result.size)
    Assertions.assertEquals(6, result.get(0).connectionCount)
    Assertions.assertEquals(destination.destinationId, result.get(0).destination.destinationId)

    // Verify all job statuses are correctly counted (1 connection per status)
    Assertions.assertEquals(1, result.get(0).connectionJobStatuses.get(JobStatus.SUCCEEDED), "Should have 1 SUCCEEDED job")
    Assertions.assertEquals(1, result.get(0).connectionJobStatuses.get(JobStatus.FAILED), "Should have 1 FAILED job")
    Assertions.assertEquals(1, result.get(0).connectionJobStatuses.get(JobStatus.RUNNING), "Should have 1 RUNNING job")
    Assertions.assertEquals(1, result.get(0).connectionJobStatuses.get(JobStatus.PENDING), "Should have 1 PENDING job")
    Assertions.assertEquals(1, result.get(0).connectionJobStatuses.get(JobStatus.INCOMPLETE), "Should have 1 INCOMPLETE job")
    Assertions.assertEquals(1, result.get(0).connectionJobStatuses.get(JobStatus.CANCELLED), "Should have 1 CANCELLED job")

    // Verify last sync time is the most recent across all connections
    Assertions.assertNotNull(result.get(0).lastSync, "Should have last sync when connections with jobs exist")
    Assertions.assertTrue(
      abs(result.get(0).lastSync!!.toEpochSecond() - baseTime.toEpochSecond()) < 2,
      "Last sync should be the most recent job time across all connections",
    )
  }

  @Throws(IOException::class)
  fun createDestination(
    workspaceId: UUID?,
    destinationDefinition: StandardDestinationDefinition,
    withTombstone: Boolean?,
  ): DestinationConnection {
    val destination =
      DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withWorkspaceId(workspaceId)
        .withName("source")
        .withDestinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .withTombstone(withTombstone)
    destinationServiceJooqImpl.writeDestinationConnectionNoSecrets(destination)
    return destination
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun testListDestinationDefinitionsForWorkspace_ReturnsUsedDestinations() {
    val helper = JooqTestDbSetupHelper()
    helper.setUpDependencies()

    val result =
      destinationServiceJooqImpl.listDestinationDefinitionsForWorkspace(helper.workspace!!.workspaceId, false)

    Assertions.assertNotNull(result)
    Assertions.assertEquals(1, result.size)
    Assertions.assertEquals(
      helper.destinationDefinition!!.destinationDefinitionId,
      result.get(0)!!.destinationDefinitionId,
    )
    Assertions.assertEquals("Test destination def", result.get(0)!!.getName())
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun testListDestinationDefinitionsForWorkspace_ExcludesTombstonedActors() {
    val helper = JooqTestDbSetupHelper()
    helper.setUpDependencies()

    val workspaceId = helper.workspace!!.workspaceId

    // Create an additional destination and tombstone it
    val additionalDestination = createAdditionalDestination(workspaceId, helper)
    val tombstonedDestination = additionalDestination.withTombstone(true)
    destinationServiceJooqImpl.writeDestinationConnectionNoSecrets(tombstonedDestination)

    // Should only return the non-tombstoned destination
    val result =
      destinationServiceJooqImpl.listDestinationDefinitionsForWorkspace(workspaceId, false)

    Assertions.assertNotNull(result)
    Assertions.assertEquals(1, result.size)
    Assertions.assertEquals(
      helper.destinationDefinition!!.destinationDefinitionId,
      result.get(0)!!.destinationDefinitionId,
    )
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun testListDestinationDefinitionsForWorkspace_IncludesTombstonedActorsWhenRequested() {
    val helper = JooqTestDbSetupHelper()
    helper.setUpDependencies()

    val workspaceId = helper.workspace!!.workspaceId

    // Create an additional destination and tombstone it
    val additionalDestination = createAdditionalDestination(workspaceId, helper)

    // Tombstone the additional destination
    val tombstonedDestination = additionalDestination.withTombstone(true)
    destinationServiceJooqImpl.writeDestinationConnectionNoSecrets(tombstonedDestination)

    // Should return the definition when including tombstones
    val result =
      destinationServiceJooqImpl.listDestinationDefinitionsForWorkspace(workspaceId, true)

    Assertions.assertNotNull(result)
    Assertions.assertEquals(1, result.size)

    // Should be the same definition used by both active and tombstoned destinations
    Assertions.assertEquals(
      helper.destinationDefinition!!.destinationDefinitionId,
      result.get(0)!!.destinationDefinitionId,
    )
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun testListDestinationDefinitionsForWorkspace_EmptyWhenNoDestinationsInWorkspace() {
    val helper = JooqTestDbSetupHelper()
    helper.setUpDependencies()

    // Use a different workspace that has no destinations
    val emptyWorkspaceId = UUID.randomUUID()

    val result =
      destinationServiceJooqImpl.listDestinationDefinitionsForWorkspace(emptyWorkspaceId, false)

    Assertions.assertNotNull(result)
    Assertions.assertEquals(0, result.size)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun testListDestinationDefinitionsForWorkspace_MultipleDestinationsWithSameDefinition() {
    val helper = JooqTestDbSetupHelper()
    helper.setUpDependencies()

    val workspaceId = helper.workspace!!.workspaceId

    // Create additional destinations using the same definition
    createAdditionalDestination(workspaceId, helper)
    createAdditionalDestination(workspaceId, helper)

    // Should return the definition only once, even though multiple destinations use it
    val result =
      destinationServiceJooqImpl.listDestinationDefinitionsForWorkspace(workspaceId, false)

    Assertions.assertNotNull(result)
    Assertions.assertEquals(1, result.size)
    Assertions.assertEquals(
      helper.destinationDefinition!!.destinationDefinitionId,
      result.get(0)!!.destinationDefinitionId,
    )
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun testListDestinationDefinitionsForWorkspace_MultipleDestinationsWithDifferentDefinitions() {
    val helper = JooqTestDbSetupHelper()
    helper.setUpDependencies()

    val workspaceId = helper.workspace!!.workspaceId

    // Create a second destination definition
    val secondDestinationDefinitionId = UUID.randomUUID()
    val secondDestinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(secondDestinationDefinitionId)
        .withName("Second destination def")
        .withTombstone(false)
    val secondDestinationDefinitionVersion: ActorDefinitionVersion? =
      createBaseActorDefVersion(secondDestinationDefinitionId, "0.0.2")
    helper.createActorDefinition(secondDestinationDefinition, secondDestinationDefinitionVersion!!)

    // Create a destination using the second definition
    val secondDestinationId = UUID.randomUUID()
    val secondDestination =
      DestinationConnection()
        .withDestinationId(secondDestinationId)
        .withDestinationDefinitionId(secondDestinationDefinitionId)
        .withWorkspaceId(workspaceId)
        .withName("test-destination-" + secondDestinationId)
        .withConfiguration(emptyObject())
        .withTombstone(false)
    destinationServiceJooqImpl.writeDestinationConnectionNoSecrets(secondDestination)

    // Should return both definitions
    val result =
      destinationServiceJooqImpl.listDestinationDefinitionsForWorkspace(workspaceId, false)

    Assertions.assertNotNull(result)
    Assertions.assertEquals(2, result.size)

    val definitionIds =
      result
        .stream()
        .map<UUID?> { obj: StandardDestinationDefinition? -> obj!!.getDestinationDefinitionId() }
        .toList()

    Assertions.assertTrue(definitionIds.contains(helper.destinationDefinition!!.destinationDefinitionId))
    Assertions.assertTrue(definitionIds.contains(secondDestinationDefinitionId))
  }

  @Throws(IOException::class, JsonValidationException::class)
  private fun createConnection(
    source: SourceConnection,
    destination: DestinationConnection,
    status: StandardSync.Status?,
  ): StandardSync {
    val connectionId = UUID.randomUUID()
    val streams =
      listOf<ConfiguredAirbyteStream>(
        catalogHelpers.createConfiguredAirbyteStream("test_stream", "test_namespace", Field.of("field", JsonSchemaType.STRING)),
      )

    val connection =
      StandardSync()
        .withConnectionId(connectionId)
        .withSourceId(source.sourceId)
        .withDestinationId(destination.destinationId)
        .withName("test-connection-" + connectionId)
        .withCatalog(ConfiguredAirbyteCatalog().withStreams(streams))
        .withManual(true)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
        .withBreakingChange(false)
        .withStatus(status)
        .withTags(mutableListOf<Tag?>())

    connectionServiceJooqImpl.writeStandardSync(connection)
    return connection
  }

  @Throws(IOException::class)
  private fun createAdditionalDestination(
    workspaceId: UUID?,
    helper: JooqTestDbSetupHelper,
  ): DestinationConnection {
    val destinationId = UUID.randomUUID()
    val destination =
      DestinationConnection()
        .withDestinationId(destinationId)
        .withDestinationDefinitionId(helper.destinationDefinition!!.destinationDefinitionId)
        .withWorkspaceId(workspaceId)
        .withName("test-destination-" + destinationId)
        .withConfiguration(emptyObject())
        .withTombstone(false)

    destinationServiceJooqImpl.writeDestinationConnectionNoSecrets(destination)
    return destination
  }

  @Throws(IOException::class, JsonValidationException::class)
  private fun createConnectionWithName(
    source: SourceConnection,
    destination: DestinationConnection,
    status: StandardSync.Status?,
    connectionName: String?,
  ): StandardSync {
    val connectionId = UUID.randomUUID()
    val streams =
      listOf<ConfiguredAirbyteStream>(
        catalogHelpers.createConfiguredAirbyteStream("test_stream", "test_namespace", Field.of("field", JsonSchemaType.STRING)),
      )

    val connection =
      StandardSync()
        .withConnectionId(connectionId)
        .withSourceId(source.sourceId)
        .withDestinationId(destination.destinationId)
        .withName(connectionName)
        .withCatalog(ConfiguredAirbyteCatalog().withStreams(streams))
        .withManual(true)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
        .withBreakingChange(false)
        .withStatus(status)
        .withTags(mutableListOf<Tag?>())

    connectionServiceJooqImpl.writeStandardSync(connection)
    return connection
  }

  private fun createJobRecord(
    connectionId: UUID,
    createdAt: OffsetDateTime?,
    jobStatus: JobStatus,
  ) {
    // Insert a job record directly into the database
    // This simulates the job table structure used in the actual query
    database!!.query<Any?>(
      ContextQueryFunction { ctx: DSLContext? ->
        ctx!!
          .insertInto<Record?>(DSL.table("jobs"))
          .columns<Any?, Any?, Any?, Any?, Any?, Any?>(
            DSL.field("config_type"),
            DSL.field("scope"),
            DSL.field("created_at"),
            DSL.field("updated_at"),
            DSL.field("status"),
            DSL.field("config"),
          ).values(
            DSL.field("cast(? as job_config_type)", "sync"),
            connectionId.toString(),
            createdAt,
            createdAt,
            DSL.field("cast(? as job_status)", jobStatus.name.lowercase(Locale.getDefault())),
            DSL.field("cast(? as jsonb)", "{}"),
          ).execute()
        null
      },
    )
  }

  companion object {
    private val catalogHelpers = CatalogHelpers(FieldGenerator())

    private fun createBaseActorDefVersion(
      actorDefId: UUID?,
      dockerImageTag: String?,
    ): ActorDefinitionVersion? {
      val spec =
        ConnectorSpecification()
          .withConnectionSpecification(jsonNode<MutableMap<String?, String?>?>(mutableMapOf<String?, String?>("type" to "object")))

      return ActorDefinitionVersion()
        .withActorDefinitionId(actorDefId)
        .withDockerRepository("airbyte/test")
        .withDockerImageTag(dockerImageTag)
        .withSpec(spec)
        .withSupportLevel(SupportLevel.COMMUNITY)
        .withInternalSupportLevel(200L)
    }
  }
}
