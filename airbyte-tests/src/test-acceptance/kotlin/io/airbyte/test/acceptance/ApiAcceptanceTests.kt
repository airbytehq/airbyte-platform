/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance

import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.api.client.model.generated.DestinationSyncMode
import io.airbyte.api.client.model.generated.JobStatus
import io.airbyte.api.client.model.generated.SyncMode
import io.airbyte.test.utils.AcceptanceTestHarness
import io.airbyte.test.utils.AcceptanceTestUtils.IS_GKE
import io.airbyte.test.utils.AcceptanceTestUtils.modifyCatalog
import io.airbyte.test.utils.TestConnectionCreate
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.HttpStatus
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable
import org.openapitools.client.infrastructure.ClientException
import java.io.IOException
import java.util.List
import java.util.Optional
import java.util.UUID
import java.util.function.Consumer

/**
 * This class tests api functionality.
 */
@Tag("api")
internal class ApiAcceptanceTests {
  lateinit var testResources: AcceptanceTestsResources
  lateinit var testHarness: AcceptanceTestHarness
  lateinit var workspaceId: UUID

  @BeforeEach
  @Throws(Exception::class)
  fun setup() {
    testResources = AcceptanceTestsResources()
    testResources.init()
    testHarness = testResources.testHarness
    workspaceId = testResources.workspaceId
    testResources.setup()
  }

  @AfterEach
  fun tearDown() {
    testResources.tearDown()
    testResources.end()
  }

  @Test
  fun testFailedGet404() {
    val e: ClientException =
      assertThrows<ClientException> {
        testHarness.getNonExistentResource()
      }
    Assertions.assertEquals(HttpStatus.NOT_FOUND.code, e.statusCode)
  }

  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE, matches = AcceptanceTestsResources.TRUE, disabledReason = DUPLICATE_TEST_IN_GKE)
  @Throws(IOException::class)
  fun testGetSourceSpec() {
    val sourceDefId = testHarness.postgresSourceDefinitionId
    val spec =
      testHarness.getSourceDefinitionSpec(
        sourceDefId,
        workspaceId,
      )
    Assertions.assertNotNull(spec.connectionSpecification)
  }

  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE, matches = AcceptanceTestsResources.TRUE, disabledReason = DUPLICATE_TEST_IN_GKE)
  @Throws(
    IOException::class,
  )
  fun testCreateSource() {
    val dbName = "acc-test-db"
    val postgresSourceDefinitionId = testHarness.postgresSourceDefinitionId
    val sourceDbConfig = testHarness.getSourceDbConfig()

    val response =
      testHarness.createSource(
        dbName,
        workspaceId,
        postgresSourceDefinitionId,
        sourceDbConfig,
      )
    val expectedConfig = testHarness.getSourceDbConfigWithHiddenPassword()
    val configKeys = listOf("password", "database", "port", "host", "ssl", "username")

    Assertions.assertEquals(dbName, response.name)
    Assertions.assertEquals(workspaceId, response.workspaceId)
    Assertions.assertEquals(postgresSourceDefinitionId, response.sourceDefinitionId)
    configKeys.forEach(
      Consumer { key: String? ->
        if (expectedConfig[key].isNumber) {
          Assertions.assertEquals(expectedConfig[key].asInt(), response.connectionConfiguration[key].asInt())
        } else {
          Assertions.assertEquals(expectedConfig[key].asText(), response.connectionConfiguration[key].asText())
        }
      },
    )
  }

  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE, matches = AcceptanceTestsResources.TRUE, disabledReason = DUPLICATE_TEST_IN_GKE)
  @Throws(
    IOException::class,
  )
  fun testUpdateSource() {
    val dbName = "acc-test-db"
    val postgresSourceDefinitionId = testHarness.postgresSourceDefinitionId
    val sourceDbConfig = testHarness.getSourceDbConfig()

    val response =
      testHarness.createSource(
        dbName,
        workspaceId,
        postgresSourceDefinitionId,
        sourceDbConfig,
      )
    val expectedConfig = testHarness.getSourceDbConfigWithHiddenPassword()
    val configKeys = listOf("password", "database", "port", "host", "ssl", "username")

    val updateResponse =
      testHarness.updateSource(
        response.sourceId,
        expectedConfig,
        "$dbName-updated",
      )

    Assertions.assertEquals("$dbName-updated", updateResponse.name)
    Assertions.assertEquals(workspaceId, updateResponse.workspaceId)
    Assertions.assertEquals(postgresSourceDefinitionId, updateResponse.sourceDefinitionId)
    configKeys.forEach(
      Consumer { key: String? ->
        if (expectedConfig[key].isNumber) {
          Assertions.assertEquals(expectedConfig[key].asInt(), updateResponse.connectionConfiguration[key].asInt())
        } else {
          Assertions.assertEquals(expectedConfig[key].asText(), updateResponse.connectionConfiguration[key].asText())
        }
      },
    )
  }

  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE, matches = AcceptanceTestsResources.TRUE, disabledReason = DUPLICATE_TEST_IN_GKE)
  @Throws(IOException::class)
  fun testDiscoverSourceSchema() {
    val sourceId = testHarness.createPostgresSource().sourceId

    val actual = testHarness.discoverSourceSchema(sourceId)

    testHarness.compareCatalog(actual)
  }

  @Test
  @Throws(Exception::class)
  fun testDeleteConnection() {
    val sourceId = testHarness.createPostgresSource().sourceId
    val destinationId = testHarness.createPostgresDestination().destinationId
    val discoverResult = testHarness.discoverSourceSchemaWithId(sourceId)
    val srcSyncMode = SyncMode.INCREMENTAL
    val dstSyncMode = DestinationSyncMode.APPEND_DEDUP
    val catalog =
      modifyCatalog(
        discoverResult.catalog,
        Optional.of(srcSyncMode),
        Optional.of(dstSyncMode),
        Optional.of(List.of(AcceptanceTestHarness.COLUMN_ID)),
        Optional.of(List.of(List.of(AcceptanceTestHarness.COLUMN_NAME))),
        Optional.of(true),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
      )

    val connectionId =
      testHarness
        .createConnection(
          TestConnectionCreate
            .Builder(
              sourceId,
              destinationId,
              catalog,
              discoverResult.catalogId!!,
              testHarness.dataplaneGroupId,
            ).build(),
        ).connectionId

    val connectionSyncRead = testHarness.syncConnection(connectionId)
    testHarness.waitWhileJobHasStatus(connectionSyncRead.job, setOf(JobStatus.RUNNING))

    // test normal deletion of connection
    log.info { "Calling delete connection..." }
    testHarness.deleteConnection(connectionId)
    testHarness.removeConnection(connectionId) // NOTE: make sure we don't try to delete it again in test teardown.

    var connectionStatus = testHarness.getConnection(connectionId).status
    Assertions.assertEquals(ConnectionStatus.DEPRECATED, connectionStatus)

    // test that repeated deletion call for same connection is successful
    log.info { "Calling delete connection a second time to test repeat call behavior..." }
    Assertions.assertDoesNotThrow { testHarness.deleteConnection(connectionId) }

    // TODO: break this into a separate testcase which we can disable for GKE.
    if (!System.getenv().containsKey("IS_GKE")) {
      // test deletion of connection when temporal workflow is in a bad state
      log.info { "Testing connection deletion when temporal is in a terminal state" }
      val anotherConnectionId =
        testHarness
          .createConnection(
            TestConnectionCreate
              .Builder(
                sourceId,
                destinationId,
                catalog,
                discoverResult.catalogId!!,
                testHarness.dataplaneGroupId,
              ).build(),
          ).connectionId

      testHarness.terminateTemporalWorkflow(anotherConnectionId)

      // we should still be able to delete the connection when the temporal workflow is in this state
      testHarness.deleteConnection(anotherConnectionId)

      connectionStatus = testHarness.getConnection(anotherConnectionId).status
      Assertions.assertEquals(ConnectionStatus.DEPRECATED, connectionStatus)
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}

    private const val DUPLICATE_TEST_IN_GKE =
      "TODO(https://github.com/airbytehq/airbyte-platform-internal/issues/5182): eliminate test duplication"
  }
}
