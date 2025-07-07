/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance

import io.airbyte.api.client.model.generated.DestinationDefinitionIdRequestBody
import io.airbyte.api.client.model.generated.DestinationSyncMode
import io.airbyte.api.client.model.generated.SourceDefinitionIdRequestBody
import io.airbyte.api.client.model.generated.StreamStatusJobType
import io.airbyte.api.client.model.generated.StreamStatusRunState
import io.airbyte.api.client.model.generated.SyncMode
import io.airbyte.test.utils.AcceptanceTestHarness
import io.airbyte.test.utils.AcceptanceTestUtils.createAirbyteAdminApiClient
import io.airbyte.test.utils.AcceptanceTestUtils.modifyCatalog
import io.airbyte.test.utils.Asserts.assertSourceAndDestinationDbRawRecordsInSync
import io.airbyte.test.utils.Asserts.assertStreamStatuses
import io.airbyte.test.utils.TestConnectionCreate
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Tags
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.Optional
import java.util.UUID

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tags(Tag("sync"), Tag("enterprise"))
internal class AdvancedAcceptanceTests {
  @Test
  @Throws(Exception::class)
  fun testManualSync() {
    testHarness!!.setup()

    val sourceId = testHarness!!.createPostgresSource().sourceId
    val destinationId = testHarness!!.createPostgresDestination().destinationId

    val discoverResult = testHarness!!.discoverSourceSchemaWithId(sourceId)
    val syncMode = SyncMode.FULL_REFRESH
    val destinationSyncMode = DestinationSyncMode.OVERWRITE
    val catalog =
      modifyCatalog(
        discoverResult.catalog,
        Optional.of(syncMode),
        Optional.of(destinationSyncMode),
        Optional.empty(),
        Optional.empty(),
        Optional.of(true),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
      )
    val conn =
      testHarness!!.createConnection(
        TestConnectionCreate
          .Builder(
            sourceId,
            destinationId,
            catalog,
            discoverResult.catalogId!!,
            testHarness!!.dataplaneGroupId,
          ).build(),
      )
    val connectionId = conn.connectionId
    val connectionSyncRead = testHarness!!.syncConnection(connectionId)
    testHarness!!.waitForSuccessfulJob(connectionSyncRead.job)
    assertSourceAndDestinationDbRawRecordsInSync(
      testHarness!!.getSourceDatabase(),
      testHarness!!.getDestinationDatabase(),
      AcceptanceTestHarness.PUBLIC_SCHEMA_NAME,
      conn.namespaceFormat!!,
      false,
      false,
    )

    assertStreamStatuses(
      testHarness!!,
      workspaceId!!,
      connectionId,
      connectionSyncRead.job.id,
      StreamStatusRunState.COMPLETE,
      StreamStatusJobType.SYNC,
    )

    testHarness!!.cleanup()
  }

  companion object {
    private val LOGGER: Logger = LoggerFactory.getLogger(AdvancedAcceptanceTests::class.java)

    private var testHarness: AcceptanceTestHarness? = null
    private var workspaceId: UUID? = null

    @BeforeAll
    @Throws(Exception::class)
    @JvmStatic
    fun init() {
      val acceptanceTestsResources = AcceptanceTestsResources()
      acceptanceTestsResources.init()
      workspaceId = acceptanceTestsResources.workspaceId
      LOGGER.info("workspaceId = {}", workspaceId)

      val apiClient = createAirbyteAdminApiClient()

      // log which connectors are being used.
      val sourceDef =
        apiClient.sourceDefinitionApi
          .getSourceDefinition(SourceDefinitionIdRequestBody(UUID.fromString("decd338e-5647-4c0b-adf4-da0e75f5a750")))
      val destinationDef =
        apiClient.destinationDefinitionApi
          .getDestinationDefinition(DestinationDefinitionIdRequestBody(UUID.fromString("25c5221d-dce2-4163-ade9-739ef790f503")))
      LOGGER.info("pg source definition: {}", sourceDef.dockerImageTag)
      LOGGER.info("pg destination definition: {}", destinationDef.dockerImageTag)

      testHarness = AcceptanceTestHarness(apiClient, workspaceId!!)
    }

    @AfterAll
    @JvmStatic
    fun end() {
      testHarness!!.stopDbAndContainers()
    }
  }
}
