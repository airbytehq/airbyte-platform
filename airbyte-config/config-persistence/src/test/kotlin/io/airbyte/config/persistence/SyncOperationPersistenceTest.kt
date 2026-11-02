/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.commons.json.Jsons
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.OperatorWebhook
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.WebhookConfigNotFoundException
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.data.DataplaneGroupServiceTestJooqImpl
import io.airbyte.data.services.impls.jooq.OperationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.test.utils.BaseConfigDatabaseTest
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

internal class SyncOperationPersistenceTest : BaseConfigDatabaseTest() {
  private var operationService: OperationService? = null
  private lateinit var workspaceService: WorkspaceService

  @BeforeEach
  fun beforeEach() {
    truncateAllTables()

    createWorkspace()
    operationService = OperationServiceJooqImpl(database, configTransactions, workspaceService)

    for (op in OPS) {
      operationService!!.writeStandardSyncOperation(op)
    }
  }

  @Test
  fun testReadWrite() {
    for (op in OPS) {
      Assertions.assertEquals(op, operationService!!.getStandardSyncOperation(op.operationId))
    }
  }

  @Test
  fun testReadNotExists() {
    Assertions.assertThrows(
      ConfigNotFoundException::class.java,
    ) {
      operationService!!.getStandardSyncOperation(
        UUID.randomUUID(),
      )
    }
  }

  @Test
  fun testList() {
    Assertions.assertEquals(OPS, operationService!!.listStandardSyncOperations())
  }

  @Test
  fun testDelete() {
    for (op in OPS) {
      Assertions.assertEquals(op, operationService!!.getStandardSyncOperation(op.operationId))
      operationService!!.deleteStandardSyncOperation(op.operationId)
      Assertions.assertThrows(
        ConfigNotFoundException::class.java,
      ) {
        operationService!!.getStandardSyncOperation(
          UUID.randomUUID(),
        )
      }
    }
  }

  @Test
  fun `operation save rejects a webhook configuration removed by another request`() {
    operationService!!.deleteStandardSyncOperation(WEBHOOK_OP.operationId)
    val workspace = workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, false)
    workspace.webhookOperationConfigs = Jsons.deserialize("""{"webhookConfigs":[]}""")
    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    Assertions.assertThrows(WebhookConfigNotFoundException::class.java) {
      operationService!!.writeStandardSyncOperation(WEBHOOK_OP)
    }
    Assertions.assertTrue(operationService!!.getStandardSyncOperation(WEBHOOK_OP.operationId).tombstone)
  }

  @Test
  fun `unused config removal preserves saved unattached transformations`() {
    val workspace = workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, false)
    workspace.webhookOperationConfigs = Jsons.deserialize("""{"webhookConfigs":[]}""")

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    Assertions.assertTrue(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, false).webhookOperationConfigs["webhookConfigs"].isEmpty)
    Assertions.assertEquals(WEBHOOK_OP, operationService!!.getStandardSyncOperation(WEBHOOK_OP.operationId))
  }

  @Test
  fun `concurrent deletion cannot strand a newly saved transformation`() {
    operationService!!.deleteStandardSyncOperation(WEBHOOK_OP.operationId)
    val originalWorkspace = workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, false)
    val workspace = Jsons.clone(originalWorkspace).withWebhookOperationConfigs(Jsons.deserialize("""{"webhookConfigs":[]}"""))
    val removalSaved = CountDownLatch(1)
    val allowCommit = CountDownLatch(1)
    val attachmentStarted = CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(2)
    try {
      val removal =
        executor.submit {
          database!!.transaction { ctx ->
            workspaceService.writeStandardWorkspaceNoSecrets(ctx, workspace)
            removalSaved.countDown()
            Assertions.assertTrue(allowCommit.await(10, TimeUnit.SECONDS))
          }
        }
      Assertions.assertTrue(removalSaved.await(10, TimeUnit.SECONDS))
      val attachment =
        executor.submit<Boolean> {
          attachmentStarted.countDown()
          try {
            operationService!!.writeStandardSyncOperation(WEBHOOK_OP)
            true
          } catch (_: WebhookConfigNotFoundException) {
            false
          }
        }
      Assertions.assertTrue(attachmentStarted.await(10, TimeUnit.SECONDS))
      Assertions.assertThrows(TimeoutException::class.java) { attachment.get(250, TimeUnit.MILLISECONDS) }
      allowCommit.countDown()
      removal.get(10, TimeUnit.SECONDS)
      Assertions.assertFalse(attachment.get(10, TimeUnit.SECONDS), "Saving a reference must fail after deletion commits")
      Assertions.assertTrue(operationService!!.getStandardSyncOperation(WEBHOOK_OP.operationId).tombstone)
      Assertions.assertTrue(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, false).webhookOperationConfigs["webhookConfigs"].isEmpty)
    } finally {
      allowCommit.countDown()
      executor.shutdownNow()
    }
  }

  private fun createWorkspace() {
    val featureFlagClient: FeatureFlagClient = mockk<TestClient>(relaxed = true)
    val secretsRepositoryReader = mockk<SecretsRepositoryReader>(relaxed = true)
    val secretsRepositoryWriter = mockk<SecretsRepositoryWriter>(relaxed = true)
    val secretPersistenceConfigService = mockk<SecretPersistenceConfigService>(relaxed = true)
    val metricClient = mockk<MetricClient>(relaxed = true)

    OrganizationServiceJooqImpl(database).writeOrganization(MockData.defaultOrganization())

    val dataplaneGroupService: DataplaneGroupService = DataplaneGroupServiceTestJooqImpl(database!!)
    dataplaneGroupService.writeDataplaneGroup(
      DataplaneGroup()
        .withId(UUID.randomUUID())
        .withOrganizationId(MockData.defaultOrganization().organizationId)
        .withName("test")
        .withEnabled(true)
        .withTombstone(false),
    )

    val workspace =
      StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withName("Another Workspace")
        .withSlug("another-workspace")
        .withInitialSetupComplete(true)
        .withTombstone(false)
        .withDataplaneGroupId(UUID.randomUUID())
        .withOrganizationId(MockData.defaultOrganization().organizationId)
        .withWebhookOperationConfigs(Jsons.deserialize("""{"webhookConfigs":[{"id":"$WEBHOOK_CONFIG_ID","name":"dbt cloud"}]}"""))
    workspaceService =
      WorkspaceServiceJooqImpl(
        database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        metricClient,
        transactions = configTransactions,
      )
    workspaceService.writeStandardWorkspaceNoSecrets(workspace)
  }

  companion object {
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val WEBHOOK_CONFIG_ID: UUID = UUID.randomUUID()
    private const val WEBHOOK_OPERATION_EXECUTION_URL = "test-webhook-url"
    private const val WEBHOOK_OPERATION_EXECUTION_BODY = "test-webhook-body"

    private val WEBHOOK_OP: StandardSyncOperation =
      StandardSyncOperation()
        .withName("webhook-operation")
        .withTombstone(false)
        .withOperationId(UUID.randomUUID())
        .withWorkspaceId(WORKSPACE_ID)
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK)
        .withOperatorWebhook(
          OperatorWebhook()
            .withWebhookConfigId(WEBHOOK_CONFIG_ID)
            .withExecutionUrl(WEBHOOK_OPERATION_EXECUTION_URL)
            .withExecutionBody(WEBHOOK_OPERATION_EXECUTION_BODY),
        )
    private val OPS: List<StandardSyncOperation> = listOf(WEBHOOK_OP)
  }
}
