/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.OperationCreate
import io.airbyte.api.server.generated.models.OperationIdRequestBody
import io.airbyte.api.server.generated.models.OperationRead
import io.airbyte.api.server.generated.models.OperationUpdate
import io.airbyte.api.server.generated.models.OperatorConfiguration
import io.airbyte.api.server.generated.models.OperatorType
import io.airbyte.api.server.generated.models.OperatorWebhookDbtCloud
import io.airbyte.commons.enums.isCompatible
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.json.Jsons.serialize
import io.airbyte.config.OperatorWebhook
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.WebhookConfig
import io.airbyte.config.WebhookOperationConfigs
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.WorkspaceService
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.function.Supplier

internal class OperationsHandlerTest {
  lateinit var workspaceService: WorkspaceService
  lateinit var uuidGenerator: Supplier<UUID>
  lateinit var operationsHandler: OperationsHandler
  lateinit var standardSyncOperation: StandardSyncOperation
  lateinit var operatorWebhook: OperatorWebhook
  lateinit var operationService: OperationService
  lateinit var connectionService: ConnectionService

  @BeforeEach
  fun setUp() {
    workspaceService = mockk<WorkspaceService>()
    operationService = mockk<OperationService>()
    connectionService = mockk<ConnectionService>()
    uuidGenerator = mockk<Supplier<UUID>>()

    operationsHandler = OperationsHandler(workspaceService, uuidGenerator, connectionService, operationService)
    operatorWebhook =
      OperatorWebhook()
        .withWebhookConfigId(WEBHOOK_CONFIG_ID)
        .withExecutionBody(
          serialize<OperatorWebhookDbtCloud?>(
            OperatorWebhookDbtCloud(accountId = DBT_CLOUD_WEBHOOK_ACCOUNT_ID, jobId = DBT_CLOUD_WEBHOOK_JOB_ID),
          ),
        ).withExecutionUrl(String.format(EXECUTION_URL_TEMPLATE, DBT_CLOUD_WEBHOOK_ACCOUNT_ID, DBT_CLOUD_WEBHOOK_JOB_ID))
    standardSyncOperation =
      StandardSyncOperation()
        .withWorkspaceId(UUID.randomUUID())
        .withOperationId(UUID.randomUUID())
        .withName("presto to hudi")
        .withTombstone(false)
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK)
        .withOperatorWebhook(operatorWebhook)
  }

  @Test
  fun testCreateWebhookOperation() {
    every { uuidGenerator.get() } returns WEBHOOK_OPERATION_ID
    val webhookConfig =
      io.airbyte.api.server.generated.models.OperatorWebhook(
        webhookConfigId = WEBHOOK_CONFIG_ID,
        webhookType = io.airbyte.api.server.generated.models.OperatorWebhook.WebhookType.DBT_CLOUD,
        dbtCloud = OperatorWebhookDbtCloud(accountId = DBT_CLOUD_WEBHOOK_ACCOUNT_ID, jobId = DBT_CLOUD_WEBHOOK_JOB_ID),
      )
    val operationCreate =
      OperationCreate(
        workspaceId = standardSyncOperation.workspaceId,
        name = WEBHOOK_OPERATION_NAME,
        operatorConfiguration = OperatorConfiguration(operatorType = OperatorType.WEBHOOK, webhook = webhookConfig),
      )

    val webhookOperationConfig =
      jsonNode<WebhookOperationConfigs?>(
        WebhookOperationConfigs().withWebhookConfigs(
          listOf<WebhookConfig?>(
            WebhookConfig().withCustomDbtHost(
              "",
            ),
          ),
        ),
      )

    val expectedPersistedOperation =
      StandardSyncOperation()
        .withWorkspaceId(standardSyncOperation.workspaceId)
        .withOperationId(WEBHOOK_OPERATION_ID)
        .withName(WEBHOOK_OPERATION_NAME)
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK)
        .withOperatorWebhook(
          OperatorWebhook()
            .withWebhookConfigId(WEBHOOK_CONFIG_ID)
            .withExecutionUrl(
              String.format(
                EXECUTION_URL_TEMPLATE,
                DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
                DBT_CLOUD_WEBHOOK_JOB_ID,
              ),
            ).withExecutionBody(EXECUTION_BODY),
        ).withTombstone(false)

    val workspace = StandardWorkspace().withWebhookOperationConfigs(webhookOperationConfig)
    every { workspaceService.getWorkspaceWithSecrets(operationCreate.workspaceId, false) } returns workspace
    every { operationService.getStandardSyncOperation(WEBHOOK_OPERATION_ID) } returns expectedPersistedOperation

    every { operationService.writeStandardSyncOperation(any()) } just Runs

    val actualOperationRead = operationsHandler.createOperation(operationCreate)

    Assertions.assertEquals(operationCreate.workspaceId, actualOperationRead.workspaceId)
    Assertions.assertEquals(WEBHOOK_OPERATION_ID, actualOperationRead.operationId)
    Assertions.assertEquals(WEBHOOK_OPERATION_NAME, actualOperationRead.name)
    Assertions.assertEquals(OperatorType.WEBHOOK, actualOperationRead.operatorConfiguration.operatorType)

    // NOTE: we expect the server to dual-write on read until the frontend moves to the new format.
    val expectedWebhookConfigRead =
      webhookConfig.copy(
        executionUrl =
          String.format(
            EXECUTION_URL_TEMPLATE,
            DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
            DBT_CLOUD_WEBHOOK_JOB_ID,
          ),
        executionBody = EXECUTION_BODY,
      )
    Assertions.assertEquals(expectedWebhookConfigRead, actualOperationRead.operatorConfiguration.webhook)

    verify { operationService.writeStandardSyncOperation(expectedPersistedOperation) }
  }

  @Test
  fun testUpdateWebhookOperation() {
    every { uuidGenerator.get() } returns WEBHOOK_OPERATION_ID
    val webhookConfig =
      io.airbyte.api.server.generated.models.OperatorWebhook(
        webhookConfigId = WEBHOOK_CONFIG_ID,
        webhookType = io.airbyte.api.server.generated.models.OperatorWebhook.WebhookType.DBT_CLOUD,
        dbtCloud =
          OperatorWebhookDbtCloud(
            accountId = NEW_DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
            jobId = DBT_CLOUD_WEBHOOK_JOB_ID,
          ),
      )
    val operationUpdate =
      OperationUpdate(
        name = WEBHOOK_OPERATION_NAME,
        operationId = WEBHOOK_OPERATION_ID,
        operatorConfiguration = OperatorConfiguration(operatorType = OperatorType.WEBHOOK, webhook = webhookConfig),
      )

    val persistedWebhook =
      OperatorWebhook()
        .withWebhookConfigId(WEBHOOK_CONFIG_ID)
        .withExecutionUrl(
          String.format(
            EXECUTION_URL_TEMPLATE,
            DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
            DBT_CLOUD_WEBHOOK_JOB_ID,
          ),
        ).withExecutionBody(EXECUTION_BODY)

    val updatedWebhook =
      OperatorWebhook()
        .withWebhookConfigId(WEBHOOK_CONFIG_ID)
        .withExecutionUrl(
          String.format(
            EXECUTION_URL_TEMPLATE,
            NEW_DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
            DBT_CLOUD_WEBHOOK_JOB_ID,
          ),
        ).withExecutionBody(EXECUTION_BODY)

    val persistedOperation =
      StandardSyncOperation()
        .withWorkspaceId(standardSyncOperation.workspaceId)
        .withOperationId(WEBHOOK_OPERATION_ID)
        .withName(WEBHOOK_OPERATION_NAME)
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK)
        .withOperatorWebhook(persistedWebhook)

    val updatedOperation =
      StandardSyncOperation()
        .withWorkspaceId(standardSyncOperation.workspaceId)
        .withOperationId(WEBHOOK_OPERATION_ID)
        .withName(WEBHOOK_OPERATION_NAME)
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK)
        .withOperatorWebhook(updatedWebhook)

    val workspace = StandardWorkspace()
    every { workspaceService.getWorkspaceWithSecrets(standardSyncOperation.workspaceId, false) } returns workspace
    every { operationService.getStandardSyncOperation(WEBHOOK_OPERATION_ID) } returnsMany listOf(persistedOperation, updatedOperation)

    every { operationService.writeStandardSyncOperation(any()) } just Runs

    val actualOperationRead = operationsHandler.updateOperation(operationUpdate)

    Assertions.assertEquals(WEBHOOK_OPERATION_ID, actualOperationRead.operationId)
    Assertions.assertEquals(WEBHOOK_OPERATION_NAME, actualOperationRead.name)
    Assertions.assertEquals(OperatorType.WEBHOOK, actualOperationRead.operatorConfiguration.operatorType)
    val expectedWebhookConfigRead =
      webhookConfig.copy(
        executionUrl =
          String.format(
            EXECUTION_URL_TEMPLATE,
            NEW_DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
            DBT_CLOUD_WEBHOOK_JOB_ID,
          ),
        executionBody = EXECUTION_BODY,
      )
    Assertions.assertEquals(expectedWebhookConfigRead, actualOperationRead.operatorConfiguration.webhook)

    verify {
      operationService.writeStandardSyncOperation(
        persistedOperation.withOperatorWebhook(
          persistedOperation.operatorWebhook.withExecutionUrl(
            String.format(
              EXECUTION_URL_TEMPLATE,
              NEW_DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
              DBT_CLOUD_WEBHOOK_JOB_ID,
            ),
          ),
        ),
      )
    }
  }

  @Test
  fun testGetOperation() {
    every { operationService.getStandardSyncOperation(standardSyncOperation.operationId) } returns standardSyncOperation

    val operationIdRequestBody = OperationIdRequestBody(operationId = standardSyncOperation.operationId)
    val actualOperationRead = operationsHandler.getOperation(operationIdRequestBody)

    val expectedOperationRead = generateOperationRead()

    Assertions.assertEquals(expectedOperationRead, actualOperationRead)
  }

  private fun generateOperationRead(): OperationRead =
    OperationRead(
      workspaceId = standardSyncOperation.workspaceId,
      operationId = standardSyncOperation.operationId,
      name = standardSyncOperation.name,
      operatorConfiguration =
        OperatorConfiguration(
          operatorType = OperatorType.WEBHOOK,
          webhook =
            io.airbyte.api.server.generated.models.OperatorWebhook(
              webhookConfigId = WEBHOOK_CONFIG_ID,
              webhookType = io.airbyte.api.server.generated.models.OperatorWebhook.WebhookType.DBT_CLOUD,
              executionUrl = operatorWebhook.executionUrl,
              executionBody = operatorWebhook.executionBody,
              dbtCloud = OperatorWebhookDbtCloud(accountId = DBT_CLOUD_WEBHOOK_ACCOUNT_ID, jobId = DBT_CLOUD_WEBHOOK_JOB_ID),
            ),
        ),
    )

  @Test
  fun testListOperationsForConnection() {
    val connectionId = UUID.randomUUID()

    every { connectionService.getStandardSync(connectionId) } returns
      StandardSync()
        .withOperationIds(listOf<UUID?>(standardSyncOperation.operationId))

    every { operationService.getStandardSyncOperation(standardSyncOperation.operationId) } returns standardSyncOperation

    every { operationService.listStandardSyncOperations() } returns listOf(standardSyncOperation)

    val connectionIdRequestBody = ConnectionIdRequestBody(connectionId = connectionId)
    val actualOperationReadList = operationsHandler.listOperationsForConnection(connectionIdRequestBody)

    Assertions.assertEquals(generateOperationRead(), actualOperationReadList.operations[0])
  }

  @Test
  fun testDeleteOperation() {
    val operationIdRequestBody = OperationIdRequestBody(operationId = standardSyncOperation.operationId)

    val spiedOperationsHandler = spyk(operationsHandler)
    every { operationService.deleteStandardSyncOperation(any()) } just Runs

    spiedOperationsHandler.deleteOperation(operationIdRequestBody)

    verify { operationService.deleteStandardSyncOperation(standardSyncOperation.operationId) }
  }

  @Test
  fun testDeleteOperationsForConnection() {
    val syncConnectionId = UUID.randomUUID()
    val otherConnectionId = UUID.randomUUID()
    val operationId = UUID.randomUUID()
    val remainingOperationId = UUID.randomUUID()
    val toDelete = mutableListOf(standardSyncOperation.operationId, operationId)
    val sync =
      StandardSync()
        .withConnectionId(syncConnectionId)
        .withOperationIds(listOf<UUID?>(standardSyncOperation.operationId, operationId, remainingOperationId))
    every { connectionService.listStandardSyncs() } returns
      listOf<StandardSync>(
        sync,
        StandardSync()
          .withConnectionId(otherConnectionId)
          .withOperationIds(listOf<UUID?>(standardSyncOperation.operationId)),
      )
    // removing an operation builds its read, which needs the schema-required fields (operatorType, name, ...)
    val operation = Jsons.clone(standardSyncOperation).withOperationId(operationId)
    val remainingOperation = Jsons.clone(standardSyncOperation).withOperationId(remainingOperationId)
    every { operationService.getStandardSyncOperation(operationId) } returns operation
    every { operationService.getStandardSyncOperation(remainingOperationId) } returns remainingOperation
    every { operationService.getStandardSyncOperation(standardSyncOperation.operationId) } returns standardSyncOperation

    every { operationService.writeStandardSyncOperation(any()) } just Runs
    every { operationService.updateConnectionOperationIds(any(), any()) } just Runs
    every { connectionService.listStandardSyncsUsingOperation(any()) } returns emptyList()

    // first, test that a remaining operation results in proper call
    operationsHandler.deleteOperationsForConnection(sync, toDelete)
    verify { operationService.writeStandardSyncOperation(operation.withTombstone(true)) }
    verify { operationService.updateConnectionOperationIds(syncConnectionId, mutableSetOf<UUID>(remainingOperationId)) }

    // next, test that removing all operations results in proper call
    toDelete.add(remainingOperationId)
    operationsHandler.deleteOperationsForConnection(sync, toDelete)
    verify { operationService.updateConnectionOperationIds(syncConnectionId, mutableSetOf()) }
  }

  @Test
  fun testEnumCompatibility() {
    Assertions.assertTrue(isCompatible<StandardSyncOperation.OperatorType, OperatorType>())
  }

  @Test
  fun testDbtCloudRegex() {
    // Validate that a non-url is rejected.
    Assertions.assertThrows(IllegalArgumentException::class.java) { checkDbtCloudUrl("not-a-url") }
    // Validate that the URL is anchored to the beginning.
    Assertions.assertThrows(
      IllegalArgumentException::class.java,
    ) {
      checkDbtCloudUrl(
        "some-nonsense-" +
          String.format(
            EXECUTION_URL_TEMPLATE,
            DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
            DBT_CLOUD_WEBHOOK_JOB_ID,
          ),
      )
    }
    // Validate that the URL is anchored to the end.
    Assertions.assertThrows(
      IllegalArgumentException::class.java,
    ) {
      checkDbtCloudUrl(
        String.format(
          EXECUTION_URL_TEMPLATE,
          DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
          DBT_CLOUD_WEBHOOK_JOB_ID,
        ) + "-some-nonsense",
      )
    }
    // Validate that the account id must be an integer.
    Assertions.assertThrows(
      IllegalArgumentException::class.java,
    ) { checkDbtCloudUrl("https://cloud.getdbt.com/api/v2/accounts/abc/jobs/123/run/") }
    // Validate that the job id must be an integer.
    Assertions.assertThrows(
      IllegalArgumentException::class.java,
    ) { checkDbtCloudUrl("https://cloud.getdbt.com/api/v2/accounts/123/jobs/abc/run/") }
  }

  private fun checkDbtCloudUrl(urlToCheck: String?) {
    val persistedOperation =
      StandardSyncOperation()
        .withWorkspaceId(standardSyncOperation.workspaceId)
        .withOperationId(WEBHOOK_OPERATION_ID)
        .withName(WEBHOOK_OPERATION_NAME)
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK)
        .withOperatorWebhook(
          OperatorWebhook()
            .withWebhookConfigId(WEBHOOK_CONFIG_ID)
            .withExecutionUrl(urlToCheck)
            .withExecutionBody(EXECUTION_BODY),
        ).withTombstone(false)
    every { operationService.getStandardSyncOperation(WEBHOOK_OPERATION_ID) } returns persistedOperation

    val operationIdRequestBody = OperationIdRequestBody(operationId = WEBHOOK_OPERATION_ID)
    operationsHandler.getOperation(operationIdRequestBody)
  }

  companion object {
    private const val WEBHOOK_OPERATION_NAME = "fake-operation-name"
    private val WEBHOOK_CONFIG_ID: UUID = UUID.randomUUID()
    private val WEBHOOK_OPERATION_ID: UUID = UUID.randomUUID()
    private const val DBT_CLOUD_WEBHOOK_ACCOUNT_ID = 123L
    private const val DBT_CLOUD_WEBHOOK_JOB_ID = 456L
    private const val NEW_DBT_CLOUD_WEBHOOK_ACCOUNT_ID = 789L
    const val EXECUTION_BODY: String = "{\"cause\": \"airbyte\"}"
    const val EXECUTION_URL_TEMPLATE: String = "https://cloud.getdbt.com/api/v2/accounts/%d/jobs/%d/run/"
  }
}
