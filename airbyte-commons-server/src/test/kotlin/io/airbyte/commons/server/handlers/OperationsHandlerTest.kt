/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.OperationCreate
import io.airbyte.api.model.generated.OperationIdRequestBody
import io.airbyte.api.model.generated.OperationRead
import io.airbyte.api.model.generated.OperationUpdate
import io.airbyte.api.model.generated.OperatorConfiguration
import io.airbyte.api.model.generated.OperatorType
import io.airbyte.api.model.generated.OperatorWebhook.WebhookTypeEnum
import io.airbyte.api.model.generated.OperatorWebhookDbtCloud
import io.airbyte.commons.enums.isCompatible
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.json.Jsons.serialize
import io.airbyte.config.OperatorWebhook
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.WebhookConfig
import io.airbyte.config.WebhookOperationConfigs
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.mockito.Mockito
import java.io.IOException
import java.util.UUID
import java.util.function.Supplier
import java.util.stream.Collectors
import java.util.stream.Stream

internal class OperationsHandlerTest {
  lateinit var workspaceService: WorkspaceService
  lateinit var uuidGenerator: Supplier<UUID>
  lateinit var operationsHandler: OperationsHandler
  lateinit var standardSyncOperation: StandardSyncOperation
  lateinit var operatorWebhook: OperatorWebhook
  lateinit var operationService: OperationService
  lateinit var connectionService: ConnectionService

  @BeforeEach
  @Throws(IOException::class)
  fun setUp() {
    workspaceService = Mockito.mock(WorkspaceService::class.java)
    operationService = Mockito.mock(OperationService::class.java)
    connectionService = Mockito.mock(ConnectionService::class.java)
    uuidGenerator = Mockito.mock(Supplier::class.java) as Supplier<UUID>

    operationsHandler = OperationsHandler(workspaceService, uuidGenerator, connectionService, operationService)
    operatorWebhook =
      OperatorWebhook()
        .withWebhookConfigId(WEBHOOK_CONFIG_ID)
        .withExecutionBody(
          serialize<OperatorWebhookDbtCloud?>(
            OperatorWebhookDbtCloud().accountId(DBT_CLOUD_WEBHOOK_ACCOUNT_ID).jobId(
              DBT_CLOUD_WEBHOOK_JOB_ID,
            ),
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
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, ConfigNotFoundException::class)
  fun testCreateWebhookOperation() {
    Mockito.`when`<UUID?>(uuidGenerator.get()).thenReturn(WEBHOOK_OPERATION_ID)
    val webhookConfig =
      io.airbyte.api.model.generated
        .OperatorWebhook()
        .webhookConfigId(WEBHOOK_CONFIG_ID)
        .webhookType(WebhookTypeEnum.DBT_CLOUD)
        .dbtCloud(
          OperatorWebhookDbtCloud()
            .accountId(DBT_CLOUD_WEBHOOK_ACCOUNT_ID)
            .jobId(DBT_CLOUD_WEBHOOK_JOB_ID),
        )
    val operationCreate =
      OperationCreate()
        .workspaceId(standardSyncOperation.getWorkspaceId())
        .name(WEBHOOK_OPERATION_NAME)
        .operatorConfiguration(
          OperatorConfiguration()
            .operatorType(OperatorType.WEBHOOK)
            .webhook(webhookConfig),
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
        .withWorkspaceId(standardSyncOperation.getWorkspaceId())
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
    Mockito.`when`<StandardWorkspace?>(workspaceService.getWorkspaceWithSecrets(operationCreate.getWorkspaceId(), false)).thenReturn(workspace)
    Mockito
      .`when`<StandardSyncOperation?>(operationService.getStandardSyncOperation(WEBHOOK_OPERATION_ID))
      .thenReturn(expectedPersistedOperation)

    val actualOperationRead = operationsHandler.createOperation(operationCreate)

    Assertions.assertEquals(operationCreate.getWorkspaceId(), actualOperationRead.getWorkspaceId())
    Assertions.assertEquals(WEBHOOK_OPERATION_ID, actualOperationRead.getOperationId())
    Assertions.assertEquals(WEBHOOK_OPERATION_NAME, actualOperationRead.getName())
    Assertions.assertEquals(OperatorType.WEBHOOK, actualOperationRead.getOperatorConfiguration().getOperatorType())

    // NOTE: we expect the server to dual-write on read until the frontend moves to the new format.
    val expectedWebhookConfigRead =
      webhookConfig
        .executionUrl(
          String.format(
            EXECUTION_URL_TEMPLATE,
            DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
            DBT_CLOUD_WEBHOOK_JOB_ID,
          ),
        ).executionBody(EXECUTION_BODY)
    Assertions.assertEquals(expectedWebhookConfigRead, actualOperationRead.getOperatorConfiguration().getWebhook())

    Mockito
      .verify(operationService)
      .writeStandardSyncOperation(expectedPersistedOperation)
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, ConfigNotFoundException::class)
  fun testUpdateWebhookOperation() {
    Mockito.`when`(uuidGenerator.get()).thenReturn(WEBHOOK_OPERATION_ID)
    val webhookConfig =
      io.airbyte.api.model.generated
        .OperatorWebhook()
        .webhookConfigId(WEBHOOK_CONFIG_ID)
        .webhookType(WebhookTypeEnum.DBT_CLOUD)
        .dbtCloud(
          OperatorWebhookDbtCloud()
            .accountId(NEW_DBT_CLOUD_WEBHOOK_ACCOUNT_ID)
            .jobId(DBT_CLOUD_WEBHOOK_JOB_ID),
        )
    val operationUpdate =
      OperationUpdate()
        .name(WEBHOOK_OPERATION_NAME)
        .operationId(WEBHOOK_OPERATION_ID)
        .operatorConfiguration(
          OperatorConfiguration()
            .operatorType(OperatorType.WEBHOOK)
            .webhook(webhookConfig),
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
        .withWorkspaceId(standardSyncOperation.getWorkspaceId())
        .withOperationId(WEBHOOK_OPERATION_ID)
        .withName(WEBHOOK_OPERATION_NAME)
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK)
        .withOperatorWebhook(persistedWebhook)

    val updatedOperation =
      StandardSyncOperation()
        .withWorkspaceId(standardSyncOperation.getWorkspaceId())
        .withOperationId(WEBHOOK_OPERATION_ID)
        .withName(WEBHOOK_OPERATION_NAME)
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK)
        .withOperatorWebhook(updatedWebhook)

    val workspace = StandardWorkspace()
    Mockito
      .`when`(workspaceService.getWorkspaceWithSecrets(standardSyncOperation.getWorkspaceId(), false))
      .thenReturn(workspace)
    Mockito
      .`when`(operationService.getStandardSyncOperation(WEBHOOK_OPERATION_ID))
      .thenReturn(persistedOperation)
      .thenReturn(updatedOperation)

    val actualOperationRead = operationsHandler.updateOperation(operationUpdate)

    Assertions.assertEquals(WEBHOOK_OPERATION_ID, actualOperationRead.getOperationId())
    Assertions.assertEquals(WEBHOOK_OPERATION_NAME, actualOperationRead.getName())
    Assertions.assertEquals(OperatorType.WEBHOOK, actualOperationRead.getOperatorConfiguration().getOperatorType())
    val expectedWebhookConfigRead =
      webhookConfig
        .executionUrl(
          String.format(
            EXECUTION_URL_TEMPLATE,
            NEW_DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
            DBT_CLOUD_WEBHOOK_JOB_ID,
          ),
        ).executionBody(EXECUTION_BODY)
    Assertions.assertEquals(expectedWebhookConfigRead, actualOperationRead.getOperatorConfiguration().getWebhook())

    Mockito
      .verify(operationService)
      .writeStandardSyncOperation(
        persistedOperation.withOperatorWebhook(
          persistedOperation.getOperatorWebhook().withExecutionUrl(
            String.format(
              EXECUTION_URL_TEMPLATE,
              NEW_DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
              DBT_CLOUD_WEBHOOK_JOB_ID,
            ),
          ),
        ),
      )
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testGetOperation() {
    Mockito
      .`when`(operationService.getStandardSyncOperation(standardSyncOperation.getOperationId()))
      .thenReturn(standardSyncOperation)

    val operationIdRequestBody = OperationIdRequestBody().operationId(standardSyncOperation.getOperationId())
    val actualOperationRead = operationsHandler.getOperation(operationIdRequestBody)

    val expectedOperationRead = generateOperationRead()

    Assertions.assertEquals(expectedOperationRead, actualOperationRead)
  }

  private fun generateOperationRead(): OperationRead? =
    OperationRead()
      .workspaceId(standardSyncOperation.getWorkspaceId())
      .operationId(standardSyncOperation.getOperationId())
      .name(standardSyncOperation.getName())
      .operatorConfiguration(
        OperatorConfiguration()
          .operatorType(OperatorType.WEBHOOK)
          .webhook(
            io.airbyte.api.model.generated
              .OperatorWebhook()
              .webhookConfigId(WEBHOOK_CONFIG_ID)
              .webhookType(WebhookTypeEnum.DBT_CLOUD)
              .executionUrl(operatorWebhook.getExecutionUrl())
              .executionBody(operatorWebhook.getExecutionBody())
              .dbtCloud(
                OperatorWebhookDbtCloud()
                  .accountId(DBT_CLOUD_WEBHOOK_ACCOUNT_ID)
                  .jobId(DBT_CLOUD_WEBHOOK_JOB_ID),
              ),
          ),
      )

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testListOperationsForConnection() {
    val connectionId = UUID.randomUUID()

    Mockito
      .`when`<StandardSync?>(connectionService.getStandardSync(connectionId))
      .thenReturn(
        StandardSync()
          .withOperationIds(listOf<UUID?>(standardSyncOperation.getOperationId())),
      )

    Mockito
      .`when`<StandardSyncOperation?>(operationService.getStandardSyncOperation(standardSyncOperation.getOperationId()))
      .thenReturn(standardSyncOperation)

    Mockito
      .`when`<List<StandardSyncOperation>?>(operationService.listStandardSyncOperations())
      .thenReturn(listOf<StandardSyncOperation>(standardSyncOperation))

    val connectionIdRequestBody = ConnectionIdRequestBody().connectionId(connectionId)
    val actualOperationReadList = operationsHandler.listOperationsForConnection(connectionIdRequestBody)

    Assertions.assertEquals(generateOperationRead(), actualOperationReadList.getOperations().get(0))
  }

  @Test
  @Throws(IOException::class)
  fun testDeleteOperation() {
    val operationIdRequestBody = OperationIdRequestBody().operationId(standardSyncOperation.getOperationId())

    val spiedOperationsHandler = Mockito.spy(operationsHandler)

    spiedOperationsHandler.deleteOperation(operationIdRequestBody)

    Mockito.verify(operationService).deleteStandardSyncOperation(standardSyncOperation.getOperationId())
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testDeleteOperationsForConnection() {
    val syncConnectionId = UUID.randomUUID()
    val otherConnectionId = UUID.randomUUID()
    val operationId = UUID.randomUUID()
    val remainingOperationId = UUID.randomUUID()
    val toDelete = Stream.of(standardSyncOperation.getOperationId(), operationId).collect(Collectors.toList())
    val sync =
      StandardSync()
        .withConnectionId(syncConnectionId)
        .withOperationIds(listOf<UUID?>(standardSyncOperation.getOperationId(), operationId, remainingOperationId))
    Mockito.`when`<List<StandardSync>?>(connectionService.listStandardSyncs()).thenReturn(
      listOf<StandardSync>(
        sync,
        StandardSync()
          .withConnectionId(otherConnectionId)
          .withOperationIds(listOf<UUID?>(standardSyncOperation.getOperationId())),
      ),
    )
    val operation = StandardSyncOperation().withOperationId(operationId)
    val remainingOperation = StandardSyncOperation().withOperationId(remainingOperationId)
    Mockito.`when`<StandardSyncOperation?>(operationService.getStandardSyncOperation(operationId)).thenReturn(operation)
    Mockito.`when`<StandardSyncOperation?>(operationService.getStandardSyncOperation(remainingOperationId)).thenReturn(remainingOperation)
    Mockito
      .`when`<StandardSyncOperation?>(operationService.getStandardSyncOperation(standardSyncOperation.getOperationId()))
      .thenReturn(standardSyncOperation)

    // first, test that a remaining operation results in proper call
    operationsHandler.deleteOperationsForConnection(sync, toDelete)
    Mockito.verify(operationService).writeStandardSyncOperation(operation.withTombstone(true))
    Mockito.verify(operationService).updateConnectionOperationIds(syncConnectionId, mutableSetOf<UUID>(remainingOperationId))

    // next, test that removing all operations results in proper call
    toDelete.add(remainingOperationId)
    operationsHandler.deleteOperationsForConnection(sync, toDelete)
    Mockito.verify(operationService).updateConnectionOperationIds(syncConnectionId, mutableSetOf<UUID>())
  }

  @Test
  fun testEnumCompatibility() {
    Assertions.assertTrue(isCompatible<StandardSyncOperation.OperatorType, OperatorType>())
  }

  @Test
  fun testDbtCloudRegex() {
    // Validate that a non-url is rejected.
    Assertions.assertThrows(IllegalArgumentException::class.java, Executable { checkDbtCloudUrl("not-a-url") })
    // Validate that the URL is anchored to the beginning.
    Assertions.assertThrows(
      IllegalArgumentException::class.java,
      Executable {
        checkDbtCloudUrl(
          "some-nonsense-" +
            String.format(
              EXECUTION_URL_TEMPLATE,
              DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
              DBT_CLOUD_WEBHOOK_JOB_ID,
            ),
        )
      },
    )
    // Validate that the URL is anchored to the end.
    Assertions.assertThrows(
      IllegalArgumentException::class.java,
      Executable {
        checkDbtCloudUrl(
          String.format(
            EXECUTION_URL_TEMPLATE,
            DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
            DBT_CLOUD_WEBHOOK_JOB_ID,
          ) + "-some-nonsense",
        )
      },
    )
    // Validate that the account id must be an integer.
    Assertions.assertThrows(
      IllegalArgumentException::class.java,
      Executable { checkDbtCloudUrl("https://cloud.getdbt.com/api/v2/accounts/abc/jobs/123/run/") },
    )
    // Validate that the job id must be an integer.
    Assertions.assertThrows(
      IllegalArgumentException::class.java,
      Executable { checkDbtCloudUrl("https://cloud.getdbt.com/api/v2/accounts/123/jobs/abc/run/") },
    )
  }

  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  private fun checkDbtCloudUrl(urlToCheck: String?) {
    val persistedOperation =
      StandardSyncOperation()
        .withWorkspaceId(standardSyncOperation.getWorkspaceId())
        .withOperationId(WEBHOOK_OPERATION_ID)
        .withName(WEBHOOK_OPERATION_NAME)
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK)
        .withOperatorWebhook(
          OperatorWebhook()
            .withWebhookConfigId(WEBHOOK_CONFIG_ID)
            .withExecutionUrl(urlToCheck)
            .withExecutionBody(EXECUTION_BODY),
        ).withTombstone(false)
    Mockito.`when`(operationService.getStandardSyncOperation(WEBHOOK_OPERATION_ID)).thenReturn(persistedOperation)

    val operationIdRequestBody = OperationIdRequestBody().operationId(WEBHOOK_OPERATION_ID)
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
