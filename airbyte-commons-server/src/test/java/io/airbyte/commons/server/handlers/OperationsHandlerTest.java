/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.OperationCreate;
import io.airbyte.api.model.generated.OperationIdRequestBody;
import io.airbyte.api.model.generated.OperationRead;
import io.airbyte.api.model.generated.OperationReadList;
import io.airbyte.api.model.generated.OperationUpdate;
import io.airbyte.api.model.generated.OperatorConfiguration;
import io.airbyte.api.model.generated.OperatorType;
import io.airbyte.api.model.generated.OperatorWebhook;
import io.airbyte.api.model.generated.OperatorWebhook.WebhookTypeEnum;
import io.airbyte.api.model.generated.OperatorWebhookDbtCloud;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.WebhookConfig;
import io.airbyte.config.WebhookOperationConfigs;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.OperationService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OperationsHandlerTest {

  private static final String WEBHOOK_OPERATION_NAME = "fake-operation-name";
  private static final UUID WEBHOOK_CONFIG_ID = UUID.randomUUID();
  private static final UUID WEBHOOK_OPERATION_ID = UUID.randomUUID();
  private static final Long DBT_CLOUD_WEBHOOK_ACCOUNT_ID = 123L;
  private static final Long DBT_CLOUD_WEBHOOK_JOB_ID = 456L;
  private static final Long NEW_DBT_CLOUD_WEBHOOK_ACCOUNT_ID = 789L;
  public static final String EXECUTION_BODY = "{\"cause\": \"airbyte\"}";
  public static final String EXECUTION_URL_TEMPLATE = "https://cloud.getdbt.com/api/v2/accounts/%d/jobs/%d/run/";
  private WorkspaceService workspaceService;
  private Supplier<UUID> uuidGenerator;
  private OperationsHandler operationsHandler;
  private StandardSyncOperation standardSyncOperation;
  private io.airbyte.config.OperatorWebhook operatorWebhook;
  private OperationService operationService;
  private ConnectionService connectionService;

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() throws IOException {
    workspaceService = mock(WorkspaceService.class);
    operationService = mock(OperationService.class);
    connectionService = mock(ConnectionService.class);
    uuidGenerator = mock(Supplier.class);

    operationsHandler = new OperationsHandler(workspaceService, uuidGenerator, connectionService, operationService);
    operatorWebhook = new io.airbyte.config.OperatorWebhook()
        .withWebhookConfigId(WEBHOOK_CONFIG_ID)
        .withExecutionBody(Jsons.serialize(new OperatorWebhookDbtCloud().accountId(DBT_CLOUD_WEBHOOK_ACCOUNT_ID).jobId(DBT_CLOUD_WEBHOOK_JOB_ID)))
        .withExecutionUrl(String.format(EXECUTION_URL_TEMPLATE, DBT_CLOUD_WEBHOOK_ACCOUNT_ID, DBT_CLOUD_WEBHOOK_JOB_ID));
    standardSyncOperation = new StandardSyncOperation()
        .withWorkspaceId(UUID.randomUUID())
        .withOperationId(UUID.randomUUID())
        .withName("presto to hudi")
        .withTombstone(false)
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK)
        .withOperatorWebhook(operatorWebhook);
  }

  @Test
  void testCreateWebhookOperation()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    when(uuidGenerator.get()).thenReturn(WEBHOOK_OPERATION_ID);
    final OperatorWebhook webhookConfig = new OperatorWebhook()
        .webhookConfigId(WEBHOOK_CONFIG_ID)
        .webhookType(WebhookTypeEnum.DBT_CLOUD)
        .dbtCloud(new OperatorWebhookDbtCloud()
            .accountId(DBT_CLOUD_WEBHOOK_ACCOUNT_ID)
            .jobId(DBT_CLOUD_WEBHOOK_JOB_ID));
    final OperationCreate operationCreate = new OperationCreate()
        .workspaceId(standardSyncOperation.getWorkspaceId())
        .name(WEBHOOK_OPERATION_NAME)
        .operatorConfiguration(new OperatorConfiguration()
            .operatorType(OperatorType.WEBHOOK).webhook(webhookConfig));

    final JsonNode webhookOperationConfig =
        Jsons.jsonNode(new WebhookOperationConfigs().withWebhookConfigs(List.of(new WebhookConfig().withCustomDbtHost(""))));

    final StandardSyncOperation expectedPersistedOperation = new StandardSyncOperation()
        .withWorkspaceId(standardSyncOperation.getWorkspaceId())
        .withOperationId(WEBHOOK_OPERATION_ID)
        .withName(WEBHOOK_OPERATION_NAME)
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK)
        .withOperatorWebhook(new io.airbyte.config.OperatorWebhook()
            .withWebhookConfigId(WEBHOOK_CONFIG_ID)
            .withExecutionUrl(String.format(EXECUTION_URL_TEMPLATE, DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
                DBT_CLOUD_WEBHOOK_JOB_ID))
            .withExecutionBody(EXECUTION_BODY))
        .withTombstone(false);

    final StandardWorkspace workspace = new StandardWorkspace().withWebhookOperationConfigs(webhookOperationConfig);
    when(workspaceService.getWorkspaceWithSecrets(operationCreate.getWorkspaceId(), false)).thenReturn(workspace);
    when(operationService.getStandardSyncOperation(WEBHOOK_OPERATION_ID)).thenReturn(expectedPersistedOperation);

    final OperationRead actualOperationRead = operationsHandler.createOperation(operationCreate);

    assertEquals(operationCreate.getWorkspaceId(), actualOperationRead.getWorkspaceId());
    assertEquals(WEBHOOK_OPERATION_ID, actualOperationRead.getOperationId());
    assertEquals(WEBHOOK_OPERATION_NAME, actualOperationRead.getName());
    assertEquals(OperatorType.WEBHOOK, actualOperationRead.getOperatorConfiguration().getOperatorType());

    // NOTE: we expect the server to dual-write on read until the frontend moves to the new format.
    final OperatorWebhook expectedWebhookConfigRead =
        webhookConfig.executionUrl(String.format(EXECUTION_URL_TEMPLATE, DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
            DBT_CLOUD_WEBHOOK_JOB_ID)).executionBody(EXECUTION_BODY);
    assertEquals(expectedWebhookConfigRead, actualOperationRead.getOperatorConfiguration().getWebhook());

    verify(operationService).writeStandardSyncOperation(eq(expectedPersistedOperation));
  }

  @Test
  void testUpdateWebhookOperation()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    when(uuidGenerator.get()).thenReturn(WEBHOOK_OPERATION_ID);
    final OperatorWebhook webhookConfig = new OperatorWebhook()
        .webhookConfigId(WEBHOOK_CONFIG_ID)
        .webhookType(WebhookTypeEnum.DBT_CLOUD)
        .dbtCloud(new OperatorWebhookDbtCloud()
            .accountId(NEW_DBT_CLOUD_WEBHOOK_ACCOUNT_ID)
            .jobId(DBT_CLOUD_WEBHOOK_JOB_ID));
    final OperationUpdate operationUpdate = new OperationUpdate()
        .name(WEBHOOK_OPERATION_NAME)
        .operationId(WEBHOOK_OPERATION_ID)
        .operatorConfiguration(new OperatorConfiguration()
            .operatorType(OperatorType.WEBHOOK).webhook(webhookConfig));

    final var persistedWebhook = new io.airbyte.config.OperatorWebhook()
        .withWebhookConfigId(WEBHOOK_CONFIG_ID)
        .withExecutionUrl(String.format(EXECUTION_URL_TEMPLATE, DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
            DBT_CLOUD_WEBHOOK_JOB_ID))
        .withExecutionBody(EXECUTION_BODY);

    final var updatedWebhook = new io.airbyte.config.OperatorWebhook()
        .withWebhookConfigId(WEBHOOK_CONFIG_ID)
        .withExecutionUrl(String.format(EXECUTION_URL_TEMPLATE, NEW_DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
            DBT_CLOUD_WEBHOOK_JOB_ID))
        .withExecutionBody(EXECUTION_BODY);

    final StandardSyncOperation persistedOperation = new StandardSyncOperation()
        .withWorkspaceId(standardSyncOperation.getWorkspaceId())
        .withOperationId(WEBHOOK_OPERATION_ID)
        .withName(WEBHOOK_OPERATION_NAME)
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK)
        .withOperatorWebhook(persistedWebhook);

    final StandardSyncOperation updatedOperation = new StandardSyncOperation()
        .withWorkspaceId(standardSyncOperation.getWorkspaceId())
        .withOperationId(WEBHOOK_OPERATION_ID)
        .withName(WEBHOOK_OPERATION_NAME)
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK)
        .withOperatorWebhook(updatedWebhook);

    StandardWorkspace workspace = new StandardWorkspace();
    when(workspaceService.getWorkspaceWithSecrets(standardSyncOperation.getWorkspaceId(), false)).thenReturn(workspace);
    when(operationService.getStandardSyncOperation(WEBHOOK_OPERATION_ID)).thenReturn(persistedOperation).thenReturn(updatedOperation);

    final OperationRead actualOperationRead = operationsHandler.updateOperation(operationUpdate);

    assertEquals(WEBHOOK_OPERATION_ID, actualOperationRead.getOperationId());
    assertEquals(WEBHOOK_OPERATION_NAME, actualOperationRead.getName());
    assertEquals(OperatorType.WEBHOOK, actualOperationRead.getOperatorConfiguration().getOperatorType());
    final OperatorWebhook expectedWebhookConfigRead =
        webhookConfig.executionUrl(String.format(EXECUTION_URL_TEMPLATE, NEW_DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
            DBT_CLOUD_WEBHOOK_JOB_ID)).executionBody(EXECUTION_BODY);
    assertEquals(expectedWebhookConfigRead, actualOperationRead.getOperatorConfiguration().getWebhook());

    verify(operationService)
        .writeStandardSyncOperation(persistedOperation.withOperatorWebhook(persistedOperation.getOperatorWebhook().withExecutionUrl(
            String.format(EXECUTION_URL_TEMPLATE, NEW_DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
                DBT_CLOUD_WEBHOOK_JOB_ID))));
  }

  @Test
  void testGetOperation() throws JsonValidationException, ConfigNotFoundException, IOException {
    when(operationService.getStandardSyncOperation(standardSyncOperation.getOperationId())).thenReturn(standardSyncOperation);

    final OperationIdRequestBody operationIdRequestBody = new OperationIdRequestBody().operationId(standardSyncOperation.getOperationId());
    final OperationRead actualOperationRead = operationsHandler.getOperation(operationIdRequestBody);

    final OperationRead expectedOperationRead = generateOperationRead();

    assertEquals(expectedOperationRead, actualOperationRead);
  }

  private OperationRead generateOperationRead() {
    return new OperationRead()
        .workspaceId(standardSyncOperation.getWorkspaceId())
        .operationId(standardSyncOperation.getOperationId())
        .name(standardSyncOperation.getName())
        .operatorConfiguration(
            new OperatorConfiguration()
                .operatorType(OperatorType.WEBHOOK).webhook(
                    new OperatorWebhook()
                        .webhookConfigId(WEBHOOK_CONFIG_ID)
                        .webhookType(WebhookTypeEnum.DBT_CLOUD)
                        .executionUrl(operatorWebhook.getExecutionUrl())
                        .executionBody(operatorWebhook.getExecutionBody())
                        .dbtCloud(new OperatorWebhookDbtCloud()
                            .accountId(DBT_CLOUD_WEBHOOK_ACCOUNT_ID)
                            .jobId(DBT_CLOUD_WEBHOOK_JOB_ID))));
  }

  @Test
  void testListOperationsForConnection() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID connectionId = UUID.randomUUID();

    when(connectionService.getStandardSync(connectionId))
        .thenReturn(new StandardSync()
            .withOperationIds(List.of(standardSyncOperation.getOperationId())));

    when(operationService.getStandardSyncOperation(standardSyncOperation.getOperationId()))
        .thenReturn(standardSyncOperation);

    when(operationService.listStandardSyncOperations())
        .thenReturn(List.of(standardSyncOperation));

    final ConnectionIdRequestBody connectionIdRequestBody = new ConnectionIdRequestBody().connectionId(connectionId);
    final OperationReadList actualOperationReadList = operationsHandler.listOperationsForConnection(connectionIdRequestBody);

    assertEquals(generateOperationRead(), actualOperationReadList.getOperations().get(0));
  }

  @Test
  void testDeleteOperation() throws IOException {
    final OperationIdRequestBody operationIdRequestBody = new OperationIdRequestBody().operationId(standardSyncOperation.getOperationId());

    final OperationsHandler spiedOperationsHandler = spy(operationsHandler);

    spiedOperationsHandler.deleteOperation(operationIdRequestBody);

    verify(operationService).deleteStandardSyncOperation(standardSyncOperation.getOperationId());
  }

  @Test
  void testDeleteOperationsForConnection() throws JsonValidationException, IOException, ConfigNotFoundException {
    final UUID syncConnectionId = UUID.randomUUID();
    final UUID otherConnectionId = UUID.randomUUID();
    final UUID operationId = UUID.randomUUID();
    final UUID remainingOperationId = UUID.randomUUID();
    final List<UUID> toDelete = Stream.of(standardSyncOperation.getOperationId(), operationId).collect(Collectors.toList());
    final StandardSync sync = new StandardSync()
        .withConnectionId(syncConnectionId)
        .withOperationIds(List.of(standardSyncOperation.getOperationId(), operationId, remainingOperationId));
    when(connectionService.listStandardSyncs()).thenReturn(List.of(
        sync,
        new StandardSync()
            .withConnectionId(otherConnectionId)
            .withOperationIds(List.of(standardSyncOperation.getOperationId()))));
    final StandardSyncOperation operation = new StandardSyncOperation().withOperationId(operationId);
    final StandardSyncOperation remainingOperation = new StandardSyncOperation().withOperationId(remainingOperationId);
    when(operationService.getStandardSyncOperation(operationId)).thenReturn(operation);
    when(operationService.getStandardSyncOperation(remainingOperationId)).thenReturn(remainingOperation);
    when(operationService.getStandardSyncOperation(standardSyncOperation.getOperationId())).thenReturn(standardSyncOperation);

    // first, test that a remaining operation results in proper call
    operationsHandler.deleteOperationsForConnection(sync, toDelete);
    verify(operationService).writeStandardSyncOperation(operation.withTombstone(true));
    verify(operationService).updateConnectionOperationIds(syncConnectionId, Collections.singleton(remainingOperationId));

    // next, test that removing all operations results in proper call
    toDelete.add(remainingOperationId);
    operationsHandler.deleteOperationsForConnection(sync, toDelete);
    verify(operationService).updateConnectionOperationIds(syncConnectionId, Collections.emptySet());
  }

  @Test
  void testEnumConversion() {
    assertTrue(Enums.isCompatible(io.airbyte.api.model.generated.OperatorType.class, io.airbyte.config.StandardSyncOperation.OperatorType.class));
  }

  @Test
  void testDbtCloudRegex() {
    // Validate that a non-url is rejected.
    assertThrows(IllegalArgumentException.class, () -> checkDbtCloudUrl("not-a-url"));
    // Validate that the URL is anchored to the beginning.
    assertThrows(IllegalArgumentException.class,
        () -> checkDbtCloudUrl("some-nonsense-" + String.format(EXECUTION_URL_TEMPLATE, DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
            DBT_CLOUD_WEBHOOK_JOB_ID)));
    // Validate that the URL is anchored to the end.
    assertThrows(IllegalArgumentException.class,
        () -> checkDbtCloudUrl(String.format(EXECUTION_URL_TEMPLATE, DBT_CLOUD_WEBHOOK_ACCOUNT_ID,
            DBT_CLOUD_WEBHOOK_JOB_ID) + "-some-nonsense"));
    // Validate that the account id must be an integer.
    assertThrows(IllegalArgumentException.class, () -> checkDbtCloudUrl("https://cloud.getdbt.com/api/v2/accounts/abc/jobs/123/run/"));
    // Validate that the job id must be an integer.
    assertThrows(IllegalArgumentException.class, () -> checkDbtCloudUrl("https://cloud.getdbt.com/api/v2/accounts/123/jobs/abc/run/"));
  }

  private void checkDbtCloudUrl(final String urlToCheck) throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardSyncOperation persistedOperation = new StandardSyncOperation()
        .withWorkspaceId(standardSyncOperation.getWorkspaceId())
        .withOperationId(WEBHOOK_OPERATION_ID)
        .withName(WEBHOOK_OPERATION_NAME)
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK)
        .withOperatorWebhook(new io.airbyte.config.OperatorWebhook()
            .withWebhookConfigId(WEBHOOK_CONFIG_ID)
            .withExecutionUrl(urlToCheck)
            .withExecutionBody(EXECUTION_BODY))
        .withTombstone(false);
    when(operationService.getStandardSyncOperation(WEBHOOK_OPERATION_ID)).thenReturn(persistedOperation);

    final OperationIdRequestBody operationIdRequestBody = new OperationIdRequestBody().operationId(WEBHOOK_OPERATION_ID);
    operationsHandler.getOperation(operationIdRequestBody);
  }

}
