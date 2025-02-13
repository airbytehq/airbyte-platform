/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.config.persistence.OrganizationPersistence.DEFAULT_ORGANIZATION_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import io.airbyte.config.Geography;
import io.airbyte.config.OperatorWebhook;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardSyncOperation.OperatorType;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.OperationService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.impls.jooq.OperationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SyncOperationPersistenceTest extends BaseConfigDatabaseTest {

  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID WEBHOOK_CONFIG_ID = UUID.randomUUID();
  private static final String WEBHOOK_OPERATION_EXECUTION_URL = "test-webhook-url";
  private static final String WEBHOOK_OPERATION_EXECUTION_BODY = "test-webhook-body";

  private OperationService operationService;

  private static final StandardSyncOperation WEBHOOK_OP = new StandardSyncOperation()
      .withName("webhook-operation")
      .withTombstone(false)
      .withOperationId(UUID.randomUUID())
      .withWorkspaceId(WORKSPACE_ID)
      .withOperatorType(OperatorType.WEBHOOK)
      .withOperatorWebhook(
          new OperatorWebhook()
              .withWebhookConfigId(WEBHOOK_CONFIG_ID)
              .withExecutionUrl(WEBHOOK_OPERATION_EXECUTION_URL)
              .withExecutionBody(WEBHOOK_OPERATION_EXECUTION_BODY));
  private static final List<StandardSyncOperation> OPS = List.of(WEBHOOK_OP);

  @BeforeEach
  void beforeEach() throws Exception {
    truncateAllTables();

    operationService = new OperationServiceJooqImpl(database);

    createWorkspace();

    for (final StandardSyncOperation op : OPS) {
      operationService.writeStandardSyncOperation(op);
    }
  }

  @Test
  void testReadWrite() throws IOException, ConfigNotFoundException, JsonValidationException {
    for (final StandardSyncOperation op : OPS) {
      assertEquals(op, operationService.getStandardSyncOperation(op.getOperationId()));
    }
  }

  @Test
  void testReadNotExists() {
    assertThrows(ConfigNotFoundException.class, () -> operationService.getStandardSyncOperation(UUID.randomUUID()));
  }

  @Test
  void testList() throws IOException, JsonValidationException {
    assertEquals(OPS, operationService.listStandardSyncOperations());
  }

  @Test
  void testDelete() throws IOException, ConfigNotFoundException, JsonValidationException {
    for (final StandardSyncOperation op : OPS) {
      assertEquals(op, operationService.getStandardSyncOperation(op.getOperationId()));
      operationService.deleteStandardSyncOperation(op.getOperationId());
      assertThrows(ConfigNotFoundException.class, () -> operationService.getStandardSyncOperation(UUID.randomUUID()));

    }
  }

  private void createWorkspace() throws IOException, JsonValidationException {
    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);
    new OrganizationServiceJooqImpl(database).writeOrganization(MockData.defaultOrganization());

    final StandardWorkspace workspace = new StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withName("Another Workspace")
        .withSlug("another-workspace")
        .withInitialSetupComplete(true)
        .withTombstone(false)
        .withDefaultGeography(Geography.AUTO)
        .withOrganizationId(DEFAULT_ORGANIZATION_ID);
    new WorkspaceServiceJooqImpl(database, featureFlagClient, secretsRepositoryReader, secretsRepositoryWriter, secretPersistenceConfigService)
        .writeStandardWorkspaceNoSecrets(workspace);
  }

}
