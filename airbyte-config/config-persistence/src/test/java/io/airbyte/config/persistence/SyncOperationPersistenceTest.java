/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import io.airbyte.config.Geography;
import io.airbyte.config.OperatorDbt;
import io.airbyte.config.OperatorNormalization;
import io.airbyte.config.OperatorNormalization.Option;
import io.airbyte.config.OperatorWebhook;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardSyncOperation.OperatorType;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.CatalogServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.ConnectorBuilderServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.HealthCheckServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OAuthServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OperationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
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

  private ConfigRepository configRepository;

  private static final StandardSyncOperation DBT_OP = new StandardSyncOperation()
      .withName("operation-1")
      .withTombstone(false)
      .withOperationId(UUID.randomUUID())
      .withWorkspaceId(WORKSPACE_ID)
      .withOperatorDbt(new OperatorDbt()
          .withDbtArguments("dbt-arguments")
          .withDockerImage("image-tag")
          .withGitRepoBranch("git-repo-branch")
          .withGitRepoUrl("git-repo-url"))
      .withOperatorNormalization(null)
      .withOperatorType(OperatorType.DBT);
  private static final StandardSyncOperation NORMALIZATION_OP = new StandardSyncOperation()
      .withName("operation-1")
      .withTombstone(false)
      .withOperationId(UUID.randomUUID())
      .withWorkspaceId(WORKSPACE_ID)
      .withOperatorDbt(null)
      .withOperatorNormalization(new OperatorNormalization().withOption(Option.BASIC))
      .withOperatorType(OperatorType.NORMALIZATION);
  private static final StandardSyncOperation WEBHOOK_OP = new StandardSyncOperation()
      .withName("webhook-operation")
      .withTombstone(false)
      .withOperationId(UUID.randomUUID())
      .withWorkspaceId(WORKSPACE_ID)
      .withOperatorType(OperatorType.WEBHOOK)
      .withOperatorDbt(null)
      .withOperatorNormalization(null)
      .withOperatorWebhook(
          new OperatorWebhook()
              .withWebhookConfigId(WEBHOOK_CONFIG_ID)
              .withExecutionUrl(WEBHOOK_OPERATION_EXECUTION_URL)
              .withExecutionBody(WEBHOOK_OPERATION_EXECUTION_BODY));
  private static final List<StandardSyncOperation> OPS = List.of(DBT_OP, NORMALIZATION_OP, WEBHOOK_OP);

  @BeforeEach
  void beforeEach() throws Exception {
    truncateAllTables();

    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);

    configRepository = new ConfigRepository(
        new ActorDefinitionServiceJooqImpl(database),
        new CatalogServiceJooqImpl(database),
        new ConnectionServiceJooqImpl(database),
        new ConnectorBuilderServiceJooqImpl(database),
        new DestinationServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretsRepositoryWriter,
            secretPersistenceConfigService),
        new HealthCheckServiceJooqImpl(database),
        new OAuthServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretPersistenceConfigService),
        new OperationServiceJooqImpl(database),
        new OrganizationServiceJooqImpl(database),
        new SourceServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretsRepositoryWriter,
            secretPersistenceConfigService),
        new WorkspaceServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretsRepositoryWriter,
            secretPersistenceConfigService));
    createWorkspace();

    for (final StandardSyncOperation op : OPS) {
      configRepository.writeStandardSyncOperation(op);
    }
  }

  @Test
  void testReadWrite() throws IOException, ConfigNotFoundException, JsonValidationException {
    for (final StandardSyncOperation op : OPS) {
      assertEquals(op, configRepository.getStandardSyncOperation(op.getOperationId()));
    }
  }

  @Test
  void testReadNotExists() {
    assertThrows(ConfigNotFoundException.class, () -> configRepository.getStandardSyncOperation(UUID.randomUUID()));
  }

  @Test
  void testList() throws IOException, JsonValidationException {
    assertEquals(OPS, configRepository.listStandardSyncOperations());
  }

  @Test
  void testDelete() throws IOException, ConfigNotFoundException, JsonValidationException {
    for (final StandardSyncOperation op : OPS) {
      assertEquals(op, configRepository.getStandardSyncOperation(op.getOperationId()));
      configRepository.deleteStandardSyncOperation(op.getOperationId());
      assertThrows(ConfigNotFoundException.class, () -> configRepository.getStandardSyncOperation(UUID.randomUUID()));

    }
  }

  private void createWorkspace() throws IOException, JsonValidationException {
    final StandardWorkspace workspace = new StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withName("Another Workspace")
        .withSlug("another-workspace")
        .withInitialSetupComplete(true)
        .withTombstone(false)
        .withDefaultGeography(Geography.AUTO);
    configRepository.writeStandardWorkspaceNoSecrets(workspace);
  }

}
