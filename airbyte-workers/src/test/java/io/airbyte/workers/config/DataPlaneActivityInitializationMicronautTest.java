/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import io.airbyte.commons.temporal.config.WorkerMode;
import io.airbyte.config.secrets.persistence.SecretPersistence;
import io.airbyte.workers.storage.DocumentStoreClient;
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity;
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivityImpl;
import io.airbyte.workers.temporal.sync.DbtTransformationActivity;
import io.airbyte.workers.temporal.sync.DbtTransformationActivityImpl;
import io.airbyte.workers.temporal.sync.NormalizationActivity;
import io.airbyte.workers.temporal.sync.NormalizationActivityImpl;
import io.airbyte.workers.temporal.sync.NormalizationSummaryCheckActivity;
import io.airbyte.workers.temporal.sync.NormalizationSummaryCheckActivityImpl;
import io.airbyte.workers.temporal.sync.RefreshSchemaActivity;
import io.airbyte.workers.temporal.sync.RefreshSchemaActivityImpl;
import io.airbyte.workers.temporal.sync.ReplicationActivity;
import io.airbyte.workers.temporal.sync.ReplicationActivityImpl;
import io.airbyte.workers.temporal.sync.WebhookOperationActivity;
import io.airbyte.workers.temporal.sync.WebhookOperationActivityImpl;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;

@MicronautTest(environments = {WorkerMode.DATA_PLANE})
@Property(name = "airbyte.internal.api.host",
          value = "airbyte.test:1337")
@Property(name = "airbyte.version",
          value = "0.4128173.0")
@Property(name = "airbyte.local.root",
          value = "/tmp/local")
@Property(name = "airbyte.workspace.root",
          value = "/tmp/workspace")
class DataPlaneActivityInitializationMicronautTest {

  // Ideally this should be broken down into different tests to get a clearer view of which bean
  // breaks. However, some properties are read from the environment variables and need to be mocked
  // in all each tests. This is a tradeoff to stay away from some redundancy in the test setup.

  // This bean is mocked because the secret persistence instantiation is very environment dependent.
  @Bean
  @Replaces(SecretPersistence.class)
  SecretPersistence secretPersistence = mock(SecretPersistence.class);

  @Bean
  @Replaces(value = DocumentStoreClient.class,
            named = "stateDocumentStore")
  @Named("stateDocumentStore")
  DocumentStoreClient documentStoreClient = mock(DocumentStoreClient.class);

  @Bean
  @Replaces(value = DocumentStoreClient.class,
            named = "outputDocumentStore")
  @Named("outputDocumentStore")
  DocumentStoreClient outputDocumentStoreClient = mock(DocumentStoreClient.class);

  @Inject
  ConfigFetchActivity configFetchActivity;

  @Inject
  DbtTransformationActivity dbtTransformationActivity;

  @Inject
  NormalizationActivity normalizationActivity;

  @Inject
  NormalizationSummaryCheckActivity normalizationSummaryCheckActivity;

  @Inject
  RefreshSchemaActivity refreshSchemaActivity;

  @Inject
  ReplicationActivity replicationActivity;

  @Inject
  WebhookOperationActivity webhookOperationActivity;

  @Test
  void testConfigFetchActivity() {
    assertEquals(ConfigFetchActivityImpl.class, configFetchActivity.getClass());
  }

  @Test
  void testDbtTransformationActivity() {
    assertEquals(DbtTransformationActivityImpl.class, dbtTransformationActivity.getClass());
  }

  @Test
  void testNormalizationActivity() {
    assertEquals(NormalizationActivityImpl.class, normalizationActivity.getClass());
  }

  @Test
  void testNormalizationSummaryCheckActivity() {
    assertEquals(NormalizationSummaryCheckActivityImpl.class, normalizationSummaryCheckActivity.getClass());
  }

  @Test
  void testRefreshSchemaActivity() {
    assertEquals(RefreshSchemaActivityImpl.class, refreshSchemaActivity.getClass());
  }

  @Test
  void testReplicationActivity() {
    assertEquals(ReplicationActivityImpl.class, replicationActivity.getClass());
  }

  @Test
  void testWebhookOperationActivity() {
    assertEquals(WebhookOperationActivityImpl.class, webhookOperationActivity.getClass());
  }

}
