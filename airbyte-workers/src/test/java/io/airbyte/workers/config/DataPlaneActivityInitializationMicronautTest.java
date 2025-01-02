/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import io.airbyte.commons.micronaut.EnvConstants;
import io.airbyte.config.secrets.persistence.SecretPersistence;
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity;
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivityImpl;
import io.airbyte.workers.temporal.sync.RefreshSchemaActivity;
import io.airbyte.workers.temporal.sync.RefreshSchemaActivityImpl;
import io.airbyte.workers.temporal.sync.WebhookOperationActivity;
import io.airbyte.workers.temporal.sync.WebhookOperationActivityImpl;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.env.Environment;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@MicronautTest(environments = {EnvConstants.DATA_PLANE, Environment.KUBERNETES})
@Property(name = "airbyte.internal-api.base-path",
          value = "http://airbyte.test:1337")
@Property(name = "airbyte.version",
          value = "0.4128173.0")
@Property(name = "airbyte.local.root",
          value = "/tmp/local")
@Property(name = "airbyte.workspace.root",
          value = "/tmp/workspace")
@Property(name = "airbyte.cloud.storage.type",
          value = "local")
@Property(name = "airbyte.cloud.storage.bucket.log",
          value = "log")
@Property(name = "airbyte.cloud.storage.bucket.state",
          value = "state")
@Property(name = "airbyte.cloud.storage.bucket.workload-output",
          value = "workload")
@Property(name = "airbyte.cloud.storage.bucket.activity-payload",
          value = "payload")
// @Property(name = "airbyte.cloud.storage.bucket.audit-logging",
// value = "audit")
class DataPlaneActivityInitializationMicronautTest {

  // Ideally this should be broken down into different tests to get a clearer view of which bean
  // breaks. However, some properties are read from the environment variables and need to be mocked
  // in all each tests. This is a tradeoff to stay away from some redundancy in the test setup.

  // This bean is mocked because the secret persistence instantiation is very environment dependent.
  @Bean
  @Replaces(SecretPersistence.class)
  SecretPersistence secretPersistence = mock(SecretPersistence.class);

  @Inject
  ConfigFetchActivity configFetchActivity;

  @Inject
  RefreshSchemaActivity refreshSchemaActivity;

  @Inject
  WebhookOperationActivity webhookOperationActivity;

  @Test
  void testConfigFetchActivity() {
    assertEquals(ConfigFetchActivityImpl.class, configFetchActivity.getClass());
  }

  @Test
  void testRefreshSchemaActivity() {
    assertEquals(RefreshSchemaActivityImpl.class, refreshSchemaActivity.getClass());
  }

  @Test
  void testWebhookOperationActivity() {
    assertEquals(WebhookOperationActivityImpl.class, webhookOperationActivity.getClass());
  }

}
