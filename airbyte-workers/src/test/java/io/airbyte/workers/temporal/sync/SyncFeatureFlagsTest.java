/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.ContainerOrchestratorDevImage;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.workers.ContainerOrchestratorConfig;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@SuppressWarnings("MissingJavadocType")
public class SyncFeatureFlagsTest {

  @Nested
  class ContainerImageInjection {

    static FeatureFlagClient client = Mockito.mock(TestClient.class);

    @Test
    void shouldInjectIfConnectionIdIsIncluded() {
      var correctUuid = UUID.randomUUID();
      var expImage = "image 1";
      var expNamespace = "ns1";
      var expSecret = "secret";
      when(client.stringVariation(ContainerOrchestratorDevImage.INSTANCE, new Connection(correctUuid))).thenReturn(expImage);

      ContainerOrchestratorConfig config =
          new ContainerOrchestratorConfig(expNamespace, null, Map.of("a", "b"), null, expSecret,
              "path", "dataplane secrets", "dataplane path",
              "image 0", "pull policy", "gcp creds", null);

      var actual = ReplicationActivityImpl.injectContainerOrchestratorImage(client, config, correctUuid);

      assertEquals(expImage, actual.containerOrchestratorImage());
      // Spot check non image fields to make sure they remain the same.
      assertEquals(expNamespace, actual.namespace());
      assertEquals(expSecret, actual.secretName());
    }

    @Test
    void shouldNotInjectIfConnectionIdIsNotIncluded() {
      var badUuid = UUID.randomUUID();
      when(client.stringVariation(ContainerOrchestratorDevImage.INSTANCE, new Connection(badUuid))).thenReturn("");

      ContainerOrchestratorConfig config =
          new ContainerOrchestratorConfig("ns 1", null, Map.of("a", "b"), null, "secret",
              "path", "dataplane secrets", "dataplane path",
              "image 0", "pull policy", "gcp creds", null);

      var actual = ReplicationActivityImpl.injectContainerOrchestratorImage(client, config, badUuid);

      assertEquals(config, actual);
    }

  }

}
