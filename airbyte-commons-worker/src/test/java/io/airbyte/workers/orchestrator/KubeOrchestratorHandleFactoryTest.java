/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.orchestrator;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.ContainerOrchestratorDevImage;
import io.airbyte.featureflag.ContainerOrchestratorJavaOpts;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.workers.ContainerOrchestratorConfig;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class KubeOrchestratorHandleFactoryTest {

  static FeatureFlagClient client = Mockito.mock(TestClient.class);

  @Nested
  class ContainerImageInjection {

    @Test
    void shouldInjectIfConnectionIdIsIncluded() {
      final var correctUuid = UUID.randomUUID();
      final var expImage = "image 1";
      final var expNamespace = "ns1";
      final var expSecret = "secret";
      when(client.stringVariation(ContainerOrchestratorDevImage.INSTANCE, new Connection(correctUuid))).thenReturn(expImage);
      when(client.stringVariation(ContainerOrchestratorJavaOpts.INSTANCE, new Connection(correctUuid))).thenReturn("");

      final ContainerOrchestratorConfig config =
          new ContainerOrchestratorConfig(expNamespace, Map.of("a", "b"), expSecret,
              "path", "dataplane secrets", "dataplane path",
              "image 0", "pull policy", "gcp creds", "airbyte-admin");

      final ContainerOrchestratorConfig actual = KubeOrchestratorHandleFactory.injectContainerOrchestratorConfig(client, config, correctUuid);

      assertEquals(expImage, actual.containerOrchestratorImage());
      // Spot check non image fields to make sure they remain the same.
      assertEquals(expNamespace, actual.namespace());
      assertEquals(expSecret, actual.secretName());
    }

    @Test
    void shouldNotInjectIfConnectionIdIsNotIncluded() {
      final var badUuid = UUID.randomUUID();
      when(client.stringVariation(ContainerOrchestratorDevImage.INSTANCE, new Connection(badUuid))).thenReturn("");
      when(client.stringVariation(ContainerOrchestratorJavaOpts.INSTANCE, new Connection(badUuid))).thenReturn("");

      final ContainerOrchestratorConfig config =
          new ContainerOrchestratorConfig("ns 1", Map.of("a", "b"), "secret",
              "path", "dataplane secrets", "dataplane path",
              "image 0", "pull policy", "gcp creds", "airbyte-admin");

      final ContainerOrchestratorConfig actual = KubeOrchestratorHandleFactory.injectContainerOrchestratorConfig(client, config, badUuid);

      assertEquals(config, actual);
    }

  }

  @Nested
  class JavaOptsInjection {

    @Test
    void shouldInjectIfConnectionIdIsIncluded() {
      final var correctUuid = UUID.randomUUID();
      final var expOptsString = "-Xmx1g -Xms1g";
      final var expNamespace = "ns1";
      final var expSecret = "secret";
      when(client.stringVariation(ContainerOrchestratorJavaOpts.INSTANCE, new Connection(correctUuid))).thenReturn(expOptsString);
      when(client.stringVariation(ContainerOrchestratorDevImage.INSTANCE, new Connection(correctUuid))).thenReturn("");

      final var orgMap = Map.of("a", "b", "JAVA_OPTS", "bad");
      final ContainerOrchestratorConfig config =
          new ContainerOrchestratorConfig(expNamespace, orgMap, expSecret,
              "path", "dataplane secrets", "dataplane path",
              "image 0", "pull policy", "gcp creds", "airbyte-admin");

      final ContainerOrchestratorConfig actual = KubeOrchestratorHandleFactory.injectContainerOrchestratorConfig(client, config, correctUuid);

      final var expMap = Map.of("a", "b", "JAVA_OPTS", expOptsString);
      assertEquals(expMap, actual.environmentVariables());
      // Spot check non image fields to make sure they remain the same.
      assertEquals(expNamespace, actual.namespace());
      assertEquals(expSecret, actual.secretName());
    }

    @Test
    void shouldNotInjectIfConnectionIdIsNotIncluded() {
      final var badUuid = UUID.randomUUID();
      when(client.stringVariation(ContainerOrchestratorJavaOpts.INSTANCE, new Connection(badUuid))).thenReturn("");
      when(client.stringVariation(ContainerOrchestratorDevImage.INSTANCE, new Connection(badUuid))).thenReturn("");

      final ContainerOrchestratorConfig config =
          new ContainerOrchestratorConfig("ns 1", Map.of("a", "b"), "secret",
              "path", "dataplane secrets", "dataplane path",
              "image 0", "pull policy", "gcp creds", "airbyte-admin");

      final ContainerOrchestratorConfig actual = KubeOrchestratorHandleFactory.injectContainerOrchestratorConfig(client, config, badUuid);

      assertEquals(config, actual);
    }

  }

}
