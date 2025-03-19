/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.workers.config;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.airbyte.config.Configs;
import io.airbyte.config.EnvConfigs;
import io.airbyte.config.ResourceRequirements;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class WorkerConfigsTest {

  private static final String JOB = "job";
  private static final Map<String, String> DEFAULT_NODE_SELECTORS = ImmutableMap.of(JOB, "default");
  private static final ResourceRequirements DEFAULT_RESOURCE_REQUIREMENTS = new ResourceRequirements();

  private Configs configs;

  @BeforeEach
  void setup() {
    configs = mock(EnvConfigs.class);
    when(configs.getJobKubeNodeSelectors()).thenReturn(DEFAULT_NODE_SELECTORS);
  }

  @Test
  @DisplayName("default workerConfigs use default node selectors")
  void testDefaultNodeSelectors() {
    final WorkerConfigs defaultWorkerConfigs = new WorkerConfigs(configs);

    Assertions.assertEquals(DEFAULT_NODE_SELECTORS, defaultWorkerConfigs.getworkerKubeNodeSelectors());
  }

  @Test
  @DisplayName("default workerConfigs use default resourceRequirements")
  void testDefaultResourceRequirements() {
    final WorkerConfigs defaultWorkerConfigs = new WorkerConfigs(configs);

    Assertions.assertEquals(DEFAULT_RESOURCE_REQUIREMENTS, defaultWorkerConfigs.getResourceRequirements());
  }

}
