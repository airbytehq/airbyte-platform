/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
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
  private static final String DEFAULT_CPU_REQUEST = "0.1";
  private static final String DEFAULT_CPU_LIMIT = "0.2";
  private static final String DEFAULT_MEMORY_REQUEST = "100Mi";
  private static final String DEFAULT_MEMORY_LIMIT = "200Mi";
  private static final ResourceRequirements DEFAULT_RESOURCE_REQUIREMENTS = new ResourceRequirements()
      .withCpuRequest(DEFAULT_CPU_REQUEST)
      .withCpuLimit(DEFAULT_CPU_LIMIT)
      .withMemoryRequest(DEFAULT_MEMORY_REQUEST)
      .withMemoryLimit(DEFAULT_MEMORY_LIMIT);

  private Configs configs;

  @BeforeEach
  void setup() {
    configs = mock(EnvConfigs.class);
    when(configs.getJobKubeNodeSelectors()).thenReturn(DEFAULT_NODE_SELECTORS);
    when(configs.getJobMainContainerCpuRequest()).thenReturn(DEFAULT_CPU_REQUEST);
    when(configs.getJobMainContainerCpuLimit()).thenReturn(DEFAULT_CPU_LIMIT);
    when(configs.getJobMainContainerMemoryRequest()).thenReturn(DEFAULT_MEMORY_REQUEST);
    when(configs.getJobMainContainerMemoryLimit()).thenReturn(DEFAULT_MEMORY_LIMIT);
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
